/**
 * Domain types for the edge command-dispatch state machine.
 *
 * This module is PURE: it has no knowledge of HTTP, SQLite, the network, wall
 * clocks or id generation. Everything that is non-deterministic (time, ids) is
 * injected by the caller so that failure timings can be tested deterministically.
 */

/** Lifecycle of a single dispatched device command. */
export type CommandStatus =
  | 'PENDING' // persisted, waiting for a gateway to lease it
  | 'LEASED' // a gateway holds a time-boxed lease and is (or will be) driving the device
  | 'SUCCEEDED' // a device confirmation of success has been persisted (terminal)
  | 'FAILED'; // a device confirmation of failure, or retry exhaustion (terminal)

export type ConfirmOutcome = 'success' | 'failure';

/** The business instruction handed to the device. Opaque to the state machine. */
export interface CommandPayload {
  deviceId: string;
  /** e.g. "calibrate", "switch_process" */
  kind: string;
  params: Record<string, unknown>;
}

/**
 * A dispatched command. `executionId` is the STABLE execution identity: it is
 * minted once at submit time and never changes across re-leases or retries, so
 * the device can deduplicate at-least-once delivery. `attemptEpoch` is the
 * fencing token: it increases on every lease acquisition and lets us reject
 * messages from a superseded lease holder ("anti-revival").
 */
export interface Command {
  readonly id: string; // internal primary id (== executionId; kept separate for clarity)
  readonly idempotencyKey: string; // upstream business idempotency key (unique)
  readonly executionId: string; // stable device-action identity, minted once
  readonly payload: CommandPayload;
  readonly status: CommandStatus;

  readonly attemptEpoch: number; // fencing token; increments per lease acquisition
  readonly attempts: number; // number of lease acquisitions so far
  readonly maxAttempts: number; // retry budget for re-leasing after expiry

  readonly leaseId: string | null; // current lease token (present iff LEASED)
  readonly leaseholder: string | null; // gateway id currently holding the lease
  readonly leaseExpiresAt: number | null; // epoch ms when the current lease dies

  readonly terminalReason: string | null; // why SUCCEEDED/FAILED (audit)

  readonly createdAt: number;
  readonly updatedAt: number;
}

/**
 * Append-only causal record. Every state change emits one or more of these so
 * that operators can reconstruct *why* a command is where it is, not merely its
 * final value.
 */
export type DomainEventType =
  | 'SUBMITTED'
  | 'SUBMIT_DEDUPED'
  | 'LEASED'
  | 'RENEWED'
  | 'RENEW_REJECTED'
  | 'LEASE_EXPIRED'
  | 'REQUEUED'
  | 'CONFIRMED_SUCCESS'
  | 'CONFIRMED_FAILURE'
  | 'CONFIRM_IGNORED'
  | 'EXHAUSTED';

export interface DomainEvent {
  readonly commandId: string;
  readonly type: DomainEventType;
  readonly at: number; // epoch ms (injected clock)
  /** The fencing epoch this event is attributed to, when meaningful. */
  readonly attemptEpoch: number | null;
  /** Structured, human-explicable context. */
  readonly detail: Record<string, unknown>;
}

/**
 * The result of applying one domain operation. The application layer persists
 * `command` and `events` atomically. `changed` says whether `command` differs
 * from the prior state (a rejected/ignored operation still emits an audit event
 * but leaves the command untouched).
 */
export interface Transition {
  readonly command: Command;
  readonly events: DomainEvent[];
  readonly changed: boolean;
  readonly outcome: TransitionOutcome;
}

/** Operation-specific, transport-agnostic outcome describing what happened. */
export type TransitionOutcome =
  | { kind: 'created' }
  | { kind: 'deduped' }
  | { kind: 'leased' }
  | { kind: 'renewed' }
  | { kind: 'renew_rejected'; reason: string }
  | { kind: 'confirmed'; outcome: ConfirmOutcome }
  | { kind: 'confirm_ignored'; reason: string }
  | { kind: 'expired_requeued' }
  | { kind: 'expired_failed' }
  | { kind: 'noop' };
