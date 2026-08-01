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
  | 'FAILED' // a device confirmation of failure, or retry exhaustion (terminal)
  | 'CANCELLED' // upstream emergency recall of a not-yet-completed action (terminal)
  | 'SUPERSEDED'; // replaced by an explicit newer safety command (terminal)

export type ConfirmOutcome = 'success' | 'failure';

/** The business instruction handed to the device. Opaque to the state machine. */
export interface CommandPayload {
  deviceId: string;
  /** e.g. "calibrate", "switch_process" */
  kind: string;
  params: Record<string, unknown>;
}

/**
 * Per-line ownership record for redundant (active/standby) gateways.
 *
 * A production line is owned by at most one gateway at a time. Ownership is a
 * heartbeat lease: the owner must renew before `expiresAt` or a standby may take
 * over. `generation` (代际) is a MONOTONIC fencing token bumped on every takeover.
 * Every command operation carries the caller's generation; the domain rejects
 * any operation stamped with a generation older than the line's current one, so
 * a partitioned old owner can neither lease work nor advance state with a late
 * confirmation — even while it still (locally) believes it holds a command lease.
 */
export interface LineOwnership {
  readonly lineId: string;
  readonly owner: string; // gateway id currently owning the line
  readonly generation: number; // monotonic; increments on each takeover
  readonly expiresAt: number; // epoch ms when the ownership heartbeat lease dies
  readonly createdAt: number; // when generation 1 was first established
  readonly updatedAt: number;
}

/**
 * A dispatched command. `executionId` is the STABLE execution identity: it is
 * minted once at submit time and never changes across re-leases or retries, so
 * the device can deduplicate at-least-once delivery. `attemptEpoch` is the
 * fencing token: it increases on every lease acquisition and lets us reject
 * messages from a superseded lease holder ("anti-revival").
 *
 * `lineId` binds the command to a production line; `ownerGeneration` records the
 * ownership generation that most recently leased it, so a superseded generation's
 * late confirmation can be fenced out even if its per-command `leaseId` matches.
 */
export interface Command {
  readonly id: string; // internal primary id (== executionId; kept separate for clarity)
  readonly idempotencyKey: string; // upstream business idempotency key (unique)
  readonly executionId: string; // stable device-action identity, minted once
  readonly lineId: string; // production line this command belongs to
  readonly payload: CommandPayload;
  readonly status: CommandStatus;

  readonly attemptEpoch: number; // fencing token; increments per lease acquisition
  readonly attempts: number; // number of lease acquisitions so far
  readonly maxAttempts: number; // retry budget for re-leasing after expiry

  readonly leaseId: string | null; // current lease token (present iff LEASED)
  readonly leaseholder: string | null; // gateway id currently holding the lease
  readonly leaseExpiresAt: number | null; // epoch ms when the current lease dies
  readonly ownerGeneration: number | null; // ownership generation that leased it

  /**
   * When this command was submitted as an explicit replacement for another, the
   * id of the command it supersedes. Lets an operator walk the recall/replace
   * lineage as one causal chain.
   */
  readonly supersedesId: string | null;

  readonly terminalReason: string | null; // why SUCCEEDED/FAILED/CANCELLED/SUPERSEDED (audit)

  readonly createdAt: number;
  readonly updatedAt: number;
}

/**
 * Append-only causal record. Every state change emits one or more of these so
 * that operators can reconstruct *why* a command is where it is, not merely its
 * final value.
 *
 * Events are either command-scoped (`subject: 'command'`, `subjectId` = command
 * id) or line-scoped (`subject: 'line'`, `subjectId` = lineId) for ownership
 * transitions. `generation` records the ownership代际 the event is attributed to,
 * so a takeover's before/after causality is reconstructable.
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
  | 'EXHAUSTED'
  | 'LEASE_FENCED' // lease attempt rejected: caller generation is stale
  | 'PREEMPTED' // in-flight command taken over by a newer generation
  | 'CANCELLED' // upstream recalled a not-yet-completed action
  | 'CANCEL_REJECTED' // recall refused: action already terminal (e.g. confirmed success)
  | 'SUPERSEDED' // this command was replaced by a newer safety command
  | 'SUPERSEDE_NOTED' // a supersede referenced an already-terminal command (no rewrite)
  // line-scoped ownership events:
  | 'OWNERSHIP_ACQUIRED' // first owner of a line (generation 1)
  | 'OWNERSHIP_RENEWED' // heartbeat extended current ownership lease
  | 'OWNERSHIP_TAKEOVER' // standby took over an expired owner (generation++)
  | 'OWNERSHIP_REJECTED'; // takeover/heartbeat rejected (healthy owner or stale)

export type EventSubject = 'command' | 'line';

export interface DomainEvent {
  /** 'command' -> subjectId is a command id; 'line' -> subjectId is a lineId. */
  readonly subject: EventSubject;
  readonly subjectId: string;
  /** Convenience alias equal to subjectId when subject === 'command'. */
  readonly commandId: string | null;
  readonly type: DomainEventType;
  readonly at: number; // epoch ms (injected clock)
  /** The per-command fencing epoch this event is attributed to, when meaningful. */
  readonly attemptEpoch: number | null;
  /** The ownership generation (代际) this event is attributed to, when meaningful. */
  readonly generation: number | null;
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
  | { kind: 'lease_fenced'; reason: string }
  | { kind: 'preempted' }
  | { kind: 'renewed' }
  | { kind: 'renew_rejected'; reason: string }
  | { kind: 'confirmed'; outcome: ConfirmOutcome }
  | { kind: 'confirm_ignored'; reason: string }
  | { kind: 'expired_requeued' }
  | { kind: 'expired_failed' }
  | { kind: 'cancelled' }
  | { kind: 'cancel_rejected'; reason: string }
  | { kind: 'superseded' }
  | { kind: 'supersede_noted'; reason: string }
  | { kind: 'noop' };

/** Result of an ownership (line-level) transition. */
export interface OwnershipTransition {
  readonly ownership: LineOwnership;
  readonly events: DomainEvent[];
  readonly changed: boolean;
  readonly outcome: OwnershipOutcome;
}

export type OwnershipOutcome =
  | { kind: 'acquired' }
  | { kind: 'renewed' }
  | { kind: 'takeover' }
  | { kind: 'rejected'; reason: string };
