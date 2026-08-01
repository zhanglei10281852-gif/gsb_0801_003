/**
 * Pure domain state machine for reliable command dispatch.
 *
 * Every function here is a pure transition: given the current `Command` and
 * inputs (including an injected `now` and any freshly generated ids), it returns
 * a new `Command`, the causal `DomainEvent`s to append, and a transport-agnostic
 * outcome. There is NO I/O and NO hidden clock, so failure interleavings can be
 * reproduced deterministically in tests.
 *
 * Invariants enforced here (the heart of the delivery guarantee):
 *  - `executionId` is minted exactly once and is immutable. Re-leases, retries
 *    and takeovers reuse it, so the device can deduplicate at-least-once delivery
 *    and neither a restart nor a gateway takeover turns one business request into
 *    two device actions.
 *  - `attemptEpoch` is a monotonic per-command fencing token bumped on every
 *    lease acquisition. Renew/confirm messages that do not match the CURRENT,
 *    still valid lease are rejected — an expired or superseded lease can never be
 *    silently revived by a late message.
 *  - `ownerGeneration` fences redundant gateways. Every lease/renew/confirm may
 *    carry the caller's ownership generation (代际) and the line's current
 *    generation. Any operation whose caller generation is OLDER than the line's
 *    current generation is rejected. After a standby takes over (generation++),
 *    the old generation can neither lease new work nor advance state with a late
 *    confirmation, even during a partition where it still locally holds a lease.
 *  - Only a confirmation delivered against the current, unexpired lease AND a
 *    non-stale generation can move a command to SUCCEEDED/FAILED.
 */

import {
  Command,
  CommandPayload,
  ConfirmOutcome,
  DomainEvent,
  DomainEventType,
  Transition,
} from './types';

export interface CreateInput {
  id: string; // primary id + stable executionId
  idempotencyKey: string;
  payload: CommandPayload;
  now: number;
  maxAttempts: number;
  lineId?: string; // production line; defaults to 'default' for single-line use
}

function event(
  commandId: string,
  type: DomainEventType,
  at: number,
  attemptEpoch: number | null,
  generation: number | null,
  detail: Record<string, unknown>,
): DomainEvent {
  return {
    subject: 'command',
    subjectId: commandId,
    commandId,
    type,
    at,
    attemptEpoch,
    generation,
    detail,
  };
}

function isTerminal(status: Command['status']): boolean {
  return status === 'SUCCEEDED' || status === 'FAILED';
}

function leaseIsLive(cmd: Command, now: number): boolean {
  return (
    cmd.status === 'LEASED' &&
    cmd.leaseExpiresAt !== null &&
    cmd.leaseExpiresAt > now
  );
}

/**
 * Is the caller's generation stale relative to the line's current generation?
 * Only meaningful when both are provided (generation fencing is opt-in so the
 * single-gateway path keeps working with nulls).
 */
function generationIsStale(
  callerGeneration: number | null | undefined,
  currentGeneration: number | null | undefined,
): boolean {
  return (
    callerGeneration != null &&
    currentGeneration != null &&
    callerGeneration < currentGeneration
  );
}

/** Create a brand new PENDING command with a stable execution identity. */
export function create(input: CreateInput): Transition {
  const cmd: Command = {
    id: input.id,
    idempotencyKey: input.idempotencyKey,
    executionId: input.id,
    lineId: input.lineId ?? 'default',
    payload: input.payload,
    status: 'PENDING',
    attemptEpoch: 0,
    attempts: 0,
    maxAttempts: input.maxAttempts,
    leaseId: null,
    leaseholder: null,
    leaseExpiresAt: null,
    ownerGeneration: null,
    terminalReason: null,
    createdAt: input.now,
    updatedAt: input.now,
  };
  return {
    command: cmd,
    changed: true,
    outcome: { kind: 'created' },
    events: [
      event(cmd.id, 'SUBMITTED', input.now, null, null, {
        idempotencyKey: cmd.idempotencyKey,
        executionId: cmd.executionId,
        lineId: cmd.lineId,
        deviceId: cmd.payload.deviceId,
        kind: cmd.payload.kind,
        maxAttempts: cmd.maxAttempts,
      }),
    ],
  };
}

export interface LeaseInput {
  leaseholder: string; // gateway id
  leaseId: string; // freshly generated lease token
  now: number;
  leaseDurationMs: number;
  /** Caller's ownership generation. Null skips generation fencing (single-line). */
  callerGeneration?: number | null;
  /** Line's authoritative current generation, read inside the same transaction. */
  currentGeneration?: number | null;
}

/**
 * Acquire (or re-acquire) a lease. Valid when the command is PENDING, or LEASED
 * with an already-expired lease, or LEASED by a SUPERSEDED ownership generation
 * (fast takeover / preemption). Each acquisition bumps `attemptEpoch` and
 * `attempts`, stamps the acquiring `ownerGeneration`, and reuses the stable
 * `executionId`. A caller whose generation is stale is fenced out with no change.
 * If the retry budget is exhausted the command becomes FAILED instead of leased.
 */
export function lease(cmd: Command, input: LeaseInput): Transition {
  const gen = input.callerGeneration ?? null;

  // Fence a stale generation before doing anything else: an old owner that was
  // taken over must not be able to lease (or re-lease) work.
  if (generationIsStale(gen, input.currentGeneration)) {
    return leaseFenced(cmd, input.now, gen, input.currentGeneration ?? null, 'stale_generation');
  }

  if (isTerminal(cmd.status)) {
    return noop(cmd, 'lease_on_terminal');
  }

  // Is the current holder a superseded generation? If so we may preempt it even
  // while its per-command lease is still live (the new owner needs to make
  // progress immediately after takeover).
  const supersededHolder =
    cmd.status === 'LEASED' &&
    cmd.ownerGeneration != null &&
    gen != null &&
    cmd.ownerGeneration < gen;

  if (leaseIsLive(cmd, input.now) && !supersededHolder) {
    // Someone holds a still-valid lease under the current (or an equal/unknown)
    // generation; not available.
    return noop(cmd, 'lease_still_held');
  }

  const events: DomainEvent[] = [];
  if (cmd.status === 'LEASED') {
    if (supersededHolder) {
      events.push(
        event(cmd.id, 'PREEMPTED', input.now, cmd.attemptEpoch, gen, {
          previousLeaseholder: cmd.leaseholder,
          previousLeaseId: cmd.leaseId,
          previousOwnerGeneration: cmd.ownerGeneration,
          newOwnerGeneration: gen,
          executionId: cmd.executionId,
        }),
      );
    } else {
      events.push(
        event(cmd.id, 'LEASE_EXPIRED', input.now, cmd.attemptEpoch, cmd.ownerGeneration, {
          previousLeaseholder: cmd.leaseholder,
          previousLeaseId: cmd.leaseId,
          expiredAt: cmd.leaseExpiresAt,
          via: 'reacquire',
        }),
      );
    }
  }

  const nextAttempts = cmd.attempts + 1;
  if (nextAttempts > cmd.maxAttempts) {
    const failed: Command = {
      ...cmd,
      status: 'FAILED',
      leaseId: null,
      leaseholder: null,
      leaseExpiresAt: null,
      terminalReason: 'retries_exhausted',
      updatedAt: input.now,
    };
    events.push(
      event(cmd.id, 'EXHAUSTED', input.now, cmd.attemptEpoch, cmd.ownerGeneration, {
        attempts: cmd.attempts,
        maxAttempts: cmd.maxAttempts,
      }),
    );
    return {
      command: failed,
      changed: true,
      outcome: { kind: 'expired_failed' },
      events,
    };
  }

  const nextEpoch = cmd.attemptEpoch + 1;
  const leased: Command = {
    ...cmd,
    status: 'LEASED',
    attemptEpoch: nextEpoch,
    attempts: nextAttempts,
    leaseId: input.leaseId,
    leaseholder: input.leaseholder,
    leaseExpiresAt: input.now + input.leaseDurationMs,
    ownerGeneration: gen,
    updatedAt: input.now,
  };
  events.push(
    event(cmd.id, 'LEASED', input.now, nextEpoch, gen, {
      leaseholder: input.leaseholder,
      leaseId: input.leaseId,
      attempt: nextAttempts,
      executionId: cmd.executionId,
      ownerGeneration: gen,
      leaseExpiresAt: leased.leaseExpiresAt,
      preempted: events.some((e) => e.type === 'PREEMPTED'),
    }),
  );
  return {
    command: leased,
    changed: true,
    outcome: events.some((e) => e.type === 'PREEMPTED')
      ? { kind: 'preempted' }
      : { kind: 'leased' },
    events,
  };
}

function leaseFenced(
  cmd: Command,
  now: number,
  callerGeneration: number | null,
  currentGeneration: number | null,
  reason: string,
): Transition {
  return {
    command: cmd,
    changed: false,
    outcome: { kind: 'lease_fenced', reason },
    events: [
      event(cmd.id, 'LEASE_FENCED', now, cmd.attemptEpoch, currentGeneration, {
        reason,
        callerGeneration,
        currentGeneration,
        currentStatus: cmd.status,
      }),
    ],
  };
}

export interface RenewInput {
  leaseId: string;
  now: number;
  leaseDurationMs: number;
  callerGeneration?: number | null;
  currentGeneration?: number | null;
}

/**
 * Extend the current lease. Rejected (no state change, audit event only) unless
 * the caller holds the CURRENT lease, that lease has not already expired, and the
 * caller's ownership generation is not stale — a dead, superseded, or old-
 * generation lease cannot be revived.
 */
export function renew(cmd: Command, input: RenewInput): Transition {
  if (generationIsStale(input.callerGeneration, input.currentGeneration)) {
    return renewRejected(cmd, input.now, 'stale_generation', {
      callerGeneration: input.callerGeneration,
      currentGeneration: input.currentGeneration,
    });
  }
  if (cmd.status !== 'LEASED') {
    return renewRejected(cmd, input.now, 'not_leased', {});
  }
  if (cmd.leaseId !== input.leaseId) {
    return renewRejected(cmd, input.now, 'stale_lease', {});
  }
  if (cmd.leaseExpiresAt === null || cmd.leaseExpiresAt <= input.now) {
    return renewRejected(cmd, input.now, 'lease_expired', {});
  }
  const renewed: Command = {
    ...cmd,
    leaseExpiresAt: input.now + input.leaseDurationMs,
    updatedAt: input.now,
  };
  return {
    command: renewed,
    changed: true,
    outcome: { kind: 'renewed' },
    events: [
      event(cmd.id, 'RENEWED', input.now, cmd.attemptEpoch, cmd.ownerGeneration, {
        leaseId: input.leaseId,
        leaseExpiresAt: renewed.leaseExpiresAt,
      }),
    ],
  };
}

function renewRejected(
  cmd: Command,
  now: number,
  reason: string,
  detail: Record<string, unknown>,
): Transition {
  return {
    command: cmd,
    changed: false,
    outcome: { kind: 'renew_rejected', reason },
    events: [
      event(cmd.id, 'RENEW_REJECTED', now, cmd.attemptEpoch, cmd.ownerGeneration, {
        reason,
        currentStatus: cmd.status,
        ...detail,
      }),
    ],
  };
}

export interface ConfirmInput {
  leaseId: string;
  outcome: ConfirmOutcome;
  now: number;
  deviceReceipt?: string; // opaque device-side receipt for the audit trail
  callerGeneration?: number | null;
  currentGeneration?: number | null;
}

/**
 * Apply a device confirmation relayed by the gateway. This is the ONLY path to
 * SUCCEEDED, and it is accepted only when the confirmation matches the current,
 * still-valid lease AND the caller's ownership generation is not stale. Late
 * confirmations from an expired/superseded lease OR an old generation are ignored
 * (audited as CONFIRM_IGNORED) so a taken-over task is never revived. Duplicate
 * confirmations for an already-terminal command are idempotent.
 */
export function confirm(cmd: Command, input: ConfirmInput): Transition {
  // A stale-generation confirmation must be ignored even if the per-command
  // lease id still matches and is live: this is the split-brain fence. It is
  // checked FIRST so a partitioned old owner cannot push state forward.
  if (generationIsStale(input.callerGeneration, input.currentGeneration)) {
    return confirmIgnored(cmd, input.now, 'stale_generation', {
      callerGeneration: input.callerGeneration,
      currentGeneration: input.currentGeneration,
      outcome: input.outcome,
    });
  }

  // Idempotent re-delivery of a confirmation that already landed.
  if (isTerminal(cmd.status)) {
    return confirmIgnored(cmd, input.now, 'already_terminal', {
      terminalStatus: cmd.status,
      outcome: input.outcome,
    });
  }
  if (cmd.status !== 'LEASED') {
    return confirmIgnored(cmd, input.now, 'no_active_lease', {
      outcome: input.outcome,
    });
  }
  if (cmd.leaseId !== input.leaseId) {
    return confirmIgnored(cmd, input.now, 'stale_lease', {
      expectedLeaseId: cmd.leaseId,
      gotLeaseId: input.leaseId,
      outcome: input.outcome,
    });
  }
  if (cmd.leaseExpiresAt === null || cmd.leaseExpiresAt <= input.now) {
    // The confirmation is for a lease that has already died: do not revive.
    return confirmIgnored(cmd, input.now, 'lease_expired', {
      outcome: input.outcome,
    });
  }

  const succeeded = input.outcome === 'success';
  const next: Command = {
    ...cmd,
    status: succeeded ? 'SUCCEEDED' : 'FAILED',
    leaseId: null,
    leaseholder: null,
    leaseExpiresAt: null,
    terminalReason: succeeded ? 'device_success' : 'device_failure',
    updatedAt: input.now,
  };
  return {
    command: next,
    changed: true,
    outcome: { kind: 'confirmed', outcome: input.outcome },
    events: [
      event(
        cmd.id,
        succeeded ? 'CONFIRMED_SUCCESS' : 'CONFIRMED_FAILURE',
        input.now,
        cmd.attemptEpoch,
        cmd.ownerGeneration,
        {
          leaseId: input.leaseId,
          leaseholder: cmd.leaseholder,
          deviceReceipt: input.deviceReceipt ?? null,
        },
      ),
    ],
  };
}

function confirmIgnored(
  cmd: Command,
  now: number,
  reason: string,
  detail: Record<string, unknown>,
): Transition {
  return {
    command: cmd,
    changed: false,
    outcome: { kind: 'confirm_ignored', reason },
    events: [
      event(cmd.id, 'CONFIRM_IGNORED', now, cmd.attemptEpoch, cmd.ownerGeneration, {
        reason,
        currentStatus: cmd.status,
        ...detail,
      }),
    ],
  };
}

/**
 * Reap an expired lease: either requeue the command (PENDING) for another
 * attempt or, if the retry budget is spent, fail it. Used by the background
 * sweeper. A no-op if the lease is still live or the command is terminal.
 */
export function expire(cmd: Command, now: number): Transition {
  if (cmd.status !== 'LEASED') {
    return noop(cmd, 'expire_not_leased');
  }
  if (cmd.leaseExpiresAt !== null && cmd.leaseExpiresAt > now) {
    return noop(cmd, 'expire_lease_live');
  }

  const events: DomainEvent[] = [
    event(cmd.id, 'LEASE_EXPIRED', now, cmd.attemptEpoch, cmd.ownerGeneration, {
      previousLeaseholder: cmd.leaseholder,
      previousLeaseId: cmd.leaseId,
      expiredAt: cmd.leaseExpiresAt,
      via: 'sweep',
    }),
  ];

  if (cmd.attempts >= cmd.maxAttempts) {
    const failed: Command = {
      ...cmd,
      status: 'FAILED',
      leaseId: null,
      leaseholder: null,
      leaseExpiresAt: null,
      terminalReason: 'retries_exhausted',
      updatedAt: now,
    };
    events.push(
      event(cmd.id, 'EXHAUSTED', now, cmd.attemptEpoch, cmd.ownerGeneration, {
        attempts: cmd.attempts,
        maxAttempts: cmd.maxAttempts,
      }),
    );
    return {
      command: failed,
      changed: true,
      outcome: { kind: 'expired_failed' },
      events,
    };
  }

  const requeued: Command = {
    ...cmd,
    status: 'PENDING',
    leaseId: null,
    leaseholder: null,
    leaseExpiresAt: null,
    updatedAt: now,
  };
  events.push(
    event(cmd.id, 'REQUEUED', now, cmd.attemptEpoch, cmd.ownerGeneration, {
      attempts: cmd.attempts,
      maxAttempts: cmd.maxAttempts,
    }),
  );
  return {
    command: requeued,
    changed: true,
    outcome: { kind: 'expired_requeued' },
    events,
  };
}

/**
 * A benign non-transition. No event is emitted (to avoid audit-log noise for
 * routine cases like "nothing to lease"); callers distinguish cases via the
 * `outcome.kind === 'noop'` return and the debug `reason`.
 */
function noop(cmd: Command, _reason: string): Transition {
  return {
    command: cmd,
    changed: false,
    outcome: { kind: 'noop' },
    events: [],
  };
}
