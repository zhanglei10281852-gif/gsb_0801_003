/**
 * Application service: orchestrates the pure domain transitions against the
 * Repository/Clock/Id ports inside atomic transactions. This layer is the
 * "seam" between the domain state machine and the outside world; it is unaware
 * of HTTP or any specific gateway transport.
 *
 * Concurrency & crash-safety model:
 *  - Every mutating operation re-reads the command (and the line's ownership)
 *    INSIDE the write transaction, applies the pure transition, and persists
 *    command/ownership + events atomically. If the process crashes after commit
 *    but before the HTTP response is flushed, the client simply retries: submit
 *    is idempotent by key, lease/renew/confirm are idempotent by lease token, so
 *    no duplicate device action is created.
 *
 * Redundant-gateway model (line ownership代际):
 *  - A production line is owned by one gateway at a time via a heartbeat lease.
 *    A standby takes over only after the owner's heartbeat lapses; takeover bumps
 *    a monotonic `generation`.
 *  - Every lease/renew/confirm carries the caller's generation. The service reads
 *    the line's authoritative generation inside the same transaction and hands
 *    both to the pure domain, which fences any stale-generation operation. This
 *    is what stops a partitioned old gateway from leasing work or advancing state
 *    with a late confirmation after it has been taken over.
 */

import * as sm from '../domain/stateMachine';
import * as own from '../domain/ownership';
import {
  Command,
  CommandPayload,
  ConfirmOutcome,
  DomainEvent,
  LineOwnership,
} from '../domain/types';
import { Clock, IdGenerator, Repository } from '../ports';

export interface ServiceConfig {
  leaseDurationMs: number;
  maxAttempts: number;
  /** Ownership heartbeat lease TTL (代际 takeover allowed once it lapses). */
  ownershipTtlMs: number;
}

export interface SubmitInput {
  idempotencyKey: string;
  deviceId: string;
  kind: string;
  params?: Record<string, unknown>;
  /** Optional per-command override of the retry budget. */
  maxAttempts?: number;
  /** Production line; defaults to 'default' for single-line deployments. */
  lineId?: string;
}

export interface SubmitResult {
  command: Command;
  deduped: boolean;
}

export interface LeaseResult {
  command: Command;
  leaseId: string;
  executionId: string;
  ownerGeneration: number | null;
  /** True when this lease preempted a superseded generation's in-flight command. */
  preempted: boolean;
}

export interface OwnershipResult {
  ownership: LineOwnership;
  outcome: 'acquired' | 'renewed' | 'takeover' | 'rejected';
  reason?: string;
}

export class DispatchService {
  constructor(
    private readonly repo: Repository,
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
    private readonly config: ServiceConfig,
  ) {}

  /**
   * Upstream submits a command with a business idempotency key. Safe to call
   * repeatedly: the first call creates the command (and its stable executionId),
   * subsequent calls with the same key return the existing command and record a
   * SUBMIT_DEDUPED audit event. This is the guard against a crash between
   * persistence and response turning one request into two device actions.
   */
  submit(input: SubmitInput): SubmitResult {
    const now = this.clock.now();
    const payload: CommandPayload = {
      deviceId: input.deviceId,
      kind: input.kind,
      params: input.params ?? {},
    };
    const maxAttempts = input.maxAttempts ?? this.config.maxAttempts;
    const lineId = input.lineId ?? 'default';

    return this.repo.transaction((tx) => {
      const existing = tx.getByIdempotencyKey(input.idempotencyKey);
      if (existing) {
        tx.appendEvents([
          {
            subject: 'command',
            subjectId: existing.id,
            commandId: existing.id,
            type: 'SUBMIT_DEDUPED',
            at: now,
            attemptEpoch: existing.attemptEpoch,
            generation: existing.ownerGeneration,
            detail: {
              idempotencyKey: input.idempotencyKey,
              currentStatus: existing.status,
            },
          },
        ]);
        return { command: existing, deduped: true };
      }

      const t = sm.create({
        id: this.ids.newCommandId(),
        idempotencyKey: input.idempotencyKey,
        payload,
        now,
        maxAttempts,
        lineId,
      });
      tx.insert(t.command);
      tx.appendEvents(t.events);
      return { command: t.command, deduped: false };
    });
  }

  /**
   * Claim ownership of a line (acquire, heartbeat-renew, or take over an expired
   * owner). Returns the resulting ownership record and the outcome. Takeover
   * bumps the generation; a claim against a still-healthy owner is rejected.
   */
  claimOwnership(lineId: string, claimant: string): OwnershipResult {
    const now = this.clock.now();
    return this.repo.transaction((tx) => {
      const current = tx.getOwnership(lineId);
      const t = current
        ? own.claim(current, {
            lineId,
            claimant,
            now,
            ownershipTtlMs: this.config.ownershipTtlMs,
          })
        : own.acquire({
            lineId,
            claimant,
            now,
            ownershipTtlMs: this.config.ownershipTtlMs,
          });
      if (t.changed) tx.putOwnership(t.ownership);
      tx.appendEvents(t.events);
      return {
        ownership: t.ownership,
        outcome: t.outcome.kind,
        reason: t.outcome.kind === 'rejected' ? t.outcome.reason : undefined,
      };
    });
  }

  /**
   * A gateway leases the oldest available command on a line, optionally further
   * restricted to a device. The caller must supply its ownership generation; a
   * stale generation is fenced out. Returns null when nothing is available or the
   * caller is fenced.
   */
  leaseNext(
    leaseholder: string,
    opts: { lineId?: string; deviceId?: string; callerGeneration?: number | null } = {},
  ): LeaseResult | null {
    const now = this.clock.now();
    const lineId = opts.lineId ?? 'default';
    return this.repo.transaction((tx) => {
      // Authoritative current generation for the line, read in-tx.
      const ownership = tx.getOwnership(lineId);
      const currentGeneration = ownership ? ownership.generation : null;
      const callerGeneration = opts.callerGeneration ?? null;

      const candidate = this.pickLeasable(tx, now, lineId, opts.deviceId);
      if (!candidate) return null;

      const t = sm.lease(candidate, {
        leaseholder,
        leaseId: this.ids.newLeaseId(),
        now,
        leaseDurationMs: this.config.leaseDurationMs,
        callerGeneration,
        currentGeneration,
      });
      if (!t.changed) {
        // A fenced attempt still emits an audit event; persist it.
        tx.appendEvents(t.events);
        return null;
      }
      tx.update(t.command);
      tx.appendEvents(t.events);

      // If the retry budget was spent, lease() fails the command instead of
      // leasing it; surface "nothing leased" to the gateway.
      if (t.command.status !== 'LEASED') return null;

      return {
        command: t.command,
        leaseId: t.command.leaseId!,
        executionId: t.command.executionId,
        ownerGeneration: t.command.ownerGeneration,
        preempted: t.outcome.kind === 'preempted',
      };
    });
  }

  /**
   * Pick a leasable command inside the transaction. We prefer PENDING, then
   * commands whose lease has already expired OR are held by a superseded
   * generation (preemption). Reading inside the write tx guarantees two gateways
   * cannot lease the same command.
   */
  private pickLeasable(
    tx: {
      getById(id: string): Command | null;
      getOwnership(lineId: string): LineOwnership | null;
    },
    now: number,
    lineId: string,
    deviceId?: string,
  ): Command | null {
    const filter = { lineId, deviceId };
    const pendingIds = this.repo.findPendingIds(filter, 50);
    for (const id of pendingIds) {
      const fresh = tx.getById(id);
      if (fresh && fresh.status === 'PENDING') return fresh;
    }
    // Expired-lease takeovers (heartbeat or per-command lease lapsed).
    const expiredIds = this.repo.findExpiredLeaseIds(now, 50, filter);
    for (const id of expiredIds) {
      const fresh = tx.getById(id);
      if (
        fresh &&
        fresh.status === 'LEASED' &&
        fresh.leaseExpiresAt !== null &&
        fresh.leaseExpiresAt <= now
      ) {
        return fresh;
      }
    }
    // Preemption: a command still within its per-command lease but held by a
    // superseded ownership generation. The new owner should take it over
    // immediately rather than wait for the lease to expire.
    const ownership = tx.getOwnership(lineId);
    if (ownership) {
      const leasedIds = this.repo
        .listCommands('LEASED', 200)
        .filter((c) => c.lineId === lineId)
        .map((c) => c.id);
      for (const id of leasedIds) {
        const fresh = tx.getById(id);
        if (
          fresh &&
          fresh.status === 'LEASED' &&
          fresh.ownerGeneration != null &&
          fresh.ownerGeneration < ownership.generation &&
          (deviceId === undefined || fresh.payload.deviceId === deviceId)
        ) {
          return fresh;
        }
      }
    }
    return null;
  }

  /** Renew a held lease. Returns the updated command or a rejection reason. */
  renew(
    commandId: string,
    leaseId: string,
    callerGeneration?: number | null,
  ): { command: Command; renewed: boolean; reason?: string } {
    const now = this.clock.now();
    return this.repo.transaction((tx) => {
      const cmd = tx.getById(commandId);
      if (!cmd) throw new NotFoundError(commandId);
      const ownership = tx.getOwnership(cmd.lineId);
      const t = sm.renew(cmd, {
        leaseId,
        now,
        leaseDurationMs: this.config.leaseDurationMs,
        callerGeneration: callerGeneration ?? null,
        currentGeneration: ownership ? ownership.generation : null,
      });
      if (t.changed) tx.update(t.command);
      tx.appendEvents(t.events);
      if (t.outcome.kind === 'renewed') {
        return { command: t.command, renewed: true };
      }
      return {
        command: t.command,
        renewed: false,
        reason:
          t.outcome.kind === 'renew_rejected' ? t.outcome.reason : 'unknown',
      };
    });
  }

  /**
   * Relay a device confirmation. Only a confirmation matching the current, live
   * lease AND a non-stale ownership generation moves the command to a terminal
   * state; anything else is recorded as CONFIRM_IGNORED. Idempotent for
   * already-terminal commands.
   */
  confirm(
    commandId: string,
    leaseId: string,
    outcome: ConfirmOutcome,
    opts: { deviceReceipt?: string; callerGeneration?: number | null } = {},
  ): { command: Command; accepted: boolean; reason?: string } {
    const now = this.clock.now();
    return this.repo.transaction((tx) => {
      const cmd = tx.getById(commandId);
      if (!cmd) throw new NotFoundError(commandId);
      const ownership = tx.getOwnership(cmd.lineId);
      const t = sm.confirm(cmd, {
        leaseId,
        outcome,
        now,
        deviceReceipt: opts.deviceReceipt,
        callerGeneration: opts.callerGeneration ?? null,
        currentGeneration: ownership ? ownership.generation : null,
      });
      if (t.changed) tx.update(t.command);
      tx.appendEvents(t.events);
      if (t.outcome.kind === 'confirmed') {
        return { command: t.command, accepted: true };
      }
      return {
        command: t.command,
        accepted: false,
        reason:
          t.outcome.kind === 'confirm_ignored' ? t.outcome.reason : 'unknown',
      };
    });
  }

  /**
   * Background reaper: fail or requeue every command whose lease has expired.
   * Each command is handled in its own transaction so one failure cannot block
   * the rest. Returns a small summary for observability. This is what prevents
   * a task from being "revived" by a late message: once swept, the epoch/lease
   * no longer matches and stale confirmations are ignored.
   */
  sweepExpired(batch = 100): { requeued: number; failed: number } {
    const now = this.clock.now();
    const ids = this.repo.findExpiredLeaseIds(now, batch);
    let requeued = 0;
    let failed = 0;
    for (const id of ids) {
      this.repo.transaction((tx) => {
        const cmd = tx.getById(id);
        if (!cmd) return;
        const t = sm.expire(cmd, now);
        if (!t.changed) return;
        tx.update(t.command);
        tx.appendEvents(t.events);
        if (t.outcome.kind === 'expired_requeued') requeued++;
        if (t.outcome.kind === 'expired_failed') failed++;
      });
    }
    return { requeued, failed };
  }

  // --- read-only query surface for upstream + ops ---

  getById(id: string): Command | null {
    return this.repo.getById(id);
  }

  getByIdempotencyKey(key: string): Command | null {
    return this.repo.getByIdempotencyKey(key);
  }

  history(subjectId: string): DomainEvent[] {
    return this.repo.eventsFor(subjectId);
  }

  recentEvents(limit = 100): DomainEvent[] {
    return this.repo.recentEvents(limit);
  }

  list(status?: Command['status'], limit = 100): Command[] {
    return this.repo.listCommands(status, limit);
  }

  getOwnership(lineId: string): LineOwnership | null {
    return this.repo.getOwnership(lineId);
  }

  listOwnership(): LineOwnership[] {
    return this.repo.listOwnership();
  }
}

export class NotFoundError extends Error {
  constructor(public readonly id: string) {
    super(`command not found: ${id}`);
    this.name = 'NotFoundError';
  }
}
