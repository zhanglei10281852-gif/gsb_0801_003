/**
 * Application service: orchestrates the pure domain transitions against the
 * Repository/Clock/Id ports inside atomic transactions. This layer is the
 * "seam" between the domain state machine and the outside world; it is unaware
 * of HTTP or any specific gateway transport.
 *
 * Concurrency & crash-safety model:
 *  - Every mutating operation re-reads the command INSIDE the write transaction,
 *    applies the pure transition, and persists command+events atomically. If the
 *    process crashes after commit but before the HTTP response is flushed, the
 *    client simply retries: submit is idempotent by key, lease/renew/confirm are
 *    idempotent by lease token, so no duplicate device action is created.
 */

import * as sm from '../domain/stateMachine';
import { Command, CommandPayload, ConfirmOutcome, DomainEvent } from '../domain/types';
import { Clock, IdGenerator, Repository } from '../ports';

export interface ServiceConfig {
  leaseDurationMs: number;
  maxAttempts: number;
}

export interface SubmitInput {
  idempotencyKey: string;
  deviceId: string;
  kind: string;
  params?: Record<string, unknown>;
  /** Optional per-command override of the retry budget. */
  maxAttempts?: number;
}

export interface SubmitResult {
  command: Command;
  deduped: boolean;
}

export interface LeaseResult {
  command: Command;
  leaseId: string;
  executionId: string;
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

    return this.repo.transaction((tx) => {
      const existing = tx.getByIdempotencyKey(input.idempotencyKey);
      if (existing) {
        tx.appendEvents([
          {
            commandId: existing.id,
            type: 'SUBMIT_DEDUPED',
            at: now,
            attemptEpoch: existing.attemptEpoch,
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
      });
      tx.insert(t.command);
      tx.appendEvents(t.events);
      return { command: t.command, deduped: false };
    });
  }

  /**
   * A gateway leases the oldest available (PENDING or expired-LEASED) command,
   * optionally restricted to a specific device (a gateway typically serves a
   * fixed set of lines/devices). Returns null when nothing is available. The
   * lease carries the stable executionId so a re-lease after a crash reuses the
   * same device identity.
   */
  leaseNext(leaseholder: string, deviceId?: string): LeaseResult | null {
    const now = this.clock.now();
    return this.repo.transaction((tx) => {
      const candidate = this.pickLeasable(tx, now, deviceId);
      if (!candidate) return null;

      const t = sm.lease(candidate, {
        leaseholder,
        leaseId: this.ids.newLeaseId(),
        now,
        leaseDurationMs: this.config.leaseDurationMs,
      });
      if (!t.changed) return null;
      tx.update(t.command);
      tx.appendEvents(t.events);

      // If the retry budget was spent, lease() fails the command instead of
      // leasing it; surface "nothing leased" to the gateway.
      if (t.command.status !== 'LEASED') return null;

      return {
        command: t.command,
        leaseId: t.command.leaseId!,
        executionId: t.command.executionId,
      };
    });
  }

  /**
   * Pick a leasable command inside the transaction. We prefer PENDING, then
   * commands whose lease has already expired (takeover). Reading inside the
   * write tx guarantees two gateways cannot lease the same command.
   */
  private pickLeasable(
    tx: { getById(id: string): Command | null },
    now: number,
    deviceId?: string,
  ): Command | null {
    // The repository's snapshot reads are outside the tx, but they are only
    // used to *find candidate ids*; the authoritative read + mutation happens on
    // `tx` under the write lock, and the pure `lease` transition re-checks
    // liveness, so a stale candidate simply results in a no-op.
    const pendingIds = this.repo.findPendingIds(deviceId, 50);
    for (const id of pendingIds) {
      const fresh = tx.getById(id);
      if (fresh && fresh.status === 'PENDING') return fresh;
    }
    const expiredIds = this.repo.findExpiredLeaseIds(now, 50, deviceId);
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
    return null;
  }

  /** Renew a held lease. Returns the updated command or a rejection reason. */
  renew(
    commandId: string,
    leaseId: string,
  ): { command: Command; renewed: boolean; reason?: string } {
    const now = this.clock.now();
    return this.repo.transaction((tx) => {
      const cmd = tx.getById(commandId);
      if (!cmd) throw new NotFoundError(commandId);
      const t = sm.renew(cmd, {
        leaseId,
        now,
        leaseDurationMs: this.config.leaseDurationMs,
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
   * lease moves the command to a terminal state; anything else is recorded as
   * CONFIRM_IGNORED. Idempotent for already-terminal commands.
   */
  confirm(
    commandId: string,
    leaseId: string,
    outcome: ConfirmOutcome,
    deviceReceipt?: string,
  ): { command: Command; accepted: boolean; reason?: string } {
    const now = this.clock.now();
    return this.repo.transaction((tx) => {
      const cmd = tx.getById(commandId);
      if (!cmd) throw new NotFoundError(commandId);
      const t = sm.confirm(cmd, { leaseId, outcome, now, deviceReceipt });
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

  history(id: string): DomainEvent[] {
    return this.repo.eventsFor(id);
  }

  recentEvents(limit = 100): DomainEvent[] {
    return this.repo.recentEvents(limit);
  }

  list(status?: Command['status'], limit = 100): Command[] {
    return this.repo.listCommands(status, limit);
  }
}

export class NotFoundError extends Error {
  constructor(public readonly id: string) {
    super(`command not found: ${id}`);
    this.name = 'NotFoundError';
  }
}
