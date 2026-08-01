import { randomUUID } from 'node:crypto';
import {
  apply,
  decideAck,
  decideClaim,
  decideExpire,
  decideRenew,
  decideSubmit,
  replay,
  systemEventFactory,
  type EventFactory,
} from '../domain/command.js';
import type {
  CommandPayload,
  CommandSnapshot,
  DomainEvent,
} from '../domain/types.js';
import { ConcurrencyError, type EventStore } from '../adapters/event-store.js';

export interface ServiceConfig {
  leaseDurationMs: number;
  maxAttempts: number;
  maxAgeMs?: number;
  claimBatchSize?: number;
  maxConcurrencyRetries?: number;
}

export interface SubmitResult {
  commandId: string;
  state: CommandSnapshot['state'];
  duplicate: boolean;
  idempotencyKey: string;
  attempt: number;
}

export interface ClaimResult {
  command: CommandSnapshot;
  events: DomainEvent[];
}

export interface OperationResult {
  accepted: boolean;
  reason?: string;
  snapshot?: CommandSnapshot;
  events: DomainEvent[];
}

export class CommandService {
  constructor(
    private readonly store: EventStore,
    private readonly config: ServiceConfig,
    private readonly factory: EventFactory = systemEventFactory
  ) {}

  private withConcurrencyRetry<T>(fn: () => T): T {
    const max = this.config.maxConcurrencyRetries ?? 5;
    let lastErr: unknown;
    for (let i = 0; i < max; i++) {
      try {
        return fn();
      } catch (err) {
        if (err instanceof ConcurrencyError) {
          lastErr = err;
          continue;
        }
        throw err;
      }
    }
    throw lastErr;
  }

  submit(input: {
    idempotencyKey: string;
    deviceId: string;
    payload: CommandPayload;
    commandId?: string;
    submittedAt?: number;
  }): SubmitResult {
    return this.withConcurrencyRetry(() => {
      const existing = this.store.getSnapshotByIdempotencyKey(input.idempotencyKey);
      if (existing) {
        return {
          commandId: existing.commandId,
          state: existing.state,
          duplicate: true,
          idempotencyKey: existing.idempotencyKey,
          attempt: existing.attempt,
        };
      }
      const commandId = input.commandId ?? randomUUID();
      const decision = decideSubmit(
        undefined,
        {
          commandId,
          idempotencyKey: input.idempotencyKey,
          deviceId: input.deviceId,
          payload: input.payload,
          submittedAt: input.submittedAt ?? this.factory.now(),
        },
        this.factory
      );
      if (!decision.accepted) {
        throw new Error(decision.reason ?? 'SUBMIT_REJECTED');
      }
      const snapshot = replay(decision.events)!;
      this.store.append(commandId, 0, decision.events, snapshot);
      return {
        commandId,
        state: snapshot.state,
        duplicate: false,
        idempotencyKey: input.idempotencyKey,
        attempt: 0,
      };
    });
  }

  getCommand(commandId: string): CommandSnapshot | undefined {
    return this.store.getSnapshot(commandId);
  }

  getByIdempotencyKey(key: string): CommandSnapshot | undefined {
    return this.store.getSnapshotByIdempotencyKey(key);
  }

  getEvents(commandId: string): DomainEvent[] {
    return this.store.listEvents({ commandId });
  }

  listEvents(filter?: Parameters<EventStore['listEvents']>[0]): DomainEvent[] {
    return this.store.listEvents(filter);
  }

  listCommands(): CommandSnapshot[] {
    return this.store.listSnapshots();
  }

  claimOne(input: {
    gatewayId: string;
    now?: number;
    maxAttempts?: number;
    leaseDurationMs?: number;
  }): ClaimResult | undefined {
    return this.withConcurrencyRetry(() => {
      const now = input.now ?? this.factory.now();
      const candidates = this.store.findClaimable({
        now,
        limit: this.config.claimBatchSize ?? 10,
      });
      for (const candidate of candidates) {
        const events = this.store.listEvents({ commandId: candidate.commandId });
        const state = replay(events);
        const leaseId = randomUUID();
        const decision = decideClaim(
          state,
          {
            gatewayId: input.gatewayId,
            leaseId,
            leaseDurationMs: input.leaseDurationMs ?? this.config.leaseDurationMs,
            claimedAt: now,
            maxAttempts: input.maxAttempts ?? this.config.maxAttempts,
          },
          this.factory
        );
        if (!decision.accepted) {
          if (decision.events.length > 0) {
            try {
              const partial = replay(events.concat(decision.events));
              this.store.append(candidate.commandId, state?.version ?? 0, decision.events, partial);
            } catch {
              // ignore; another worker likely moved it; continue
            }
          }
          continue;
        }
        const snapshot = replay(events.concat(decision.events))!;
        try {
          this.store.append(candidate.commandId, state?.version ?? 0, decision.events, snapshot);
          return { command: snapshot, events: decision.events };
        } catch (err) {
          if (err instanceof ConcurrencyError) continue;
          throw err;
        }
      }
      return undefined;
    });
  }

  renew(input: {
    commandId: string;
    gatewayId: string;
    leaseId: string;
    leaseDurationMs?: number;
    renewedAt?: number;
  }): OperationResult {
    return this.withConcurrencyRetry(() => {
      const events = this.store.listEvents({ commandId: input.commandId });
      const state = replay(events);
      if (!state) return { accepted: false, reason: 'COMMAND_NOT_FOUND', events: [] };
      const decision = decideRenew(
        state,
        {
          gatewayId: input.gatewayId,
          leaseId: input.leaseId,
          leaseDurationMs: input.leaseDurationMs ?? this.config.leaseDurationMs,
          renewedAt: input.renewedAt ?? this.factory.now(),
        },
        this.factory
      );
      if (!decision.accepted) {
        return { accepted: false, reason: decision.reason, events: decision.events };
      }
      const snapshot = apply(state, decision.events[decision.events.length - 1]);
      this.store.append(input.commandId, state.version, decision.events, snapshot);
      return { accepted: true, snapshot, events: decision.events };
    });
  }

  ack(input: {
    commandId: string;
    gatewayId: string;
    leaseId: string;
    ackCode: string;
    success: boolean;
    ackPayload?: Record<string, unknown>;
    receivedAt?: number;
  }): OperationResult {
    return this.withConcurrencyRetry(() => {
      const events = this.store.listEvents({ commandId: input.commandId });
      const state = replay(events);
      if (!state) return { accepted: false, reason: 'COMMAND_NOT_FOUND', events: [] };
      const decision = decideAck(
        state,
        {
          gatewayId: input.gatewayId,
          leaseId: input.leaseId,
          ackCode: input.ackCode,
          ackPayload: input.ackPayload,
          success: input.success,
          receivedAt: input.receivedAt ?? this.factory.now(),
        },
        this.factory
      );
      if (!decision.accepted) {
        return { accepted: false, reason: decision.reason, events: decision.events };
      }
      const snapshot = apply(state, decision.events[decision.events.length - 1]);
      this.store.append(input.commandId, state.version, decision.events, snapshot);
      return { accepted: true, snapshot, events: decision.events };
    });
  }

  expireOutdated(now?: number): DomainEvent[] {
    const n = now ?? this.factory.now();
    const candidates = this.store.findClaimable({ now: n, limit: 1000 });
    const produced: DomainEvent[] = [];
    for (const candidate of candidates) {
      try {
        const result = this.withConcurrencyRetry(() => {
          const events = this.store.listEvents({ commandId: candidate.commandId });
          const state = replay(events);
          const decision = decideExpire(
            state,
            {
              now: n,
              maxAttempts: this.config.maxAttempts,
              maxAgeMs: this.config.maxAgeMs,
            },
            this.factory
          );
          if (!decision.accepted || decision.events.length === 0) return [];
          const snapshot = replay(events.concat(decision.events));
          this.store.append(candidate.commandId, state?.version ?? 0, decision.events, snapshot);
          return decision.events;
        });
        produced.push(...result);
      } catch (err) {
        if (err instanceof ConcurrencyError) continue;
        throw err;
      }
    }
    return produced;
  }
}
