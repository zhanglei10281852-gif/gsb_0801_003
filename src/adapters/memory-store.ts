import type { CommandSnapshot, DomainEvent } from '../domain/types.js';
import {
  ConcurrencyError,
  type EventStore,
  type ListEventsFilter,
} from './event-store.js';

export class InMemoryEventStore implements EventStore {
  private readonly events = new Map<string, DomainEvent[]>();
  private readonly snapshots = new Map<string, CommandSnapshot>();
  private readonly idempotencyIndex = new Map<string, string>();

  append(
    commandId: string,
    expectedVersion: number,
    events: DomainEvent[],
    snapshotAfter: CommandSnapshot | undefined
  ): void {
    const current = this.snapshots.get(commandId);
    const actualVersion = current?.version ?? 0;
    if (actualVersion !== expectedVersion) {
      throw new ConcurrencyError(commandId, expectedVersion, actualVersion);
    }
    const list = this.events.get(commandId) ?? [];
    for (const e of events) list.push(e);
    this.events.set(commandId, list);
    if (snapshotAfter) {
      this.snapshots.set(commandId, snapshotAfter);
      if (snapshotAfter.idempotencyKey) {
        this.idempotencyIndex.set(snapshotAfter.idempotencyKey, commandId);
      }
    }
  }

  getSnapshot(commandId: string): CommandSnapshot | undefined {
    const s = this.snapshots.get(commandId);
    return s ? structuredClone(s) : undefined;
  }

  getSnapshotByIdempotencyKey(idempotencyKey: string): CommandSnapshot | undefined {
    const commandId = this.idempotencyIndex.get(idempotencyKey);
    return commandId ? this.getSnapshot(commandId) : undefined;
  }

  listEvents(filter: ListEventsFilter = {}): DomainEvent[] {
    let all: DomainEvent[];
    if (filter.commandId) {
      all = [...(this.events.get(filter.commandId) ?? [])];
    } else {
      all = [...this.snapshots.keys()].flatMap((id) => this.events.get(id) ?? []);
    }
    if (filter.sinceEventId) {
      const sinceEvent = all.find((e) => e.eventId === filter.sinceEventId);
      if (sinceEvent) {
        all = all.filter(
          (e) => e.recordedAt > sinceEvent.recordedAt ||
                 (e.recordedAt === sinceEvent.recordedAt && e.version > sinceEvent.version)
        );
      }
    }
    all.sort((a, b) =>
      a.recordedAt - b.recordedAt || a.version - b.version || a.commandId.localeCompare(b.commandId)
    );
    if (filter.limit) all = all.slice(0, filter.limit);
    return all.map((e) => structuredClone(e));
  }

  findClaimable({ now, limit = 10 }: { now: number; limit?: number }): CommandSnapshot[] {
    return [...this.snapshots.values()]
      .filter(
        (c) =>
          c.state === 'PENDING' ||
          (c.state === 'CLAIMED' && (c.leaseExpiresAt ?? 0) <= now)
      )
      .sort((a, b) => a.createdAt - b.createdAt || a.commandId.localeCompare(b.commandId))
      .slice(0, limit)
      .map((s) => structuredClone(s));
  }

  listSnapshots(): CommandSnapshot[] {
    return [...this.snapshots.values()]
      .sort((a, b) => a.createdAt - b.createdAt)
      .map((s) => structuredClone(s));
  }

  close() {
    this.events.clear();
    this.snapshots.clear();
    this.idempotencyIndex.clear();
  }
}
