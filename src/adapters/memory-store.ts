import type { CommandSnapshot, DomainEvent } from "../domain/types.js";
import {
  type AppendItem,
  ConcurrencyError,
  type EventStore,
  type ListEventsFilter,
} from "./event-store.js";

export class InMemoryEventStore implements EventStore {
  private readonly events = new Map<string, DomainEvent[]>();
  private readonly snapshots = new Map<string, CommandSnapshot>();
  private readonly idempotencyIndex = new Map<string, string>();

  append(
    commandId: string,
    expectedVersion: number,
    events: DomainEvent[],
    snapshotAfter: CommandSnapshot | undefined,
  ): void {
    this.appendBatch([{ commandId, expectedVersion, events, snapshotAfter }]);
  }

  appendBatch(items: AppendItem[]): void {
    for (const item of items) {
      const current = this.snapshots.get(item.commandId);
      const actualVersion = current?.version ?? 0;
      if (actualVersion !== item.expectedVersion) {
        throw new ConcurrencyError(
          item.commandId,
          item.expectedVersion,
          actualVersion,
        );
      }
    }
    for (const item of items) {
      if (item.events.length === 0) continue;
      const list = this.events.get(item.commandId) ?? [];
      for (const e of item.events) list.push(e);
      this.events.set(item.commandId, list);
      if (item.snapshotAfter) {
        const snapshot = structuredClone(item.snapshotAfter);
        this.snapshots.set(item.commandId, snapshot);
        if (snapshot.idempotencyKey) {
          this.idempotencyIndex.set(snapshot.idempotencyKey, item.commandId);
        }
      }
    }
  }

  getSnapshot(commandId: string): CommandSnapshot | undefined {
    const s = this.snapshots.get(commandId);
    return s ? structuredClone(s) : undefined;
  }

  getSnapshotByIdempotencyKey(
    idempotencyKey: string,
  ): CommandSnapshot | undefined {
    const commandId = this.idempotencyIndex.get(idempotencyKey);
    return commandId ? this.getSnapshot(commandId) : undefined;
  }

  listEvents(filter: ListEventsFilter = {}): DomainEvent[] {
    let all: DomainEvent[];
    if (filter.commandId) {
      all = [...(this.events.get(filter.commandId) ?? [])];
    } else {
      all = [...this.snapshots.keys()].flatMap(
        (id) => this.events.get(id) ?? [],
      );
    }
    if (filter.sinceEventId) {
      const sinceEvent = all.find((e) => e.eventId === filter.sinceEventId);
      if (sinceEvent) {
        all = all.filter(
          (e) =>
            e.recordedAt > sinceEvent.recordedAt ||
            (e.recordedAt === sinceEvent.recordedAt &&
              e.version > sinceEvent.version),
        );
      }
    }
    all.sort(
      (a, b) =>
        a.recordedAt - b.recordedAt ||
        a.version - b.version ||
        a.commandId.localeCompare(b.commandId),
    );
    if (filter.limit) all = all.slice(0, filter.limit);
    return all.map((e) => structuredClone(e));
  }

  findClaimable({
    now,
    limit = 10,
  }: {
    now: number;
    limit?: number;
  }): CommandSnapshot[] {
    return [...this.snapshots.values()]
      .filter(
        (c) =>
          c.state === "PENDING" ||
          (c.state === "CLAIMED" && (c.leaseExpiresAt ?? 0) <= now),
      )
      .sort(
        (a, b) =>
          a.createdAt - b.createdAt || a.commandId.localeCompare(b.commandId),
      )
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
