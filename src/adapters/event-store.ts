import type { CommandSnapshot, DomainEvent } from "../domain/types.js";

export interface ListEventsFilter {
  commandId?: string;
  sinceEventId?: string;
  limit?: number;
}

export interface ClaimableFilter {
  now: number;
  limit?: number;
}

export interface AppendItem {
  commandId: string;
  expectedVersion: number;
  events: DomainEvent[];
  snapshotAfter: CommandSnapshot | undefined;
}

export interface EventStore {
  append(
    commandId: string,
    expectedVersion: number,
    events: DomainEvent[],
    snapshotAfter: CommandSnapshot | undefined,
  ): void;
  appendBatch(items: AppendItem[]): void;
  getSnapshot(commandId: string): CommandSnapshot | undefined;
  getSnapshotByIdempotencyKey(
    idempotencyKey: string,
  ): CommandSnapshot | undefined;
  listEvents(filter?: ListEventsFilter): DomainEvent[];
  findClaimable(filter: ClaimableFilter): CommandSnapshot[];
  listSnapshots(): CommandSnapshot[];
  close(): void;
}

export class ConcurrencyError extends Error {
  constructor(commandId: string, expected: number, actual: number) {
    super(
      `Concurrency conflict for ${commandId}: expected ${expected}, actual ${actual}`,
    );
    this.name = "ConcurrencyError";
  }
}
