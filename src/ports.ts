/**
 * Ports: the interfaces the application layer depends on. Concrete adapters
 * (SQLite, in-memory, system clock, uuid) implement these. Keeping the domain
 * and application logic behind ports is what lets the failure timings be tested
 * deterministically with fakes.
 */

import { Command, DomainEvent, LineOwnership } from './domain/types';

/** Injected clock so tests can advance time by hand. */
export interface Clock {
  now(): number;
}

/** Injected id source so tests get stable, predictable ids. */
export interface IdGenerator {
  /** Command / stable execution id. */
  newCommandId(): string;
  /** Per-acquisition lease token. */
  newLeaseId(): string;
}

/**
 * A unit of work executed atomically. The application layer reads state, runs a
 * pure domain transition, and persists the resulting command + events inside a
 * single transaction via this port. `mutate` MUST be all-or-nothing.
 */
export interface Repository {
  /**
   * Run `work` inside a single serialized write transaction. The repository
   * guarantees that concurrent transactions do not interleave (SQLite uses a
   * single writer; the in-memory fake serializes explicitly). If `work` throws,
   * the transaction is rolled back.
   */
  transaction<T>(work: (tx: RepoTx) => T): T;

  /** Read a command by internal id (outside a transaction). */
  getById(id: string): Command | null;
  /** Read a command by upstream idempotency key (outside a transaction). */
  getByIdempotencyKey(key: string): Command | null;
  /** All events for a subject (command id OR line id), in insertion order. */
  eventsFor(subjectId: string): DomainEvent[];
  /** Recent events across all subjects, newest first. */
  recentEvents(limit: number): DomainEvent[];
  /** Snapshot list of commands (optionally filtered by status). */
  listCommands(status?: Command['status'], limit?: number): Command[];
  /**
   * Ids of PENDING commands, oldest first, optionally filtered by line and/or
   * device. Used by the lease path to find the next available task.
   */
  findPendingIds(
    filter: { lineId?: string; deviceId?: string },
    limit: number,
  ): string[];
  /** Ids of commands whose lease has expired as of `now`. */
  findExpiredLeaseIds(
    now: number,
    limit: number,
    filter?: { lineId?: string; deviceId?: string },
  ): string[];

  /** Read a line's ownership record (outside a transaction). */
  getOwnership(lineId: string): LineOwnership | null;
  /** Snapshot list of all line ownership records. */
  listOwnership(): LineOwnership[];

  close(): void;
}

/** The transactional handle passed to `Repository.transaction`. */
export interface RepoTx {
  getById(id: string): Command | null;
  getByIdempotencyKey(key: string): Command | null;
  /** Insert a brand new command. Fails if the id or idempotency key exists. */
  insert(command: Command): void;
  /** Overwrite an existing command row. */
  update(command: Command): void;
  /** Append causal events. */
  appendEvents(events: DomainEvent[]): void;

  /** Read a line's ownership record inside the transaction. */
  getOwnership(lineId: string): LineOwnership | null;
  /** Insert or overwrite a line's ownership record (upsert). */
  putOwnership(ownership: LineOwnership): void;
}
