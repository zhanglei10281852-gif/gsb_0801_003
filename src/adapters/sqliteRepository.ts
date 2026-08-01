/**
 * SQLite-backed repository (durable adapter).
 *
 * Durability & atomicity notes that back the delivery guarantee:
 *  - WAL journaling + `synchronous = FULL` so a committed transaction survives a
 *    process crash or power loss (fsync on commit).
 *  - SQLite has a single writer; we wrap each unit of work in an IMMEDIATE
 *    transaction so command mutation and its causal events commit atomically.
 *  - The `commands` table has a UNIQUE constraint on `idempotency_key`; the
 *    upstream submit path relies on this to collapse retries into one row even
 *    if two submits race.
 */

import Database from 'better-sqlite3';
import { existsSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { Command } from '../domain/types';
import { DomainEvent } from '../domain/types';
import { Repository, RepoTx } from '../ports';

interface CommandRow {
  id: string;
  idempotency_key: string;
  execution_id: string;
  payload: string;
  status: string;
  attempt_epoch: number;
  attempts: number;
  max_attempts: number;
  lease_id: string | null;
  leaseholder: string | null;
  lease_expires_at: number | null;
  terminal_reason: string | null;
  created_at: number;
  updated_at: number;
}

interface EventRow {
  seq: number;
  command_id: string;
  type: string;
  at: number;
  attempt_epoch: number | null;
  detail: string;
}

function rowToCommand(r: CommandRow): Command {
  return {
    id: r.id,
    idempotencyKey: r.idempotency_key,
    executionId: r.execution_id,
    payload: JSON.parse(r.payload),
    status: r.status as Command['status'],
    attemptEpoch: r.attempt_epoch,
    attempts: r.attempts,
    maxAttempts: r.max_attempts,
    leaseId: r.lease_id,
    leaseholder: r.leaseholder,
    leaseExpiresAt: r.lease_expires_at,
    terminalReason: r.terminal_reason,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
  };
}

function rowToEvent(r: EventRow): DomainEvent {
  return {
    commandId: r.command_id,
    type: r.type as DomainEvent['type'],
    at: r.at,
    attemptEpoch: r.attempt_epoch,
    detail: JSON.parse(r.detail),
  };
}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS commands (
  id               TEXT PRIMARY KEY,
  idempotency_key  TEXT NOT NULL UNIQUE,
  execution_id     TEXT NOT NULL,
  payload          TEXT NOT NULL,
  status           TEXT NOT NULL,
  attempt_epoch    INTEGER NOT NULL,
  attempts         INTEGER NOT NULL,
  max_attempts     INTEGER NOT NULL,
  lease_id         TEXT,
  leaseholder      TEXT,
  lease_expires_at INTEGER,
  terminal_reason  TEXT,
  created_at       INTEGER NOT NULL,
  updated_at       INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_commands_status ON commands(status);
CREATE INDEX IF NOT EXISTS idx_commands_lease_expiry ON commands(status, lease_expires_at);

CREATE TABLE IF NOT EXISTS events (
  seq           INTEGER PRIMARY KEY AUTOINCREMENT,
  command_id    TEXT NOT NULL,
  type          TEXT NOT NULL,
  at            INTEGER NOT NULL,
  attempt_epoch INTEGER,
  detail        TEXT NOT NULL,
  FOREIGN KEY (command_id) REFERENCES commands(id)
);

CREATE INDEX IF NOT EXISTS idx_events_command ON events(command_id, seq);
`;

export class SqliteRepository implements Repository {
  private readonly db: Database.Database;

  constructor(filename: string) {
    if (filename !== ':memory:') {
      const dir = dirname(filename);
      if (dir && dir !== '.' && !existsSync(dir)) {
        mkdirSync(dir, { recursive: true });
      }
    }
    this.db = new Database(filename);
    this.db.pragma('journal_mode = WAL');
    // Durability over throughput: fsync on every commit so an acknowledged
    // write cannot be lost by a crash. This is the crux of the "persisted
    // before responding" guarantee.
    this.db.pragma('synchronous = FULL');
    this.db.pragma('foreign_keys = ON');
    this.db.pragma('busy_timeout = 5000');
    this.db.exec(SCHEMA);
  }

  transaction<T>(work: (tx: RepoTx) => T): T {
    const tx = new SqliteTx(this.db);
    // IMMEDIATE acquires the write lock up front, avoiding a read-then-upgrade
    // deadlock and ensuring serialized writers.
    const run = this.db.transaction(() => work(tx));
    return run.immediate();
  }

  getById(id: string): Command | null {
    const row = this.db
      .prepare('SELECT * FROM commands WHERE id = ?')
      .get(id) as CommandRow | undefined;
    return row ? rowToCommand(row) : null;
  }

  getByIdempotencyKey(key: string): Command | null {
    const row = this.db
      .prepare('SELECT * FROM commands WHERE idempotency_key = ?')
      .get(key) as CommandRow | undefined;
    return row ? rowToCommand(row) : null;
  }

  eventsFor(id: string): DomainEvent[] {
    const rows = this.db
      .prepare('SELECT * FROM events WHERE command_id = ? ORDER BY seq ASC')
      .all(id) as EventRow[];
    return rows.map(rowToEvent);
  }

  recentEvents(limit: number): DomainEvent[] {
    const rows = this.db
      .prepare('SELECT * FROM events ORDER BY seq DESC LIMIT ?')
      .all(limit) as EventRow[];
    return rows.map(rowToEvent);
  }

  listCommands(status?: Command['status'], limit = 100): Command[] {
    const rows = status
      ? (this.db
          .prepare(
            'SELECT * FROM commands WHERE status = ? ORDER BY created_at ASC LIMIT ?',
          )
          .all(status, limit) as CommandRow[])
      : (this.db
          .prepare('SELECT * FROM commands ORDER BY created_at ASC LIMIT ?')
          .all(limit) as CommandRow[]);
    return rows.map(rowToCommand);
  }

  findPendingIds(deviceId: string | undefined, limit: number): string[] {
    // payload is JSON; deviceId is stored inside it. json_extract keeps the
    // filter in SQL so we don't scan/parse in JS.
    const rows = deviceId
      ? (this.db
          .prepare(
            `SELECT id FROM commands
             WHERE status = 'PENDING' AND json_extract(payload, '$.deviceId') = ?
             ORDER BY created_at ASC LIMIT ?`,
          )
          .all(deviceId, limit) as { id: string }[])
      : (this.db
          .prepare(
            `SELECT id FROM commands WHERE status = 'PENDING'
             ORDER BY created_at ASC LIMIT ?`,
          )
          .all(limit) as { id: string }[]);
    return rows.map((r) => r.id);
  }

  findExpiredLeaseIds(now: number, limit: number, deviceId?: string): string[] {
    const rows = deviceId
      ? (this.db
          .prepare(
            `SELECT id FROM commands
             WHERE status = 'LEASED' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?
               AND json_extract(payload, '$.deviceId') = ?
             ORDER BY lease_expires_at ASC LIMIT ?`,
          )
          .all(now, deviceId, limit) as { id: string }[])
      : (this.db
          .prepare(
            `SELECT id FROM commands
             WHERE status = 'LEASED' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?
             ORDER BY lease_expires_at ASC LIMIT ?`,
          )
          .all(now, limit) as { id: string }[]);
    return rows.map((r) => r.id);
  }

  close(): void {
    this.db.close();
  }
}

class SqliteTx implements RepoTx {
  constructor(private readonly db: Database.Database) {}

  getById(id: string): Command | null {
    const row = this.db
      .prepare('SELECT * FROM commands WHERE id = ?')
      .get(id) as CommandRow | undefined;
    return row ? rowToCommand(row) : null;
  }

  getByIdempotencyKey(key: string): Command | null {
    const row = this.db
      .prepare('SELECT * FROM commands WHERE idempotency_key = ?')
      .get(key) as CommandRow | undefined;
    return row ? rowToCommand(row) : null;
  }

  insert(c: Command): void {
    this.db
      .prepare(
        `INSERT INTO commands
          (id, idempotency_key, execution_id, payload, status, attempt_epoch,
           attempts, max_attempts, lease_id, leaseholder, lease_expires_at,
           terminal_reason, created_at, updated_at)
         VALUES
          (@id, @idempotency_key, @execution_id, @payload, @status, @attempt_epoch,
           @attempts, @max_attempts, @lease_id, @leaseholder, @lease_expires_at,
           @terminal_reason, @created_at, @updated_at)`,
      )
      .run(this.toRow(c));
  }

  update(c: Command): void {
    this.db
      .prepare(
        `UPDATE commands SET
           status = @status,
           attempt_epoch = @attempt_epoch,
           attempts = @attempts,
           max_attempts = @max_attempts,
           lease_id = @lease_id,
           leaseholder = @leaseholder,
           lease_expires_at = @lease_expires_at,
           terminal_reason = @terminal_reason,
           updated_at = @updated_at
         WHERE id = @id`,
      )
      .run(this.toRow(c));
  }

  appendEvents(events: DomainEvent[]): void {
    if (events.length === 0) return;
    const stmt = this.db.prepare(
      `INSERT INTO events (command_id, type, at, attempt_epoch, detail)
       VALUES (?, ?, ?, ?, ?)`,
    );
    for (const e of events) {
      stmt.run(e.commandId, e.type, e.at, e.attemptEpoch, JSON.stringify(e.detail));
    }
  }

  private toRow(c: Command): CommandRow {
    return {
      id: c.id,
      idempotency_key: c.idempotencyKey,
      execution_id: c.executionId,
      payload: JSON.stringify(c.payload),
      status: c.status,
      attempt_epoch: c.attemptEpoch,
      attempts: c.attempts,
      max_attempts: c.maxAttempts,
      lease_id: c.leaseId,
      leaseholder: c.leaseholder,
      lease_expires_at: c.leaseExpiresAt,
      terminal_reason: c.terminalReason,
      created_at: c.createdAt,
      updated_at: c.updatedAt,
    };
  }
}
