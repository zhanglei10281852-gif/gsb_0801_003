/**
 * SQLite-backed repository (durable adapter).
 *
 * Durability & atomicity notes that back the delivery guarantee:
 *  - WAL journaling + `synchronous = FULL` so a committed transaction survives a
 *    process crash or power loss (fsync on commit).
 *  - SQLite has a single writer; we wrap each unit of work in an IMMEDIATE
 *    transaction so command/ownership mutation and its causal events commit
 *    atomically.
 *  - The `commands` table has a UNIQUE constraint on `idempotency_key`; the
 *    upstream submit path relies on this to collapse retries into one row even
 *    if two submits race.
 *  - Ownership (line-level代际) is persisted in `line_ownership`, so the current
 *    generation survives a process restart and old generations stay fenced.
 */

import Database from 'better-sqlite3';
import { existsSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { Command, DomainEvent, LineOwnership } from '../domain/types';
import { Repository, RepoTx } from '../ports';

interface CommandRow {
  id: string;
  idempotency_key: string;
  execution_id: string;
  line_id: string;
  payload: string;
  status: string;
  attempt_epoch: number;
  attempts: number;
  max_attempts: number;
  lease_id: string | null;
  leaseholder: string | null;
  lease_expires_at: number | null;
  owner_generation: number | null;
  supersedes_id: string | null;
  terminal_reason: string | null;
  created_at: number;
  updated_at: number;
}

interface EventRow {
  seq: number;
  subject: string;
  subject_id: string;
  command_id: string | null;
  type: string;
  at: number;
  attempt_epoch: number | null;
  generation: number | null;
  detail: string;
}

interface OwnershipRow {
  line_id: string;
  owner: string;
  generation: number;
  expires_at: number;
  created_at: number;
  updated_at: number;
}

function rowToCommand(r: CommandRow): Command {
  return {
    id: r.id,
    idempotencyKey: r.idempotency_key,
    executionId: r.execution_id,
    lineId: r.line_id,
    payload: JSON.parse(r.payload),
    status: r.status as Command['status'],
    attemptEpoch: r.attempt_epoch,
    attempts: r.attempts,
    maxAttempts: r.max_attempts,
    leaseId: r.lease_id,
    leaseholder: r.leaseholder,
    leaseExpiresAt: r.lease_expires_at,
    ownerGeneration: r.owner_generation,
    supersedesId: r.supersedes_id,
    terminalReason: r.terminal_reason,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
  };
}

function rowToEvent(r: EventRow): DomainEvent {
  return {
    subject: r.subject as DomainEvent['subject'],
    subjectId: r.subject_id,
    commandId: r.command_id,
    type: r.type as DomainEvent['type'],
    at: r.at,
    attemptEpoch: r.attempt_epoch,
    generation: r.generation,
    detail: JSON.parse(r.detail),
  };
}

function rowToOwnership(r: OwnershipRow): LineOwnership {
  return {
    lineId: r.line_id,
    owner: r.owner,
    generation: r.generation,
    expiresAt: r.expires_at,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
  };
}

// Base tables. `CREATE TABLE IF NOT EXISTS` is a no-op on an existing (possibly
// older-round) `commands` table; additive columns are then reconciled by
// migrate(), and indexes are created AFTER migration so they can reference
// back-filled columns on an upgraded DB.
const TABLES = `
CREATE TABLE IF NOT EXISTS commands (
  id               TEXT PRIMARY KEY,
  idempotency_key  TEXT NOT NULL UNIQUE,
  execution_id     TEXT NOT NULL,
  line_id          TEXT NOT NULL DEFAULT 'default',
  payload          TEXT NOT NULL,
  status           TEXT NOT NULL,
  attempt_epoch    INTEGER NOT NULL,
  attempts         INTEGER NOT NULL,
  max_attempts     INTEGER NOT NULL,
  lease_id         TEXT,
  leaseholder      TEXT,
  lease_expires_at INTEGER,
  owner_generation INTEGER,
  supersedes_id    TEXT,
  terminal_reason  TEXT,
  created_at       INTEGER NOT NULL,
  updated_at       INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS line_ownership (
  line_id     TEXT PRIMARY KEY,
  owner       TEXT NOT NULL,
  generation  INTEGER NOT NULL,
  expires_at  INTEGER NOT NULL,
  created_at  INTEGER NOT NULL,
  updated_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
  seq           INTEGER PRIMARY KEY AUTOINCREMENT,
  subject       TEXT NOT NULL,
  subject_id    TEXT NOT NULL,
  command_id    TEXT,
  type          TEXT NOT NULL,
  at            INTEGER NOT NULL,
  attempt_epoch INTEGER,
  generation    INTEGER,
  detail        TEXT NOT NULL
);
`;

const INDEXES = `
CREATE INDEX IF NOT EXISTS idx_commands_status ON commands(status);
CREATE INDEX IF NOT EXISTS idx_commands_line ON commands(line_id, status);
CREATE INDEX IF NOT EXISTS idx_commands_lease_expiry ON commands(status, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_commands_supersedes ON commands(supersedes_id);
CREATE INDEX IF NOT EXISTS idx_events_subject ON events(subject_id, seq);
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
    this.db.exec(TABLES);
    this.migrate();
    this.db.exec(INDEXES);
  }

  /**
   * Additive, idempotent migrations so a database created by an earlier round
   * keeps working after an in-place upgrade. `CREATE TABLE IF NOT EXISTS` does
   * NOT add columns to an existing table, so round-2 (`line_id`,
   * `owner_generation`) and round-3 (`supersedes_id`) columns are back-filled
   * here for older files. This keeps the persistence contract consistent across
   * all three rounds' schemas.
   */
  private migrate(): void {
    const cols = new Set(
      (this.db.prepare(`PRAGMA table_info(commands)`).all() as { name: string }[]).map(
        (c) => c.name,
      ),
    );
    const addColumn = (name: string, ddl: string, backfill?: string) => {
      if (!cols.has(name)) {
        this.db.exec(`ALTER TABLE commands ADD COLUMN ${ddl}`);
        if (backfill) this.db.exec(backfill);
      }
    };
    // Round 2:
    addColumn(
      'line_id',
      `line_id TEXT NOT NULL DEFAULT 'default'`,
      `UPDATE commands SET line_id = 'default' WHERE line_id IS NULL`,
    );
    addColumn('owner_generation', `owner_generation INTEGER`);
    // Round 3:
    addColumn('supersedes_id', `supersedes_id TEXT`);
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

  eventsFor(subjectId: string): DomainEvent[] {
    const rows = this.db
      .prepare('SELECT * FROM events WHERE subject_id = ? ORDER BY seq ASC')
      .all(subjectId) as EventRow[];
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

  findPendingIds(
    filter: { lineId?: string; deviceId?: string },
    limit: number,
  ): string[] {
    const { sql, params } = whereClause('PENDING', filter);
    const rows = this.db
      .prepare(
        `SELECT id FROM commands ${sql} ORDER BY created_at ASC LIMIT ?`,
      )
      .all(...params, limit) as { id: string }[];
    return rows.map((r) => r.id);
  }

  findExpiredLeaseIds(
    now: number,
    limit: number,
    filter: { lineId?: string; deviceId?: string } = {},
  ): string[] {
    const clauses = ["status = 'LEASED'", 'lease_expires_at IS NOT NULL', 'lease_expires_at <= ?'];
    const params: unknown[] = [now];
    if (filter.lineId !== undefined) {
      clauses.push('line_id = ?');
      params.push(filter.lineId);
    }
    if (filter.deviceId !== undefined) {
      clauses.push("json_extract(payload, '$.deviceId') = ?");
      params.push(filter.deviceId);
    }
    const rows = this.db
      .prepare(
        `SELECT id FROM commands WHERE ${clauses.join(' AND ')}
         ORDER BY lease_expires_at ASC LIMIT ?`,
      )
      .all(...params, limit) as { id: string }[];
    return rows.map((r) => r.id);
  }

  getOwnership(lineId: string): LineOwnership | null {
    const row = this.db
      .prepare('SELECT * FROM line_ownership WHERE line_id = ?')
      .get(lineId) as OwnershipRow | undefined;
    return row ? rowToOwnership(row) : null;
  }

  listOwnership(): LineOwnership[] {
    const rows = this.db
      .prepare('SELECT * FROM line_ownership ORDER BY line_id ASC')
      .all() as OwnershipRow[];
    return rows.map(rowToOwnership);
  }

  findBySupersedesId(id: string): Command | null {
    const row = this.db
      .prepare('SELECT * FROM commands WHERE supersedes_id = ? ORDER BY created_at ASC LIMIT 1')
      .get(id) as CommandRow | undefined;
    return row ? rowToCommand(row) : null;
  }

  close(): void {
    this.db.close();
  }
}

/** Build a WHERE clause for a status + optional line/device filter. */
function whereClause(
  status: string,
  filter: { lineId?: string; deviceId?: string },
): { sql: string; params: unknown[] } {
  const clauses = ['status = ?'];
  const params: unknown[] = [status];
  if (filter.lineId !== undefined) {
    clauses.push('line_id = ?');
    params.push(filter.lineId);
  }
  if (filter.deviceId !== undefined) {
    clauses.push("json_extract(payload, '$.deviceId') = ?");
    params.push(filter.deviceId);
  }
  return { sql: `WHERE ${clauses.join(' AND ')}`, params };
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
          (id, idempotency_key, execution_id, line_id, payload, status, attempt_epoch,
           attempts, max_attempts, lease_id, leaseholder, lease_expires_at,
           owner_generation, supersedes_id, terminal_reason, created_at, updated_at)
         VALUES
          (@id, @idempotency_key, @execution_id, @line_id, @payload, @status, @attempt_epoch,
           @attempts, @max_attempts, @lease_id, @leaseholder, @lease_expires_at,
           @owner_generation, @supersedes_id, @terminal_reason, @created_at, @updated_at)`,
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
           owner_generation = @owner_generation,
           supersedes_id = @supersedes_id,
           terminal_reason = @terminal_reason,
           updated_at = @updated_at
         WHERE id = @id`,
      )
      .run(this.toRow(c));
  }

  appendEvents(events: DomainEvent[]): void {
    if (events.length === 0) return;
    const stmt = this.db.prepare(
      `INSERT INTO events (subject, subject_id, command_id, type, at, attempt_epoch, generation, detail)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    );
    for (const e of events) {
      stmt.run(
        e.subject,
        e.subjectId,
        e.commandId,
        e.type,
        e.at,
        e.attemptEpoch,
        e.generation,
        JSON.stringify(e.detail),
      );
    }
  }

  getOwnership(lineId: string): LineOwnership | null {
    const row = this.db
      .prepare('SELECT * FROM line_ownership WHERE line_id = ?')
      .get(lineId) as OwnershipRow | undefined;
    return row ? rowToOwnership(row) : null;
  }

  putOwnership(o: LineOwnership): void {
    this.db
      .prepare(
        `INSERT INTO line_ownership (line_id, owner, generation, expires_at, created_at, updated_at)
         VALUES (@line_id, @owner, @generation, @expires_at, @created_at, @updated_at)
         ON CONFLICT(line_id) DO UPDATE SET
           owner = @owner,
           generation = @generation,
           expires_at = @expires_at,
           updated_at = @updated_at`,
      )
      .run({
        line_id: o.lineId,
        owner: o.owner,
        generation: o.generation,
        expires_at: o.expiresAt,
        created_at: o.createdAt,
        updated_at: o.updatedAt,
      });
  }

  private toRow(c: Command): CommandRow {
    return {
      id: c.id,
      idempotency_key: c.idempotencyKey,
      execution_id: c.executionId,
      line_id: c.lineId,
      payload: JSON.stringify(c.payload),
      status: c.status,
      attempt_epoch: c.attemptEpoch,
      attempts: c.attempts,
      max_attempts: c.maxAttempts,
      lease_id: c.leaseId,
      leaseholder: c.leaseholder,
      lease_expires_at: c.leaseExpiresAt,
      owner_generation: c.ownerGeneration,
      supersedes_id: c.supersedesId,
      terminal_reason: c.terminalReason,
      created_at: c.createdAt,
      updated_at: c.updatedAt,
    };
  }
}
