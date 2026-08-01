/**
 * SQLite 存储适配器(better-sqlite3,WAL + synchronous=FULL)。
 *
 * 恢复边界:状态机的每次决策(状态行 + 确认行 + 所有权行 + 因果事件)在单个事务中
 * 原子落库;服务不依赖任何内存状态,崩溃重启后仅从 SQLite 恢复——包括所有权代际,
 * 因此 fencing 状态跨重启保持。
 */
import Database from 'better-sqlite3';
import { AckRecord, Attempt, Command, DomainEvent, Ownership, StoredEvent } from '../domain/types';

const SCHEMA = `
CREATE TABLE IF NOT EXISTS commands (
  id TEXT PRIMARY KEY,
  idempotency_key TEXT NOT NULL UNIQUE,
  line_id TEXT NOT NULL DEFAULT 'default',
  action TEXT NOT NULL,
  params TEXT NOT NULL,
  status TEXT NOT NULL,
  execution_token TEXT NOT NULL,
  attempt_count INTEGER NOT NULL,
  active_attempt_id TEXT,
  next_attempt_not_before INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS attempts (
  id TEXT PRIMARY KEY,
  command_id TEXT NOT NULL,
  attempt_no INTEGER NOT NULL,
  gateway_id TEXT NOT NULL,
  generation INTEGER NOT NULL DEFAULT 0,
  lease_id TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL,
  leased_at INTEGER NOT NULL,
  lease_expires_at INTEGER NOT NULL,
  renewed_count INTEGER NOT NULL,
  UNIQUE (command_id, attempt_no)
);
CREATE TABLE IF NOT EXISTS acks (
  id TEXT PRIMARY KEY,
  command_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  result TEXT,
  applied INTEGER NOT NULL,
  ignore_reason TEXT,
  received_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS ownerships (
  line_id TEXT PRIMARY KEY,
  owner_gateway_id TEXT NOT NULL,
  generation INTEGER NOT NULL,
  lease_expires_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  command_id TEXT,
  line_id TEXT,
  attempt_id TEXT,
  type TEXT NOT NULL,
  data TEXT NOT NULL,
  caused_by TEXT NOT NULL,
  at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_commands_pending ON commands (status, line_id, next_attempt_not_before);
CREATE INDEX IF NOT EXISTS idx_attempts_lease ON attempts (lease_expires_at) WHERE status = 'ACTIVE';
CREATE INDEX IF NOT EXISTS idx_events_command ON events (command_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_line ON events (line_id, seq);
`;

/* eslint-disable @typescript-eslint/no-explicit-any */
function rowToCommand(r: any): Command {
  return {
    id: r.id,
    idempotencyKey: r.idempotency_key,
    lineId: r.line_id,
    action: r.action,
    params: JSON.parse(r.params),
    status: r.status,
    executionToken: r.execution_token,
    attemptCount: r.attempt_count,
    activeAttemptId: r.active_attempt_id,
    nextAttemptNotBefore: r.next_attempt_not_before,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
  };
}

function rowToAttempt(r: any): Attempt {
  return {
    id: r.id,
    commandId: r.command_id,
    attemptNo: r.attempt_no,
    gatewayId: r.gateway_id,
    generation: r.generation,
    leaseId: r.lease_id,
    status: r.status,
    leasedAt: r.leased_at,
    leaseExpiresAt: r.lease_expires_at,
    renewedCount: r.renewed_count,
  };
}

function rowToAck(r: any): AckRecord {
  return {
    id: r.id,
    commandId: r.command_id,
    attemptId: r.attempt_id,
    result: r.result === null ? null : JSON.parse(r.result),
    applied: r.applied === 1,
    ignoreReason: r.ignore_reason,
    receivedAt: r.received_at,
  };
}

function rowToOwnership(r: any): Ownership {
  return {
    lineId: r.line_id,
    ownerGatewayId: r.owner_gateway_id,
    generation: r.generation,
    leaseExpiresAt: r.lease_expires_at,
    updatedAt: r.updated_at,
  };
}

function rowToEvent(r: any): StoredEvent {
  return {
    seq: r.seq,
    commandId: r.command_id,
    lineId: r.line_id ?? null,
    attemptId: r.attempt_id,
    type: r.type,
    data: JSON.parse(r.data),
    causedBy: r.caused_by,
    at: r.at,
  };
}

export class SqliteRepository {
  private db: Database.Database;

  constructor(path: string) {
    this.db = new Database(path);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = FULL');
    this.db.pragma('busy_timeout = 5000');
    this.db.exec(SCHEMA);
    this.migrate();
  }

  /** 面向既有数据库文件的轻量迁移(补列 / 重建事件表以支持产线维度) */
  private migrate(): void {
    const cols = (t: string) =>
      (this.db.prepare(`PRAGMA table_info(${t})`).all() as any[]).map((c) => c.name as string);
    if (!cols('commands').includes('line_id')) {
      this.db.exec(`ALTER TABLE commands ADD COLUMN line_id TEXT NOT NULL DEFAULT 'default'`);
    }
    if (!cols('attempts').includes('generation')) {
      this.db.exec(`ALTER TABLE attempts ADD COLUMN generation INTEGER NOT NULL DEFAULT 0`);
    }
    if (!cols('events').includes('line_id')) {
      this.db.exec(`
        BEGIN;
        ALTER TABLE events RENAME TO events_old;
        CREATE TABLE events (
          seq INTEGER PRIMARY KEY AUTOINCREMENT,
          command_id TEXT,
          line_id TEXT,
          attempt_id TEXT,
          type TEXT NOT NULL,
          data TEXT NOT NULL,
          caused_by TEXT NOT NULL,
          at INTEGER NOT NULL
        );
        INSERT INTO events (seq, command_id, attempt_id, type, data, caused_by, at, line_id)
          SELECT seq, command_id, attempt_id, type, data, caused_by, at, NULL FROM events_old;
        DROP TABLE events_old;
        CREATE INDEX IF NOT EXISTS idx_events_command ON events (command_id, seq);
        CREATE INDEX IF NOT EXISTS idx_events_line ON events (line_id, seq);
        COMMIT;
      `);
    }
  }

  close(): void {
    this.db.close();
  }

  /** 在单个事务中执行:读状态 → 状态机决策 → 状态+事件原子落库 */
  tx<T>(fn: () => T): T {
    return this.db.transaction(fn)();
  }

  findCommandByKey(key: string): Command | null {
    const r = this.db.prepare('SELECT * FROM commands WHERE idempotency_key = ?').get(key);
    return r ? rowToCommand(r) : null;
  }

  findCommandById(id: string): Command | null {
    const r = this.db.prepare('SELECT * FROM commands WHERE id = ?').get(id);
    return r ? rowToCommand(r) : null;
  }

  findAttemptByLease(leaseId: string): Attempt | null {
    const r = this.db.prepare('SELECT * FROM attempts WHERE lease_id = ?').get(leaseId);
    return r ? rowToAttempt(r) : null;
  }

  findAttemptById(id: string): Attempt | null {
    const r = this.db.prepare('SELECT * FROM attempts WHERE id = ?').get(id);
    return r ? rowToAttempt(r) : null;
  }

  findAckById(ackId: string): AckRecord | null {
    const r = this.db.prepare('SELECT * FROM acks WHERE id = ?').get(ackId);
    return r ? rowToAck(r) : null;
  }

  getOwnership(lineId: string): Ownership | null {
    const r = this.db.prepare('SELECT * FROM ownerships WHERE line_id = ?').get(lineId);
    return r ? rowToOwnership(r) : null;
  }

  upsertOwnership(o: Ownership): void {
    this.db
      .prepare(
        `INSERT INTO ownerships (line_id, owner_gateway_id, generation, lease_expires_at, updated_at)
         VALUES (@lineId, @ownerGatewayId, @generation, @leaseExpiresAt, @updatedAt)
         ON CONFLICT(line_id) DO UPDATE SET
           owner_gateway_id = @ownerGatewayId, generation = @generation,
           lease_expires_at = @leaseExpiresAt, updated_at = @updatedAt`,
      )
      .run(o);
  }

  /** 指定网关在指定产线上仍在飞的投递(供接管时中止) */
  listActiveAttemptsByGatewayOnLine(lineId: string, gatewayId: string): Attempt[] {
    const rows = this.db
      .prepare(
        `SELECT a.* FROM attempts a
         JOIN commands c ON c.id = a.command_id
         WHERE c.line_id = ? AND a.gateway_id = ? AND a.status = 'ACTIVE'`,
      )
      .all(lineId, gatewayId);
    return rows.map(rowToAttempt);
  }

  /** 所有"租约已越过截止时间、但尚未结算"的待派指令(供巡检器/惰性结算) */
  listExpiredDispatched(now: number): Command[] {
    const rows = this.db
      .prepare(
        `SELECT c.* FROM commands c
         JOIN attempts a ON a.id = c.active_attempt_id
         WHERE c.status = 'DISPATCHED' AND a.status = 'ACTIVE' AND a.lease_expires_at < ?`,
      )
      .all(now);
    return rows.map(rowToCommand);
  }

  /** 指定产线上可被领取的指令:PENDING 且到达可领取时间,按提交顺序 */
  listClaimable(lineId: string, now: number, limit: number): Command[] {
    const rows = this.db
      .prepare(
        `SELECT * FROM commands
         WHERE status = 'PENDING' AND line_id = ? AND next_attempt_not_before <= ?
         ORDER BY created_at, id LIMIT ?`,
      )
      .all(lineId, now, limit);
    return rows.map(rowToCommand);
  }

  upsertCommand(c: Command): void {
    this.db
      .prepare(
        `INSERT INTO commands (id, idempotency_key, line_id, action, params, status, execution_token,
                               attempt_count, active_attempt_id, next_attempt_not_before, created_at, updated_at)
         VALUES (@id, @idempotencyKey, @lineId, @action, @params, @status, @executionToken,
                 @attemptCount, @activeAttemptId, @nextAttemptNotBefore, @createdAt, @updatedAt)
         ON CONFLICT(id) DO UPDATE SET
           status = @status, attempt_count = @attemptCount,
           active_attempt_id = @activeAttemptId,
           next_attempt_not_before = @nextAttemptNotBefore, updated_at = @updatedAt`,
      )
      .run({
        id: c.id,
        idempotencyKey: c.idempotencyKey,
        lineId: c.lineId,
        action: c.action,
        params: JSON.stringify(c.params ?? null),
        status: c.status,
        executionToken: c.executionToken,
        attemptCount: c.attemptCount,
        activeAttemptId: c.activeAttemptId,
        nextAttemptNotBefore: c.nextAttemptNotBefore,
        createdAt: c.createdAt,
        updatedAt: c.updatedAt,
      });
  }

  upsertAttempt(a: Attempt): void {
    this.db
      .prepare(
        `INSERT INTO attempts (id, command_id, attempt_no, gateway_id, generation, lease_id, status,
                               leased_at, lease_expires_at, renewed_count)
         VALUES (@id, @commandId, @attemptNo, @gatewayId, @generation, @leaseId, @status,
                 @leasedAt, @leaseExpiresAt, @renewedCount)
         ON CONFLICT(id) DO UPDATE SET
           status = @status, lease_expires_at = @leaseExpiresAt, renewed_count = @renewedCount`,
      )
      .run(a);
  }

  insertAck(a: AckRecord): void {
    this.db
      .prepare(
        `INSERT INTO acks (id, command_id, attempt_id, result, applied, ignore_reason, received_at)
         VALUES (@id, @commandId, @attemptId, @result, @applied, @ignoreReason, @receivedAt)`,
      )
      .run({
        id: a.id,
        commandId: a.commandId,
        attemptId: a.attemptId,
        result: a.result === null || a.result === undefined ? null : JSON.stringify(a.result),
        applied: a.applied ? 1 : 0,
        ignoreReason: a.ignoreReason,
        receivedAt: a.receivedAt,
      });
  }

  insertEvent(e: DomainEvent): void {
    this.db
      .prepare(
        `INSERT INTO events (command_id, line_id, attempt_id, type, data, caused_by, at)
         VALUES (@commandId, @lineId, @attemptId, @type, @data, @causedBy, @at)`,
      )
      .run({ ...e, data: JSON.stringify(e.data ?? {}) });
  }

  listEvents(commandId?: string): StoredEvent[] {
    const rows = commandId
      ? this.db.prepare('SELECT * FROM events WHERE command_id = ? ORDER BY seq').all(commandId)
      : this.db.prepare('SELECT * FROM events ORDER BY seq').all();
    return rows.map(rowToEvent);
  }

  /** 产线维度事件流:还原一次接管前后的完整因果(所有权/争抢/fencing/该线全部指令) */
  listEventsByLine(lineId: string): StoredEvent[] {
    const rows = this.db.prepare('SELECT * FROM events WHERE line_id = ? ORDER BY seq').all(lineId);
    return rows.map(rowToEvent);
  }

  countEvents(commandId: string, type: string): number {
    const r = this.db
      .prepare('SELECT COUNT(*) AS n FROM events WHERE command_id = ? AND type = ?')
      .get(commandId, type) as { n: number };
    return r.n;
  }
}
