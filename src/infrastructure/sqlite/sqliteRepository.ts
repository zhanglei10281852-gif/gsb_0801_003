import { DB } from "./database.js";
import { Command, CommandEvent } from "../../domain/types.js";
import {
  CommandRepository,
  EventStore,
  UnitOfWork,
} from "../../application/ports.js";

interface CommandRow {
  command_id: string;
  idempotency_key: string;
  payload: string;
  status: string;
  current_lease_id: string | null;
  attempt: number;
  max_attempts: number;
  lease_expires_at: number | null;
  gateway_id: string | null;
  confirmation_code: string | null;
  device_timestamp: number | null;
  failure_reason: string | null;
  cancel_reason: string | null;
  superseded_by_command_id: string | null;
  created_at: number;
  updated_at: number;
}

interface EventRow {
  event_id: string;
  command_id: string;
  event_type: string;
  lease_id: string | null;
  attempt: number | null;
  gateway_id: string | null;
  payload: string | null;
  caused_by: string;
  timestamp: number;
}

function rowToCommand(row: CommandRow): Command {
  return {
    commandId: row.command_id,
    idempotencyKey: row.idempotency_key,
    payload: JSON.parse(row.payload),
    status: row.status as Command["status"],
    currentLeaseId: row.current_lease_id,
    attempt: row.attempt,
    maxAttempts: row.max_attempts,
    leaseExpiresAt: row.lease_expires_at,
    gatewayId: row.gateway_id,
    confirmationCode: row.confirmation_code,
    deviceTimestamp: row.device_timestamp,
    failureReason: row.failure_reason,
    cancelReason: row.cancel_reason,
    supersededByCommandId: row.superseded_by_command_id,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function rowToEvent(row: EventRow): CommandEvent {
  return {
    eventId: row.event_id,
    commandId: row.command_id,
    eventType: row.event_type as CommandEvent["eventType"],
    leaseId: row.lease_id,
    attempt: row.attempt,
    gatewayId: row.gateway_id,
    payload: row.payload ? JSON.parse(row.payload) : null,
    causedBy: row.caused_by,
    timestamp: row.timestamp,
  };
}

export class SqliteCommandRepository implements CommandRepository {
  constructor(private readonly db: DB) {}

  async findByCommandId(commandId: string): Promise<Command | null> {
    const row = this.db
      .prepare("SELECT * FROM commands WHERE command_id = ?")
      .get(commandId) as CommandRow | undefined;
    return row ? rowToCommand(row) : null;
  }

  async findByIdempotencyKey(key: string): Promise<Command | null> {
    const row = this.db
      .prepare("SELECT * FROM commands WHERE idempotency_key = ?")
      .get(key) as CommandRow | undefined;
    return row ? rowToCommand(row) : null;
  }

  async findClaimable(now: number, deviceId?: string): Promise<Command | null> {
    const deviceClause = deviceId
      ? "AND json_extract(payload, '$.deviceId') = ?"
      : "";
    const params: unknown[] = deviceId ? [deviceId, now, deviceId] : [now];
    const row = this.db
      .prepare(
        `
        SELECT * FROM commands
        WHERE (status = 'PENDING' ${deviceClause})
           OR (status = 'CLAIMED'
               AND lease_expires_at IS NOT NULL
               AND lease_expires_at <= ?
               ${deviceClause})
        ORDER BY updated_at ASC
        LIMIT 1
      `,
      )
      .get(...params) as CommandRow | undefined;
    return row ? rowToCommand(row) : null;
  }

  async findExpiredLeases(now: number, limit: number): Promise<Command[]> {
    const rows = this.db
      .prepare(
        `
        SELECT * FROM commands
        WHERE status = 'CLAIMED'
          AND lease_expires_at IS NOT NULL
          AND lease_expires_at <= ?
        ORDER BY updated_at ASC
        LIMIT ?
      `,
      )
      .all(now, limit) as CommandRow[];
    return rows.map(rowToCommand);
  }

  async save(command: Command, events: CommandEvent[]): Promise<void> {
    const upsertCmd = this.db.prepare(`
      INSERT INTO commands (
        command_id, idempotency_key, payload, status, current_lease_id,
        attempt, max_attempts, lease_expires_at, gateway_id,
        confirmation_code, device_timestamp, failure_reason,
        cancel_reason, superseded_by_command_id, created_at, updated_at
      ) VALUES (
        @command_id, @idempotency_key, @payload, @status, @current_lease_id,
        @attempt, @max_attempts, @lease_expires_at, @gateway_id,
        @confirmation_code, @device_timestamp, @failure_reason,
        @cancel_reason, @superseded_by_command_id, @created_at, @updated_at
      )
      ON CONFLICT(command_id) DO UPDATE SET
        status = excluded.status,
        current_lease_id = excluded.current_lease_id,
        attempt = excluded.attempt,
        lease_expires_at = excluded.lease_expires_at,
        gateway_id = excluded.gateway_id,
        confirmation_code = excluded.confirmation_code,
        device_timestamp = excluded.device_timestamp,
        failure_reason = excluded.failure_reason,
        cancel_reason = excluded.cancel_reason,
        superseded_by_command_id = excluded.superseded_by_command_id,
        updated_at = excluded.updated_at
    `);

    const insertEvent = this.db.prepare(`
      INSERT OR IGNORE INTO command_events (
        event_id, command_id, event_type, lease_id, attempt,
        gateway_id, payload, caused_by, timestamp
      ) VALUES (
        @event_id, @command_id, @event_type, @lease_id, @attempt,
        @gateway_id, @payload, @caused_by, @timestamp
      )
    `);

    const saveTxn = this.db.transaction(
      (cmds: Command[], evts: CommandEvent[]) => {
        for (const c of cmds) {
          upsertCmd.run({
            command_id: c.commandId,
            idempotency_key: c.idempotencyKey,
            payload: JSON.stringify(c.payload),
            status: c.status,
            current_lease_id: c.currentLeaseId,
            attempt: c.attempt,
            max_attempts: c.maxAttempts,
            lease_expires_at: c.leaseExpiresAt,
            gateway_id: c.gatewayId,
            confirmation_code: c.confirmationCode,
            device_timestamp: c.deviceTimestamp,
            failure_reason: c.failureReason,
            cancel_reason: c.cancelReason,
            superseded_by_command_id: c.supersededByCommandId,
            created_at: c.createdAt,
            updated_at: c.updatedAt,
          });
        }
        for (const e of evts) {
          insertEvent.run({
            event_id: e.eventId,
            command_id: e.commandId,
            event_type: e.eventType,
            lease_id: e.leaseId,
            attempt: e.attempt,
            gateway_id: e.gatewayId,
            payload: e.payload ? JSON.stringify(e.payload) : null,
            caused_by: e.causedBy,
            timestamp: e.timestamp,
          });
        }
      },
    );

    saveTxn([command], events);
  }

  async saveAll(
    items: Array<{ command: Command; events: CommandEvent[] }>,
  ): Promise<void> {
    for (const item of items) {
      await this.save(item.command, item.events);
    }
  }
}

export class SqliteEventStore implements EventStore {
  constructor(private readonly db: DB) {}

  async append(events: CommandEvent[]): Promise<void> {
    const insert = this.db.prepare(`
      INSERT OR IGNORE INTO command_events (
        event_id, command_id, event_type, lease_id, attempt,
        gateway_id, payload, caused_by, timestamp
      ) VALUES (
        @event_id, @command_id, @event_type, @lease_id, @attempt,
        @gateway_id, @payload, @caused_by, @timestamp
      )
    `);
    const txn = this.db.transaction((evts: CommandEvent[]) => {
      for (const e of evts) {
        insert.run({
          event_id: e.eventId,
          command_id: e.commandId,
          event_type: e.eventType,
          lease_id: e.leaseId,
          attempt: e.attempt,
          gateway_id: e.gatewayId,
          payload: e.payload ? JSON.stringify(e.payload) : null,
          caused_by: e.causedBy,
          timestamp: e.timestamp,
        });
      }
    });
    txn(events);
  }

  async getEvents(commandId: string): Promise<CommandEvent[]> {
    const rows = this.db
      .prepare(
        "SELECT * FROM command_events WHERE command_id = ? ORDER BY timestamp ASC, rowid ASC",
      )
      .all(commandId) as EventRow[];
    return rows.map(rowToEvent);
  }

  async getAllEvents(limit?: number): Promise<CommandEvent[]> {
    const rows = limit
      ? (this.db
          .prepare(
            "SELECT * FROM command_events ORDER BY timestamp ASC, rowid ASC LIMIT ?",
          )
          .all(limit) as EventRow[])
      : (this.db
          .prepare(
            "SELECT * FROM command_events ORDER BY timestamp ASC, rowid ASC",
          )
          .all() as EventRow[]);
    return rows.map(rowToEvent);
  }
}

export class SqliteUnitOfWork implements UnitOfWork {
  constructor(
    private readonly db: DB,
    private readonly commands: SqliteCommandRepository,
    private readonly events: SqliteEventStore,
  ) {}

  async transaction<T>(
    fn: (repos: {
      commands: CommandRepository;
      events: EventStore;
    }) => Promise<T>,
  ): Promise<T> {
    const begin = this.db.prepare("BEGIN IMMEDIATE");
    const commit = this.db.prepare("COMMIT");
    const rollback = this.db.prepare("ROLLBACK");

    begin.run();
    try {
      const result = await fn({ commands: this.commands, events: this.events });
      commit.run();
      return result;
    } catch (err) {
      if (this.db.inTransaction) {
        rollback.run();
      }
      throw err;
    }
  }
}

export function createSqliteAdapters(db: DB) {
  const commands = new SqliteCommandRepository(db);
  const events = new SqliteEventStore(db);
  const uow = new SqliteUnitOfWork(db, commands, events);
  return { commands, events, uow };
}
