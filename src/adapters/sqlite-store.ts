import Database from "better-sqlite3";
import { mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import type { CommandSnapshot, DomainEvent } from "../domain/types.js";
import {
  ConcurrencyError,
  type EventStore,
  type ListEventsFilter,
} from "./event-store.js";

export interface SqliteStoreOptions {
  filePath: string;
  wal?: boolean;
}

function parseEvent(row: Record<string, unknown>): DomainEvent {
  return {
    eventId: row.event_id as string,
    commandId: row.command_id as string,
    version: row.version as number,
    type: row.event_type as string,
    data: JSON.parse(row.data as string) as Record<string, unknown>,
    causedBy: row.caused_by as DomainEvent["causedBy"],
    causationId: (row.causation_id as string | null) ?? undefined,
    occurredAt: row.occurred_at as number,
    recordedAt: row.recorded_at as number,
  };
}

function parseSnapshot(row: Record<string, unknown>): CommandSnapshot {
  const leaseExpiresAt = row.lease_expires_at as number | null;
  return {
    commandId: row.command_id as string,
    idempotencyKey: row.idempotency_key as string,
    deviceId: row.device_id as string,
    payload: JSON.parse(row.payload as string) as CommandSnapshot["payload"],
    state: row.state as CommandSnapshot["state"],
    version: row.version as number,
    attempt: row.attempt as number,
    generation: (row.generation as number | null | undefined) ?? 0,
    gatewayId: (row.gateway_id as string | null) ?? undefined,
    leaseId: (row.lease_id as string | null) ?? undefined,
    leaseExpiresAt: leaseExpiresAt === null ? undefined : leaseExpiresAt,
    deliveredAt: (row.delivered_at as number | null) ?? undefined,
    terminalReason: (row.terminal_reason as string | null) ?? undefined,
    createdAt: row.created_at as number,
    updatedAt: row.updated_at as number,
  };
}

export class SqliteEventStore implements EventStore {
  private readonly db: InstanceType<typeof Database>;

  constructor(options: SqliteStoreOptions) {
    const path = resolve(options.filePath);
    mkdirSync(dirname(path), { recursive: true });
    this.db = new Database(path);
    this.db.pragma("journal_mode = WAL");
    if (options.wal === false) this.db.pragma("journal_mode = DELETE");
    this.db.pragma("foreign_keys = ON");
    this.db.pragma("synchronous = FULL");
    this.migrate();
  }

  private migrate() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY,
        command_id TEXT NOT NULL,
        version INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        data TEXT NOT NULL,
        caused_by TEXT NOT NULL,
        causation_id TEXT,
        occurred_at INTEGER NOT NULL,
        recorded_at INTEGER NOT NULL
      );
      CREATE UNIQUE INDEX IF NOT EXISTS ux_events_command_version
        ON events(command_id, version);
      CREATE INDEX IF NOT EXISTS ix_events_recorded_at ON events(recorded_at);
      CREATE INDEX IF NOT EXISTS ix_events_command_id ON events(command_id);

      CREATE TABLE IF NOT EXISTS commands (
        command_id TEXT PRIMARY KEY,
        idempotency_key TEXT NOT NULL UNIQUE,
        device_id TEXT NOT NULL,
        payload TEXT NOT NULL,
        state TEXT NOT NULL,
        version INTEGER NOT NULL,
        attempt INTEGER NOT NULL,
        generation INTEGER NOT NULL DEFAULT 0,
        gateway_id TEXT,
        lease_id TEXT,
        lease_expires_at INTEGER,
        delivered_at INTEGER,
        terminal_reason TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS ix_commands_state ON commands(state);
      CREATE INDEX IF NOT EXISTS ix_commands_lease ON commands(lease_expires_at);
      CREATE INDEX IF NOT EXISTS ix_commands_device ON commands(device_id);
    `);

    const cols = this.db.prepare("PRAGMA table_info(commands)").all() as {
      name: string;
    }[];
    if (!cols.some((c) => c.name === "generation")) {
      this.db.exec(
        "ALTER TABLE commands ADD COLUMN generation INTEGER NOT NULL DEFAULT 0",
      );
    }
  }

  append(
    commandId: string,
    expectedVersion: number,
    events: DomainEvent[],
    snapshotAfter: CommandSnapshot | undefined,
  ): void {
    if (events.length === 0) return;
    const insertEvent = this.db.prepare(`
      INSERT INTO events (
        event_id, command_id, version, event_type, data,
        caused_by, causation_id, occurred_at, recorded_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const upsertCommand = this.db.prepare(`
      INSERT INTO commands (
        command_id, idempotency_key, device_id, payload, state, version,
        attempt, generation, gateway_id, lease_id, lease_expires_at,
        delivered_at, terminal_reason, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(command_id) DO UPDATE SET
        idempotency_key=excluded.idempotency_key,
        device_id=excluded.device_id,
        payload=excluded.payload,
        state=excluded.state,
        version=excluded.version,
        attempt=excluded.attempt,
        generation=excluded.generation,
        gateway_id=excluded.gateway_id,
        lease_id=excluded.lease_id,
        lease_expires_at=excluded.lease_expires_at,
        delivered_at=excluded.delivered_at,
        terminal_reason=excluded.terminal_reason,
        updated_at=excluded.updated_at
    `);

    const tx = this.db.transaction(() => {
      if (expectedVersion === 0) {
        const existing = this.db
          .prepare("SELECT version FROM commands WHERE command_id = ?")
          .get(commandId) as { version: number } | undefined;
        if (existing)
          throw new ConcurrencyError(commandId, 0, existing.version);
      } else {
        const current = this.db
          .prepare("SELECT version FROM commands WHERE command_id = ?")
          .get(commandId) as { version: number } | undefined;
        if (!current || current.version !== expectedVersion) {
          throw new ConcurrencyError(
            commandId,
            expectedVersion,
            current?.version ?? -1,
          );
        }
      }

      for (const e of events) {
        insertEvent.run(
          e.eventId,
          e.commandId,
          e.version,
          e.type,
          JSON.stringify(e.data),
          e.causedBy,
          e.causationId ?? null,
          e.occurredAt,
          e.recordedAt,
        );
      }

      if (snapshotAfter) {
        upsertCommand.run(
          snapshotAfter.commandId,
          snapshotAfter.idempotencyKey,
          snapshotAfter.deviceId,
          JSON.stringify(snapshotAfter.payload),
          snapshotAfter.state,
          snapshotAfter.version,
          snapshotAfter.attempt,
          snapshotAfter.generation,
          snapshotAfter.gatewayId ?? null,
          snapshotAfter.leaseId ?? null,
          snapshotAfter.leaseExpiresAt ?? null,
          snapshotAfter.deliveredAt ?? null,
          snapshotAfter.terminalReason ?? null,
          snapshotAfter.createdAt,
          snapshotAfter.updatedAt,
        );
      }
    });
    tx();
  }

  getSnapshot(commandId: string): CommandSnapshot | undefined {
    const row = this.db
      .prepare("SELECT * FROM commands WHERE command_id = ?")
      .get(commandId) as Record<string, unknown> | undefined;
    return row ? parseSnapshot(row) : undefined;
  }

  getSnapshotByIdempotencyKey(
    idempotencyKey: string,
  ): CommandSnapshot | undefined {
    const row = this.db
      .prepare("SELECT * FROM commands WHERE idempotency_key = ?")
      .get(idempotencyKey) as Record<string, unknown> | undefined;
    return row ? parseSnapshot(row) : undefined;
  }

  listEvents(filter: ListEventsFilter = {}): DomainEvent[] {
    const clauses: string[] = [];
    const params: unknown[] = [];
    if (filter.commandId) {
      clauses.push("command_id = ?");
      params.push(filter.commandId);
    }
    if (filter.sinceEventId) {
      clauses.push(
        "recorded_at > (SELECT recorded_at FROM events WHERE event_id = ?)",
      );
      params.push(filter.sinceEventId);
    }
    const where = clauses.length > 0 ? `WHERE ${clauses.join(" AND ")}` : "";
    const limit = filter.limit
      ? `LIMIT ${Math.max(1, Math.floor(filter.limit))}`
      : "";
    const rows = this.db
      .prepare(
        `SELECT * FROM events ${where} ORDER BY recorded_at ASC, version ASC ${limit}`,
      )
      .all(...params) as Record<string, unknown>[];
    return rows.map(parseEvent);
  }

  findClaimable({
    now,
    limit = 10,
  }: {
    now: number;
    limit?: number;
  }): CommandSnapshot[] {
    const rows = this.db
      .prepare(
        `
        SELECT * FROM commands
        WHERE state = 'PENDING'
           OR (state = 'CLAIMED' AND lease_expires_at <= ?)
        ORDER BY created_at ASC, command_id ASC
        LIMIT ?
      `,
      )
      .all(now, limit) as Record<string, unknown>[];
    return rows.map(parseSnapshot);
  }

  listSnapshots(): CommandSnapshot[] {
    const rows = this.db
      .prepare("SELECT * FROM commands ORDER BY created_at ASC")
      .all() as Record<string, unknown>[];
    return rows.map(parseSnapshot);
  }

  close() {
    this.db.close();
  }
}
