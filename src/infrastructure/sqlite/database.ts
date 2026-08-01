import Database from 'better-sqlite3';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import fs from 'node:fs';

const SCHEMA = `
CREATE TABLE IF NOT EXISTS commands (
  command_id TEXT PRIMARY KEY,
  idempotency_key TEXT NOT NULL UNIQUE,
  payload TEXT NOT NULL,
  status TEXT NOT NULL,
  current_lease_id TEXT,
  attempt INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL,
  lease_expires_at INTEGER,
  gateway_id TEXT,
  confirmation_code TEXT,
  device_timestamp INTEGER,
  failure_reason TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS command_events (
  event_id TEXT PRIMARY KEY,
  command_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  lease_id TEXT,
  attempt INTEGER,
  gateway_id TEXT,
  payload TEXT,
  caused_by TEXT NOT NULL,
  timestamp INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_command ON command_events(command_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_events_type ON command_events(event_type);
CREATE INDEX IF NOT EXISTS idx_commands_status ON commands(status, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_commands_updated ON commands(updated_at);
`;

export function openDatabase(dbPath: string): Database.Database {
  const dir = path.dirname(dbPath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = FULL');
  db.pragma('foreign_keys = ON');
  db.exec(SCHEMA);
  return db;
}

export function closeDatabase(db: Database.Database): void {
  db.pragma('wal_checkpoint(TRUNCATE)');
  db.close();
}

export type DB = Database.Database;
