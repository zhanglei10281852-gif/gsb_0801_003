/**
 * Cross-round persistence consistency: a SQLite database created by an EARLIER
 * round's schema must keep working after an in-place upgrade. This test builds a
 * round-1-shaped `commands` table (no line_id / owner_generation / supersedes_id,
 * no line_ownership / events tables), then opens it with the current
 * SqliteRepository and asserts the additive migration back-fills the columns,
 * preserves the old row, and supports round-2/round-3 operations.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { rmSync } from 'node:fs';
import { SqliteRepository } from '../src/adapters/sqliteRepository';
import { DispatchService } from '../src/app/dispatchService';
import { systemClock, uuidIds } from '../src/adapters/clock';

test('a round-1 schema database upgrades in place and stays usable', () => {
  const dbFile = join(tmpdir(), `edge-migrate-${process.pid}-${Date.now()}.sqlite`);
  const cleanup = () => {
    for (const s of ['', '-wal', '-shm', '-journal']) {
      try {
        rmSync(dbFile + s, { force: true });
      } catch {
        /* ignore */
      }
    }
  };

  try {
    // 1) Build a round-1-shaped database by hand and insert one legacy command.
    const raw = new Database(dbFile);
    raw.exec(`
      CREATE TABLE commands (
        id TEXT PRIMARY KEY,
        idempotency_key TEXT NOT NULL UNIQUE,
        execution_id TEXT NOT NULL,
        payload TEXT NOT NULL,
        status TEXT NOT NULL,
        attempt_epoch INTEGER NOT NULL,
        attempts INTEGER NOT NULL,
        max_attempts INTEGER NOT NULL,
        lease_id TEXT,
        leaseholder TEXT,
        lease_expires_at INTEGER,
        terminal_reason TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );
    `);
    raw
      .prepare(
        `INSERT INTO commands
          (id, idempotency_key, execution_id, payload, status, attempt_epoch,
           attempts, max_attempts, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        'legacy_1',
        'legacy-key',
        'legacy_1',
        JSON.stringify({ deviceId: 'd', kind: 'calibrate', params: {} }),
        'PENDING',
        0,
        0,
        5,
        1,
        1,
      );
    raw.close();

    // 2) Open with the current repository: migrate() should back-fill columns.
    const repo = new SqliteRepository(dbFile);
    const legacy = repo.getById('legacy_1');
    assert.ok(legacy, 'legacy row survived the upgrade');
    assert.equal(legacy!.status, 'PENDING');
    assert.equal(legacy!.lineId, 'default'); // back-filled
    assert.equal(legacy!.ownerGeneration, null); // added, null
    assert.equal(legacy!.supersedesId, null); // added, null

    // 3) Round-2 + round-3 features work against the upgraded DB.
    const svc = new DispatchService(repo, systemClock, uuidIds, {
      leaseDurationMs: 1000,
      maxAttempts: 5,
      ownershipTtlMs: 1000,
    });
    // Ownership (round 2) on the new line_ownership table:
    const own = svc.claimOwnership('default', 'gw1');
    assert.equal(own.outcome, 'acquired');
    // Supersede (round 3) the legacy command:
    const repl = svc.submit({
      idempotencyKey: 'legacy-replacement',
      deviceId: 'd',
      kind: 'calibrate',
      supersedesId: 'legacy_1',
    });
    assert.equal(repo.getById('legacy_1')!.status, 'SUPERSEDED');
    const lin = svc.lineage(repl.command.id)!;
    assert.deepEqual(lin.chain.map((c) => c.id), ['legacy_1', repl.command.id]);

    repo.close();
  } finally {
    cleanup();
  }
});
