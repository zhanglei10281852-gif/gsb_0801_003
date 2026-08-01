/**
 * Composition root: wire the SQLite repository, system clock and uuid generator
 * into the DispatchService, expose it over HTTP, and run the periodic lease
 * reaper. Configuration is read from the environment with safe defaults.
 *
 * On startup nothing special needs to happen for crash recovery: the SQLite
 * file already holds the durable truth. Any command left LEASED whose lease has
 * expired will be requeued by the reaper (or on the next lease attempt), and any
 * committed confirmation already moved its command to a terminal state before
 * the crash. There is no in-memory state to rebuild.
 */

import { DispatchService } from './app/dispatchService';
import { systemClock, uuidIds } from './adapters/clock';
import { SqliteRepository } from './adapters/sqliteRepository';
import { createHttpServer } from './http/server';

function intEnv(name: string, def: number): number {
  const raw = process.env[name];
  if (raw === undefined) return def;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : def;
}

function main(): void {
  const port = intEnv('PORT', 8080);
  const dbFile = process.env.DB_FILE ?? 'data/dispatch.sqlite';
  const leaseDurationMs = intEnv('LEASE_MS', 10_000);
  const maxAttempts = intEnv('MAX_ATTEMPTS', 5);
  const sweepIntervalMs = intEnv('SWEEP_MS', 1_000);
  const ownershipTtlMs = intEnv('OWNERSHIP_TTL_MS', 15_000);

  const repo = new SqliteRepository(dbFile);
  const service = new DispatchService(repo, systemClock, uuidIds, {
    leaseDurationMs,
    maxAttempts,
    ownershipTtlMs,
  });
  const server = createHttpServer(service);

  // Periodic reaper. Requeues/fails expired leases so lost gateways don't strand
  // work. Force-runnable via POST /ops/sweep for deterministic tests.
  const reaper = setInterval(() => {
    try {
      service.sweepExpired();
    } catch (err) {
      process.stderr.write(`[reaper] sweep failed: ${String(err)}\n`);
    }
  }, sweepIntervalMs);
  reaper.unref();

  server.listen(port, () => {
    process.stdout.write(
      `edge-dispatch listening on :${port} ` +
        `(db=${dbFile}, leaseMs=${leaseDurationMs}, maxAttempts=${maxAttempts}, ` +
        `ownershipTtlMs=${ownershipTtlMs})\n`,
    );
  });

  const shutdown = (signal: string) => {
    process.stdout.write(`\n[shutdown] ${signal} received, closing...\n`);
    clearInterval(reaper);
    server.close(() => {
      repo.close();
      process.exit(0);
    });
    // Safety net if connections linger.
    setTimeout(() => process.exit(0), 3_000).unref();
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

main();
