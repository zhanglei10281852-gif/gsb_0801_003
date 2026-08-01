/**
 * End-to-end acceptance runner.
 *
 * This is the real thing: it starts the COMPILED server (dist/main.js) as a
 * child process against a fresh temp SQLite file, then runs every simulator
 * scenario (also compiled, dist/sim/cli.js) against it as separate processes.
 * A scenario "passes" if its child exits 0. Finally it prints a summary and a
 * sample of the causal event log, then exits non-zero if anything failed.
 *
 * It additionally performs a crash-recovery check: it kills the server after a
 * command is persisted, restarts it against the SAME database file, and asserts
 * the command survived and can still be completed exactly once.
 */

import { spawn, ChildProcess } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { rmSync } from 'node:fs';
import { httpJson } from '../sim/httpClient';

const ROOT = join(__dirname, '..', '..'); // dist/e2e -> project root
const SERVER_ENTRY = join(ROOT, 'dist', 'main.js');
const SIM_ENTRY = join(ROOT, 'dist', 'sim', 'cli.js');

const PORT = 8099;
const BASE_URL = `http://127.0.0.1:${PORT}`;
const LEASE_MS = 1200;
const dbFile = join(tmpdir(), `edge-e2e-${process.pid}-${Date.now()}.sqlite`);

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function startServer(): ChildProcess {
  const child = spawn(process.execPath, [SERVER_ENTRY], {
    env: {
      ...process.env,
      PORT: String(PORT),
      DB_FILE: dbFile,
      LEASE_MS: String(LEASE_MS),
      MAX_ATTEMPTS: '5',
      SWEEP_MS: '500',
    },
    stdio: ['ignore', 'inherit', 'inherit'],
  });
  return child;
}

async function waitForHealth(timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const r = await httpJson(BASE_URL, 'GET', '/healthz');
      if (r.status === 200) return;
    } catch {
      /* not up yet */
    }
    await sleep(150);
  }
  throw new Error('server did not become healthy in time');
}

function runScenario(name: string): Promise<boolean> {
  return new Promise((resolve) => {
    const child = spawn(
      process.execPath,
      [SIM_ENTRY, name, '--url', BASE_URL, '--lease-ms', String(LEASE_MS)],
      { stdio: ['ignore', 'inherit', 'inherit'] },
    );
    child.on('exit', (code) => resolve(code === 0));
  });
}

async function stopServer(child: ChildProcess, signal: NodeJS.Signals = 'SIGTERM'): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  await new Promise<void>((resolve) => {
    child.on('exit', () => resolve());
    child.kill(signal);
    setTimeout(() => {
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
      resolve();
    }, 3000);
  });
}

async function crashRecoveryCheck(): Promise<boolean> {
  process.stdout.write('\n=== crash-recovery check ===\n');
  // 1) start, submit a command, then hard-kill the server.
  let server = startServer();
  await waitForHealth();
  const key = `recover-${Date.now()}`;
  const sub = await httpJson<{ command: { id: string; executionId: string } }>(
    BASE_URL,
    'POST',
    '/commands',
    { idempotencyKey: key, deviceId: 'dev-R', kind: 'calibrate' },
  );
  const id = sub.body.command.id;
  process.stdout.write(`  submitted ${id}, hard-killing server (SIGKILL)\n`);
  await stopServer(server, 'SIGKILL');

  // 2) restart against the SAME db file; the command must still be there.
  server = startServer();
  await waitForHealth();
  const got = await httpJson<{ command: { id: string; executionId: string; status: string } }>(
    BASE_URL,
    'GET',
    `/commands/${id}`,
  );
  const survived = got.status === 200 && got.body.command.id === id;
  const sameExec = got.body.command.executionId === sub.body.command.executionId;
  process.stdout.write(
    `  after restart: status=${got.body.command?.status}, survived=${survived}, sameExecutionId=${sameExec}\n`,
  );

  // 3) re-submit with the same key: must dedupe to the same command (no second
  //    device action created by the retry that the crashed response triggered).
  const resub = await httpJson<{ command: { id: string }; deduped: boolean }>(
    BASE_URL,
    'POST',
    '/commands',
    { idempotencyKey: key, deviceId: 'dev-R', kind: 'calibrate' },
  );
  const dedup = resub.body.deduped && resub.body.command.id === id;
  process.stdout.write(`  re-submit after restart deduped=${resub.body.deduped}\n`);

  // 4) complete it exactly once.
  const lease = await httpJson<{ leaseId: string }>(BASE_URL, 'POST', '/gateway/lease', {
    leaseholder: 'gw-recover',
    deviceId: 'dev-R',
  });
  const conf = await httpJson<{ accepted: boolean }>(BASE_URL, 'POST', '/gateway/confirm', {
    commandId: id,
    leaseId: lease.body.leaseId,
    outcome: 'success',
    deviceReceipt: 'r-recover',
  });
  const final = await httpJson<{ command: { status: string } }>(
    BASE_URL,
    'GET',
    `/commands/${id}`,
  );
  const completed = conf.body.accepted && final.body.command.status === 'SUCCEEDED';
  process.stdout.write(`  completed after recovery=${completed}\n`);

  await stopServer(server);
  const ok = survived && sameExec && dedup && completed;
  process.stdout.write(`  crash-recovery PASS=${ok}\n`);
  return ok;
}

async function main(): Promise<void> {
  cleanupDb();
  const scenarios = [
    'happy',
    'duplicate-submit',
    'drop-confirm',
    'vanish',
    'stale-confirm',
    'duplicate-confirm',
  ];

  process.stdout.write(`starting compiled server on ${BASE_URL} (db=${dbFile})\n`);
  const server = startServer();
  const results: Record<string, boolean> = {};
  let allOk = true;
  try {
    await waitForHealth();
    for (const name of scenarios) {
      process.stdout.write(`\n=== scenario: ${name} ===\n`);
      const ok = await runScenario(name);
      results[name] = ok;
      allOk = allOk && ok;
    }

    // Show a slice of the audit trail to demonstrate the causal record.
    const events = await httpJson<{ events: unknown[] }>(BASE_URL, 'GET', '/ops/events?limit=15');
    process.stdout.write('\n=== recent causal events (sample) ===\n');
    process.stdout.write(JSON.stringify(events.body.events, null, 2) + '\n');
  } finally {
    await stopServer(server);
  }

  const recovered = await crashRecoveryCheck();
  results['crash-recovery'] = recovered;
  allOk = allOk && recovered;

  process.stdout.write('\n=== ACCEPTANCE SUMMARY ===\n');
  for (const [name, ok] of Object.entries(results)) {
    process.stdout.write(`  ${ok ? 'PASS' : 'FAIL'}  ${name}\n`);
  }
  cleanupDb();
  if (!allOk) {
    process.stdout.write('\nRESULT: FAIL\n');
    process.exit(1);
  }
  process.stdout.write('\nRESULT: PASS\n');
  process.exit(0);
}

function cleanupDb(): void {
  for (const suffix of ['', '-wal', '-shm', '-journal']) {
    try {
      rmSync(dbFile + suffix, { force: true });
    } catch {
      /* ignore */
    }
  }
}

main().catch((err) => {
  process.stderr.write(`acceptance runner error: ${String(err)}\n`);
  cleanupDb();
  process.exit(1);
});
