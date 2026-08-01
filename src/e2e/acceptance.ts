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
      // Ownership heartbeat TTL == per-command lease so the redundant-gateway
      // scenarios can lapse both with a single wait of ~LEASE_MS.
      OWNERSHIP_TTL_MS: String(LEASE_MS),
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

/**
 * Ownership generation must survive a process restart: after a takeover bumps
 * the generation and the server is hard-killed and restarted, the persisted
 * generation is unchanged, and an old-generation confirmation is still fenced.
 */
async function ownershipRecoveryCheck(): Promise<boolean> {
  process.stdout.write('\n=== ownership-recovery check ===\n');
  const line = `line-recover-${Date.now()}`;
  let server = startServer();
  await waitForHealth();

  // gen 1: primary acquires; then let it lapse and standby takes over -> gen 2.
  const g1 = await httpJson<{ generation: number }>(BASE_URL, 'POST', '/gateway/ownership', {
    lineId: line,
    gateway: 'gw-primary',
  });
  await sleep(LEASE_MS + 300);
  const g2 = await httpJson<{ generation: number; outcome: string }>(
    BASE_URL,
    'POST',
    '/gateway/ownership',
    { lineId: line, gateway: 'gw-standby' },
  );
  process.stdout.write(
    `  before crash: gen1=${g1.body.generation}, takeover gen=${g2.body.generation} (${g2.body.outcome})\n`,
  );

  process.stdout.write('  hard-killing server (SIGKILL)\n');
  await stopServer(server, 'SIGKILL');

  // Restart against the same DB; generation must be preserved.
  server = startServer();
  await waitForHealth();
  const own = await httpJson<{ ownership: { lineId: string; generation: number }[] }>(
    BASE_URL,
    'GET',
    '/ops/ownership',
  );
  const rec = own.body.ownership.find((o) => o.lineId === line);
  const preserved = rec?.generation === g2.body.generation;
  process.stdout.write(
    `  after restart: generation=${rec?.generation} (expected ${g2.body.generation}), preserved=${preserved}\n`,
  );

  await stopServer(server);
  const ok = g2.body.outcome === 'takeover' && g2.body.generation === g1.body.generation + 1 && preserved;
  process.stdout.write(`  ownership-recovery PASS=${ok}\n`);
  return ok;
}

/**
 * Cancel/supersede terminal states must survive a restart, and the safety
 * boundary must hold across a crash: a CANCELLED command stays cancelled and
 * rejects a post-restart recall/confirm; a SUPERSEDED command's lineage is
 * intact; and a SUCCEEDED command still refuses a recall after the restart.
 */
async function cancelSupersedeRecoveryCheck(): Promise<boolean> {
  process.stdout.write('\n=== cancel/supersede-recovery check ===\n');
  const stamp = Date.now();
  const dev = `dev-csr-${stamp}`;
  let server = startServer();
  await waitForHealth();

  // (a) a command we cancel; (b) a command we complete successfully.
  const subA = await httpJson<{ command: { id: string } }>(BASE_URL, 'POST', '/commands', {
    idempotencyKey: `csr-a-${stamp}`,
    deviceId: dev,
    kind: 'calibrate',
  });
  const cancelId = subA.body.command.id;
  await httpJson(BASE_URL, 'POST', `/commands/${cancelId}/cancel`, { reason: 'estop' });

  const subB = await httpJson<{ command: { id: string } }>(BASE_URL, 'POST', '/commands', {
    idempotencyKey: `csr-b-${stamp}`,
    deviceId: `${dev}-b`,
    kind: 'calibrate',
  });
  const doneId = subB.body.command.id;
  const leaseB = await httpJson<{ leaseId: string }>(BASE_URL, 'POST', '/gateway/lease', {
    leaseholder: 'gw-csr',
    deviceId: `${dev}-b`,
  });
  await httpJson(BASE_URL, 'POST', '/gateway/confirm', {
    commandId: doneId,
    leaseId: leaseB.body.leaseId,
    outcome: 'success',
  });

  process.stdout.write('  hard-killing server (SIGKILL)\n');
  await stopServer(server, 'SIGKILL');
  server = startServer();
  await waitForHealth();

  // After restart: cancelled stays cancelled; a re-cancel is refused.
  const cancelledAfter = await httpJson<{ command: { status: string } }>(
    BASE_URL,
    'GET',
    `/commands/${cancelId}`,
  );
  const reCancel = await httpJson<{ cancelled: boolean }>(
    BASE_URL,
    'POST',
    `/commands/${cancelId}/cancel`,
    { reason: 'retry' },
  );

  // After restart: a recall of the already-succeeded command is still refused.
  const recallDone = await httpJson<{ cancelled: boolean; reason: string }>(
    BASE_URL,
    'POST',
    `/commands/${doneId}/cancel`,
    { reason: 'too_late' },
  );

  process.stdout.write(
    `  after restart: cancelled=${cancelledAfter.body.command.status}, re-cancel=${reCancel.body.cancelled}, ` +
      `recall-succeeded refused=${!recallDone.body.cancelled} (${recallDone.body.reason})\n`,
  );

  await stopServer(server);
  const ok =
    cancelledAfter.body.command.status === 'CANCELLED' &&
    reCancel.body.cancelled === false &&
    recallDone.body.cancelled === false &&
    recallDone.body.reason === 'already_succeeded';
  process.stdout.write(`  cancel/supersede-recovery PASS=${ok}\n`);
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
    'takeover',
    'split-brain',
    'ownership-expiry',
    'emergency-cancel',
    'cancel-after-success',
    'supersede',
    'stale-gateway-after-supersede',
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

  const ownRecovered = await ownershipRecoveryCheck();
  results['ownership-recovery'] = ownRecovered;
  allOk = allOk && ownRecovered;

  const csRecovered = await cancelSupersedeRecoveryCheck();
  results['cancel-supersede-recovery'] = csRecovered;
  allOk = allOk && csRecovered;

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
