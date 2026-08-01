/**
 * Scriptable simulator CLI.
 *
 * Usage:
 *   node dist/sim/cli.js <scenario> [--url http://127.0.0.1:8080]
 *
 * Scenarios (each drives the *running* service over HTTP and prints a report):
 *   happy            submit -> lease -> confirm success
 *   duplicate-submit submit the same idempotency key repeatedly; expect one cmd
 *   drop-confirm     device acts, confirm is lost; after lease expiry a re-lease
 *                    reuses the same executionId -> device does NOT act twice
 *   vanish           gateway leases then disappears; reaper requeues; another
 *                    gateway finishes it, still one device action
 *   stale-confirm    a late confirmation from an expired lease is ignored
 *   duplicate-confirm at-least-once confirm relay is idempotent
 *
 * The upstream idempotency key is time-seeded so repeated CLI runs against a
 * persistent DB do not collide.
 */

import { httpJson } from './httpClient';
import { SimDevice } from './device';
import { SimGateway } from './gateway';

interface Args {
  scenario: string;
  url: string;
  leaseMs: number;
}

function parseArgs(argv: string[]): Args {
  const scenario = argv[0] ?? 'happy';
  let url = 'http://127.0.0.1:8080';
  let leaseMs = 1500;
  for (let i = 1; i < argv.length; i++) {
    if (argv[i] === '--url') url = argv[++i] ?? url;
    else if (argv[i] === '--lease-ms') leaseMs = Number(argv[++i] ?? leaseMs);
  }
  return { scenario, url, leaseMs };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function log(label: string, data?: unknown): void {
  if (data === undefined) process.stdout.write(`  ${label}\n`);
  else process.stdout.write(`  ${label}: ${JSON.stringify(data)}\n`);
}

async function submit(
  url: string,
  key: string,
  deviceId: string,
  kind: string,
  params: Record<string, unknown> = {},
) {
  return httpJson<{ command: { id: string; status: string }; deduped: boolean }>(
    url,
    'POST',
    '/commands',
    { idempotencyKey: key, deviceId, kind, params },
  );
}

async function getCommand(url: string, id: string) {
  return httpJson<{ command: { status: string; executionId: string } }>(
    url,
    'GET',
    `/commands/${encodeURIComponent(id)}`,
  );
}

async function forceSweep(url: string) {
  return httpJson<{ requeued: number; failed: number }>(url, 'POST', '/ops/sweep');
}

type Scenario = (args: Args) => Promise<boolean>;

const scenarios: Record<string, Scenario> = {
  async happy(args) {
    const key = `happy-${Date.now()}`;
    const dev = `dev-happy-${Date.now()}`;
    const device = new SimDevice(dev);
    const gw = new SimGateway(args.url, 'gw-1', device);
    process.stdout.write('[happy] submit -> lease -> confirm success\n');

    const s = await submit(args.url, key, dev, 'calibrate', { target: 3 });
    log('submitted', { id: s.body.command.id, status: s.body.command.status });

    const handled = await gw.pumpOnce();
    log('gateway handled', handled);

    const after = await getCommand(args.url, s.body.command.id);
    log('final status', after.body.command.status);
    const ok = after.body.command.status === 'SUCCEEDED' && device.distinctActions === 1;
    log('PASS', ok);
    return ok;
  },

  async 'duplicate-submit'(args) {
    const key = `dup-submit-${Date.now()}`;
    const dev = `dev-dupsubmit-${Date.now()}`;
    process.stdout.write('[duplicate-submit] same key 3x -> one command\n');
    const r1 = await submit(args.url, key, dev, 'switch_process', { line: 2 });
    const r2 = await submit(args.url, key, dev, 'switch_process', { line: 2 });
    const r3 = await submit(args.url, key, dev, 'switch_process', { line: 2 });
    log('statuses', [r1.status, r2.status, r3.status]);
    log('ids', [r1.body.command.id, r2.body.command.id, r3.body.command.id]);
    const sameId =
      r1.body.command.id === r2.body.command.id &&
      r2.body.command.id === r3.body.command.id;
    const dedup = r2.body.deduped && r3.body.deduped && !r1.body.deduped;
    const ok = sameId && dedup;
    log('PASS', ok);
    return ok;
  },

  async 'drop-confirm'(args) {
    const key = `drop-${Date.now()}`;
    const dev = `dev-drop-${Date.now()}`;
    const device = new SimDevice(dev);
    const gw1 = new SimGateway(args.url, 'gw-1', device);
    const gw2 = new SimGateway(args.url, 'gw-2', device);
    process.stdout.write(
      '[drop-confirm] device acts, confirm lost; re-lease reuses executionId; no double action\n',
    );
    const s = await submit(args.url, key, dev, 'calibrate');
    const id = s.body.command.id;

    const first = await gw1.pumpOnce({ dropConfirm: true });
    log('gw1 acted, confirm dropped', {
      executionId: first.executionId,
      distinctActions: device.distinctActions,
    });

    // Wait for the lease to expire, then sweep so it becomes leasable again.
    await sleep(args.leaseMs + 300);
    log('sweep', (await forceSweep(args.url)).body);

    const second = await gw2.pumpOnce();
    log('gw2 re-leased + confirmed', {
      executionId: second.executionId,
      duplicate: second.device?.duplicate,
      distinctActions: device.distinctActions,
    });

    const after = await getCommand(args.url, id);
    const ok =
      after.body.command.status === 'SUCCEEDED' &&
      first.executionId === second.executionId && // stable identity reused
      device.distinctActions === 1; // acted exactly once
    log('final status', after.body.command.status);
    log('PASS', ok);
    return ok;
  },

  async vanish(args) {
    const key = `vanish-${Date.now()}`;
    const dev = `dev-vanish-${Date.now()}`;
    const device = new SimDevice(dev);
    const gw1 = new SimGateway(args.url, 'gw-ghost', device);
    const gw2 = new SimGateway(args.url, 'gw-2', device);
    process.stdout.write('[vanish] gateway leases then disappears; another finishes it\n');
    const s = await submit(args.url, key, dev, 'switch_process');
    const id = s.body.command.id;

    const v = await gw1.pumpOnce({ vanishAfterLease: true });
    log('gw-ghost leased then vanished', { executionId: v.executionId });

    await sleep(args.leaseMs + 300);
    log('sweep', (await forceSweep(args.url)).body);

    const done = await gw2.pumpOnce();
    log('gw2 completed', { duplicate: done.device?.duplicate });
    const after = await getCommand(args.url, id);
    const ok =
      after.body.command.status === 'SUCCEEDED' &&
      v.executionId === done.executionId &&
      device.distinctActions === 1;
    log('final status', after.body.command.status);
    log('PASS', ok);
    return ok;
  },

  async 'stale-confirm'(args) {
    const key = `stale-${Date.now()}`;
    const dev = `dev-stale-${Date.now()}`;
    const device = new SimDevice(dev);
    const gw = new SimGateway(args.url, 'gw-1', device);
    process.stdout.write('[stale-confirm] late confirm from an expired lease is ignored\n');
    const s = await submit(args.url, key, dev, 'calibrate');
    const id = s.body.command.id;

    // Lease + capture lease id, then let it expire without confirming.
    const lease = await httpJson<{ leaseId: string; executionId: string }>(
      args.url,
      'POST',
      '/gateway/lease',
      { leaseholder: 'gw-1', deviceId: dev },
    );
    device.execute({ executionId: lease.body.executionId, kind: 'calibrate', params: {} });
    const staleLeaseId = lease.body.leaseId;

    await sleep(args.leaseMs + 300);
    await forceSweep(args.url); // command goes back to PENDING (requeued)

    // Late confirmation with the now-stale lease id must be rejected.
    const late = await gw.confirmRaw(id, staleLeaseId, 'success', 'rcpt_late');
    log('late confirm result', late);

    const after = await getCommand(args.url, id);
    log('status after late confirm', after.body.command.status);
    const ok = !late.accepted && after.body.command.status !== 'SUCCEEDED';
    log('PASS', ok);
    return ok;
  },

  async 'duplicate-confirm'(args) {
    const key = `dupconf-${Date.now()}`;
    const dev = `dev-dupconf-${Date.now()}`;
    const device = new SimDevice(dev);
    const gw = new SimGateway(args.url, 'gw-1', device);
    process.stdout.write('[duplicate-confirm] at-least-once confirm relay is idempotent\n');
    const s = await submit(args.url, key, dev, 'calibrate');
    const id = s.body.command.id;
    const handled = await gw.pumpOnce({ duplicateConfirm: true });
    log('handled (confirmed twice)', {
      confirmAccepted: handled.confirmAccepted,
      distinctActions: device.distinctActions,
    });
    const after = await getCommand(args.url, id);
    const ok = after.body.command.status === 'SUCCEEDED' && device.distinctActions === 1;
    log('final status', after.body.command.status);
    log('PASS', ok);
    return ok;
  },
};

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const scenario = scenarios[args.scenario];
  if (!scenario) {
    process.stderr.write(
      `unknown scenario "${args.scenario}". available: ${Object.keys(scenarios).join(', ')}\n`,
    );
    process.exit(2);
  }
  const ok = await scenario(args);
  process.exit(ok ? 0 : 1);
}

main().catch((err) => {
  process.stderr.write(`simulator error: ${String(err)}\n`);
  process.exit(1);
});
