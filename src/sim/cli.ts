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
 *   takeover         primary owns a line and vanishes; standby takes over (代际++),
 *                    finishes the work; still one device action
 *   split-brain      partitioned old primary keeps its stale generation; after a
 *                    standby takes over, the old primary can neither lease nor
 *                    push a confirmation through (fenced by generation)
 *   ownership-expiry a standby is refused while the primary is healthy, then
 *                    allowed to take over once the primary's heartbeat lapses
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
  lineId?: string,
) {
  return httpJson<{ command: { id: string; status: string }; deduped: boolean }>(
    url,
    'POST',
    '/commands',
    { idempotencyKey: key, deviceId, kind, params, lineId },
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
    const late = await gw.confirmRaw(id, staleLeaseId, 'success', {
      receipt: 'rcpt_late',
    });
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

  // --- redundant-gateway scenarios ---

  async takeover(args) {
    const stamp = Date.now();
    const line = `line-takeover-${stamp}`;
    const dev = `dev-takeover-${stamp}`;
    const key = `takeover-${stamp}`;
    // Primary and standby share ONE physical device (they serve the same line).
    const device = new SimDevice(dev);
    const primary = new SimGateway(args.url, 'gw-primary', device, line);
    const standby = new SimGateway(args.url, 'gw-standby', device, line);
    process.stdout.write(
      '[takeover] primary owns line, leases + vanishes; standby takes over (代际++) and finishes\n',
    );

    const own1 = await primary.acquireOwnership();
    log('primary acquired ownership', { generation: own1.generation });
    const s = await submit(args.url, key, dev, 'calibrate', {}, line);
    const id = s.body.command.id;

    // Primary leases the task then vanishes (no confirm, no heartbeat).
    const v = await primary.pumpOnce({ vanishAfterLease: true });
    log('primary leased then vanished', { executionId: v.executionId });

    // Standby cannot take over yet (primary ownership still healthy)...
    const early = await standby.acquireOwnership();
    log('standby early claim (expect rejected)', {
      outcome: early.outcome,
      generation: early.generation,
    });

    // ...wait for the ownership heartbeat to lapse, then take over.
    await sleep(args.leaseMs + 300); // ownership TTL is set == leaseMs in e2e
    const own2 = await standby.acquireOwnership();
    log('standby takeover', { outcome: own2.outcome, generation: own2.generation });

    // Reap the primary's expired per-command lease, then standby finishes it.
    await forceSweep(args.url);
    const done = await standby.pumpOnce();
    log('standby handled', {
      executionId: done.executionId,
      duplicate: done.device?.duplicate,
      distinctActions: device.distinctActions,
    });

    const after = await getCommand(args.url, id);
    const ok =
      early.outcome === 'rejected' &&
      own2.outcome === 'takeover' &&
      own2.generation === own1.generation + 1 &&
      after.body.command.status === 'SUCCEEDED' &&
      v.executionId === done.executionId && // stable identity across takeover
      device.distinctActions === 1; // acted exactly once
    log('final status', after.body.command.status);
    log('PASS', ok);
    return ok;
  },

  async 'split-brain'(args) {
    const stamp = Date.now();
    const line = `line-split-${stamp}`;
    const dev = `dev-split-${stamp}`;
    const key = `split-${stamp}`;
    const device = new SimDevice(dev);
    const oldPrimary = new SimGateway(args.url, 'gw-old', device, line);
    const standby = new SimGateway(args.url, 'gw-new', device, line);
    process.stdout.write(
      '[split-brain] partitioned old primary keeps stale generation; fenced after takeover\n',
    );

    const own1 = await oldPrimary.acquireOwnership();
    const s = await submit(args.url, key, dev, 'calibrate', {}, line);
    const id = s.body.command.id;

    // Old primary leases the task (generation 1) and drives the device, but the
    // network partitions BEFORE it can confirm. Capture its lease id.
    const leased = await oldPrimary.pumpOnce({ dropConfirm: true });
    log('old primary leased + acted, confirm lost', {
      generation: own1.generation,
      distinctActions: device.distinctActions,
    });
    const staleLeaseId = leased.leaseId!;

    // Meanwhile its heartbeat lapses and the standby takes over (generation 2).
    await sleep(args.leaseMs + 300);
    const own2 = await standby.acquireOwnership();
    log('standby takeover', { outcome: own2.outcome, generation: own2.generation });

    // The partition heals. The OLD primary, still believing it holds generation
    // 1 and its lease, tries to (a) lease more work and (b) push its confirm.
    // Both must be fenced by generation.
    oldPrimary.forceGeneration(own1.generation); // it never learned it lost
    const staleLease = await httpJson<{ commandId?: string }>(
      args.url,
      'POST',
      '/gateway/lease',
      { leaseholder: 'gw-old', lineId: line, deviceId: dev, generation: own1.generation },
    );
    log('old primary lease attempt (expect 204 fenced)', { status: staleLease.status });

    const staleConfirm = await oldPrimary.confirmRaw(id, staleLeaseId, 'success', {
      generation: own1.generation,
      receipt: 'rcpt_old',
    });
    log('old primary confirm attempt (expect rejected)', staleConfirm);

    // The new owner reaps + finishes the command legitimately.
    await forceSweep(args.url);
    const done = await standby.pumpOnce();
    log('standby completed', { duplicate: done.device?.duplicate });

    const after = await getCommand(args.url, id);
    const ok =
      own2.generation === own1.generation + 1 &&
      staleLease.status === 204 && // old generation could not lease
      !staleConfirm.accepted && // old generation confirm fenced
      staleConfirm.reason === 'stale_generation' &&
      after.body.command.status === 'SUCCEEDED' &&
      device.distinctActions === 1; // device acted exactly once despite the mess
    log('final status', after.body.command.status);
    log('PASS', ok);
    return ok;
  },

  async 'ownership-expiry'(args) {
    const stamp = Date.now();
    const line = `line-ownexp-${stamp}`;
    const dev = `dev-ownexp-${stamp}`;
    const device = new SimDevice(dev);
    const primary = new SimGateway(args.url, 'gw-p', device, line);
    const standby = new SimGateway(args.url, 'gw-s', device, line);
    process.stdout.write(
      '[ownership-expiry] standby refused while primary healthy, allowed after heartbeat lapses\n',
    );

    const own1 = await primary.acquireOwnership();
    log('primary owns', { generation: own1.generation });

    // Standby tries immediately: must be rejected (primary healthy).
    const refused = await standby.acquireOwnership();
    log('standby claim while healthy (expect rejected)', {
      outcome: refused.outcome,
    });

    // Primary heartbeats once to prove renew keeps generation stable.
    const hb = await primary.acquireOwnership();
    log('primary heartbeat', { outcome: hb.outcome, generation: hb.generation });

    // Let the heartbeat lapse, then standby may take over.
    await sleep(args.leaseMs + 300);
    const taken = await standby.acquireOwnership();
    log('standby takeover after lapse', {
      outcome: taken.outcome,
      generation: taken.generation,
    });

    const ok =
      refused.outcome === 'rejected' &&
      hb.outcome === 'renewed' &&
      hb.generation === own1.generation &&
      taken.outcome === 'takeover' &&
      taken.generation === own1.generation + 1;
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
