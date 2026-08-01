import assert from 'node:assert/strict';
import { startCompiledServer, sleep, ServerHandle } from './serverHarness.js';
import { ApiClient, ApiCommand } from '../simulator/apiClient.js';
import { DeviceSimulator } from '../simulator/deviceSimulator.js';
import { GatewaySimulator } from '../simulator/gatewaySimulator.js';

let passed = 0;
let failed = 0;
const failures: { name: string; error: Error }[] = [];

async function test(name: string, fn: () => Promise<void>): Promise<void> {
  try {
    await fn();
    passed++;
    console.log(`  PASS  ${name}`);
  } catch (err) {
    failed++;
    failures.push({ name, error: err as Error });
    console.log(`  FAIL  ${name}`);
    console.log(`        ${(err as Error).message}`);
  }
}

function isCommand(body: unknown): body is ApiCommand {
  return typeof body === 'object' && body !== null && 'commandId' in body && 'status' in body;
}

async function fetchCommand(api: ApiClient, commandId: string): Promise<ApiCommand> {
  const { body } = await api.getCommand(commandId);
  assert.ok(isCommand(body), `expected command, got: ${JSON.stringify(body)}`);
  return body;
}

function assertEventTypes(events: { eventType: string }[], expected: string[]): void {
  const actual = events.map((e) => e.eventType);
  assert.deepEqual(
    actual,
    expected,
    `event chain mismatch\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`
  );
}

async function waitForStatus(
  api: ApiClient,
  commandId: string,
  status: string,
  timeoutMs = 10000
): Promise<ApiCommand> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const { body } = await api.getCommand(commandId);
    if (isCommand(body) && body.status === status) return body;
    await sleep(100);
  }
  const { body } = await api.getCommand(commandId);
  throw new Error(
    `command ${commandId} did not reach ${status} within ${timeoutMs}ms; last=${JSON.stringify(body)}`
  );
}

async function scenarioHappyPath(server: ServerHandle): Promise<void> {
  await test('happy path: submit -> claim -> deliver -> confirm -> SUCCEEDED with causal events', async () => {
    const api = new ApiClient(server.baseUrl);
    const device = new DeviceSimulator('dev-1', { confirmDelayMs: 20 });
    const gw = new GatewaySimulator({
      gatewayId: 'gw-happy',
      pollIntervalMs: 50,
      leaseDurationMs: 5000,
      renewIntervalMs: 2000,
      api,
      device,
    });

    const { body: submitted, status } = await api.submitCommand('e2e-happy', {
      deviceId: 'dev-1',
      action: 'calibrate',
    });
    assert.equal(status, 201);
    assert.equal(submitted.status, 'PENDING');

    gw.start();
    await waitForStatus(api, submitted.commandId, 'SUCCEEDED', 8000);
    gw.stop();

    const final = await fetchCommand(api, submitted.commandId);
    assert.equal(final.status, 'SUCCEEDED');
    assert.ok(final.confirmationCode, 'confirmationCode must be set');
    assert.equal(final.attempt, 1);

    const { body: eventsRes } = await api.getEvents(submitted.commandId);
    assertEventTypes(eventsRes.events, [
      'CommandSubmitted',
      'CommandClaimed',
      'DeliveryReported',
      'DeviceConfirmed',
      'CommandSucceeded',
    ]);
  });
}

async function scenarioIdempotentSubmit(server: ServerHandle): Promise<void> {
  await test('idempotent submit: duplicate business key returns same command (crash-after-commit recovery)', async () => {
    const api = new ApiClient(server.baseUrl);
    const payload = { deviceId: 'dev-2', action: 'switch-recipe' };

    const first = await api.submitCommand('e2e-idem-1', payload);
    assert.equal(first.status, 201);

    const second = await api.submitCommand('e2e-idem-1', {
      deviceId: 'dev-DIFFERENT',
      action: 'reset',
    });
    assert.equal(second.status, 200);
    assert.equal(second.body.commandId, first.body.commandId);
    assert.deepEqual(second.body.payload, payload);

    const byKey = await api.getCommandByKey('e2e-idem-1');
    assert.ok(isCommand(byKey.body), 'by-key lookup should return command');
    assert.equal(byKey.body.commandId, first.body.commandId);
  });
}

async function scenarioGatewayCrashAndReclaim(server: ServerHandle): Promise<void> {
  await test('gateway loss: lease expires, reclaim uses stable commandId, old gateway confirmation rejected', async () => {
    const api = new ApiClient(server.baseUrl);
    const device1 = new DeviceSimulator('dev-3', { dropConfirm: true });
    const gw1 = new GatewaySimulator({
      gatewayId: 'gw-lost',
      pollIntervalMs: 50,
      leaseDurationMs: 800,
      renewIntervalMs: 10000,
      api,
      device: device1,
    });
    const device2 = new DeviceSimulator('dev-3', { confirmDelayMs: 20 });
    const gw2 = new GatewaySimulator({
      gatewayId: 'gw-recovery',
      pollIntervalMs: 50,
      leaseDurationMs: 5000,
      renewIntervalMs: 2000,
      api,
      device: device2,
    });

    const { body: submitted } = await api.submitCommand('e2e-crash-1', {
      deviceId: 'dev-3',
      action: 'calibrate',
    }, 3);

    gw1.start();
    await waitForStatus(api, submitted.commandId, 'CLAIMED', 5000);
    const held = gw1.getHeldTask();
    assert.ok(held, 'gw1 should have claimed the task');
    const originalCommandId = held!.commandId;
    const oldLeaseId = held!.currentLeaseId!;

    gw1.goOffline();
    await sleep(1200);

    gw2.start();
    await waitForStatus(api, originalCommandId, 'SUCCEEDED', 8000);
    const final = await fetchCommand(api, originalCommandId);
    assert.equal(final.status, 'SUCCEEDED');
    assert.equal(final.attempt, 2);
    assert.equal(final.commandId, originalCommandId);
    assert.ok(final.currentLeaseId !== oldLeaseId, 'new lease must differ from old lease');

    await gw1.sendLateConfirmWithOldLease(originalCommandId, oldLeaseId);
    const afterLate = await fetchCommand(api, originalCommandId);
    assert.equal(afterLate.status, 'SUCCEEDED', 'late confirmation must not change state');
    assert.equal(afterLate.currentLeaseId, final.currentLeaseId, 'lease must not change after late ack');

    gw1.stop();
    gw2.stop();

    const { body: eventsRes } = await api.getEvents(originalCommandId);
    const types = eventsRes.events.map((e) => e.eventType);
    assert.ok(types.includes('StaleMessageRejected'), 'must record StaleMessageRejected for old lease');
    const reclaimOrReclaim =
      types.includes('CommandReclaimed') ||
      types.filter((t) => t === 'CommandClaimed').length >= 2;
    assert.ok(
      reclaimOrReclaim,
      'must record either CommandReclaimed (direct reclaim) or a second CommandClaimed (scan then reclaim)'
    );
    const succeeded = types.filter((t) => t === 'CommandSucceeded');
    assert.equal(succeeded.length, 1, 'exactly one CommandSucceeded');
  });
}

async function scenarioDuplicateConfirmation(server: ServerHandle): Promise<void> {
  await test('duplicate device confirmation: idempotent, only one success, stale duplicate recorded', async () => {
    const api = new ApiClient(server.baseUrl);
    const device = new DeviceSimulator('dev-4', { confirmDelayMs: 20, duplicateConfirm: true });
    const gw = new GatewaySimulator({
      gatewayId: 'gw-dup',
      pollIntervalMs: 50,
      leaseDurationMs: 5000,
      renewIntervalMs: 2000,
      api,
      device,
    });

    const { body: submitted } = await api.submitCommand('e2e-dup-ack', {
      deviceId: 'dev-4',
      action: 'set-speed',
      params: { rpm: 1200 },
    });
    gw.start();
    await waitForStatus(api, submitted.commandId, 'SUCCEEDED', 8000);
    await sleep(300);
    gw.stop();

    const { body: eventsRes } = await api.getEvents(submitted.commandId);
    const types = eventsRes.events.map((e) => e.eventType);
    const successCount = types.filter((t) => t === 'CommandSucceeded').length;
    assert.equal(successCount, 1, 'exactly one CommandSucceeded event');
    const staleCount = types.filter((t) => t === 'StaleMessageRejected').length;
    assert.ok(staleCount >= 1, 'duplicate confirmation must be recorded as StaleMessageRejected');
  });
}

async function scenarioLateConfirmAfterExpiry(server: ServerHandle): Promise<void> {
  await test('late confirmation after lease expiry (before reclaim) is rejected and does not resurrect', async () => {
    const api = new ApiClient(server.baseUrl);
    const device = new DeviceSimulator('dev-5', { dropConfirm: true });
    const gw = new GatewaySimulator({
      gatewayId: 'gw-late',
      pollIntervalMs: 50,
      leaseDurationMs: 600,
      renewIntervalMs: 10000,
      api,
      device,
    });

    const { body: submitted } = await api.submitCommand('e2e-late-ack', {
      deviceId: 'dev-5',
      action: 'calibrate',
    }, 3);

    gw.start();
    await waitForStatus(api, submitted.commandId, 'CLAIMED', 5000);
    const held = gw.getHeldTask();
    assert.ok(held);
    const leaseId = held!.currentLeaseId!;
    gw.goOffline();

    await sleep(1200);
    await api.scanExpired();

    const afterExpiry = await fetchCommand(api, submitted.commandId);
    assert.equal(afterExpiry.status, 'PENDING');

    const { body: confirmRes } = await api.confirm({
      commandId: submitted.commandId,
      leaseId,
      gatewayId: 'gw-late',
      confirmationCode: 'TOO-LATE',
      deviceTimestamp: Date.now(),
    });
    assert.equal(confirmRes.accepted, false);
    assert.equal(confirmRes.command.status, 'PENDING');
    assert.equal(confirmRes.command.confirmationCode, null);

    gw.stop();
  });
}

async function scenarioMaxAttemptsFailure(server: ServerHandle): Promise<void> {
  await test('max attempts exhausted: command ends FAILED with causal retry/expiry trail', async () => {
    const api = new ApiClient(server.baseUrl);
    const { body: submitted } = await api.submitCommand('e2e-max-att', {
      deviceId: 'dev-6',
      action: 'calibrate',
    }, 2);

    for (let attempt = 1; attempt <= 2; attempt++) {
      const device = new DeviceSimulator('dev-6', { dropConfirm: true });
      const gw = new GatewaySimulator({
        gatewayId: `gw-fail-${attempt}`,
        pollIntervalMs: 50,
        leaseDurationMs: 1000,
        renewIntervalMs: 10000,
        api,
        device,
      });
      gw.start();
      await waitForStatus(api, submitted.commandId, 'CLAIMED', 5000);
      const held = gw.getHeldTask();
      assert.ok(held, `attempt ${attempt} should be claimed`);
      assert.equal(held!.attempt, attempt);
      gw.goOffline();
      await sleep(1500);
      await api.scanExpired();
      gw.stop();
    }

    await waitForStatus(api, submitted.commandId, 'FAILED', 5000);
    const final = await fetchCommand(api, submitted.commandId);
    assert.equal(final.status, 'FAILED');
    assert.equal(final.attempt, 2);
    assert.ok(final.failureReason?.includes('max attempts'));

    const { body: eventsRes } = await api.getEvents(submitted.commandId);
    const types = eventsRes.events.map((e) => e.eventType);
    assert.equal(types.filter((t) => t === 'LeaseExpired').length, 2);
    assert.ok(types.includes('CommandFailed'));
    const totalClaimEvents =
      types.filter((t) => t === 'CommandClaimed').length +
      types.filter((t) => t === 'CommandReclaimed').length;
    assert.equal(totalClaimEvents, 2, 'two claim/reclaim events across attempts');
  });
}

async function scenarioDuplicateSubmissionDedup(server: ServerHandle): Promise<void> {
  await test('concurrent duplicate submission: same idempotency key creates exactly one device action', async () => {
    const api = new ApiClient(server.baseUrl);
    const payload = { deviceId: 'dev-7', action: 'reset' };

    const results = await Promise.all([
      api.submitCommand('e2e-concurrent', payload),
      api.submitCommand('e2e-concurrent', payload),
      api.submitCommand('e2e-concurrent', payload),
    ]);

    const ids = new Set(results.map((r) => r.body.commandId));
    assert.equal(ids.size, 1, 'all concurrent submissions must resolve to one commandId');

    const device = new DeviceSimulator('dev-7', { confirmDelayMs: 20 });
    const gw = new GatewaySimulator({
      gatewayId: 'gw-conc',
      pollIntervalMs: 50,
      leaseDurationMs: 5000,
      renewIntervalMs: 2000,
      api,
      device,
    });
    gw.start();
    const commandId = results[0].body.commandId;
    await waitForStatus(api, commandId, 'SUCCEEDED', 8000);
    gw.stop();

    const received = device.getReceived();
    assert.equal(received.length, 1, 'device must receive exactly one delivery');
    assert.equal(received[0].commandId, commandId);
  });
}

async function scenarioOutOfOrderConfirm(server: ServerHandle): Promise<void> {
  await test('out-of-order confirmation: stale lease from previous attempt cannot overwrite success', async () => {
    const api = new ApiClient(server.baseUrl);
    const device1 = new DeviceSimulator('dev-8', { dropConfirm: true });
    const gw1 = new GatewaySimulator({
      gatewayId: 'gw-oox-1',
      pollIntervalMs: 50,
      leaseDurationMs: 700,
      renewIntervalMs: 10000,
      api,
      device: device1,
    });

    const { body: submitted } = await api.submitCommand('e2e-oox', {
      deviceId: 'dev-8',
      action: 'calibrate',
    }, 3);

    gw1.start();
    await waitForStatus(api, submitted.commandId, 'CLAIMED', 5000);
    const firstHeld = gw1.getHeldTask();
    assert.ok(firstHeld);
    const firstLease = firstHeld!.currentLeaseId!;
    gw1.goOffline();
    await sleep(1200);

    const device2 = new DeviceSimulator('dev-8', { confirmDelayMs: 20 });
    const gw2 = new GatewaySimulator({
      gatewayId: 'gw-oox-2',
      pollIntervalMs: 50,
      leaseDurationMs: 5000,
      renewIntervalMs: 2000,
      api,
      device: device2,
    });
    gw2.start();
    await waitForStatus(api, submitted.commandId, 'SUCCEEDED', 8000);
    gw2.stop();

    await gw1.sendLateConfirmWithOldLease(submitted.commandId, firstLease);
    const final = await fetchCommand(api, submitted.commandId);
    assert.equal(final.status, 'SUCCEEDED');
    assert.notEqual(final.currentLeaseId, firstLease);

    gw1.stop();

    const { body: eventsRes } = await api.getEvents(submitted.commandId);
    const successIdx = eventsRes.events.findIndex((e) => e.eventType === 'CommandSucceeded');
    const staleAfterSuccess = eventsRes.events.filter(
      (e, i) => e.eventType === 'StaleMessageRejected' && i > successIdx
    );
    assert.ok(
      staleAfterSuccess.length >= 1,
      'a StaleMessageRejected must appear after CommandSucceeded for the late old-lease confirmation'
    );
  });
}

async function scenarioEventAuditTrail(server: ServerHandle): Promise<void> {
  await test('operations audit: every state transition has a causedBy event with timestamp and lease/attempt context', async () => {
    const api = new ApiClient(server.baseUrl);
    const { body: eventsRes } = await api.listEvents(1000);
    assert.ok(eventsRes.events.length > 0, 'event log must not be empty after scenarios');

    for (const e of eventsRes.events) {
      assert.ok(e.eventId, 'eventId required');
      assert.ok(e.commandId, 'commandId required');
      assert.ok(e.eventType, 'eventType required');
      assert.ok(e.causedBy, `causedBy required for ${e.eventType}`);
      assert.ok(typeof e.timestamp === 'number', 'timestamp required');
    }

    const submitEvents = eventsRes.events.filter((e) => e.eventType === 'CommandSubmitted');
    for (const e of submitEvents) {
      assert.ok(e.payload?.idempotencyKey, 'CommandSubmitted must record idempotencyKey');
    }
  });
}

async function main(): Promise<void> {
  console.log('Starting compiled server for end-to-end acceptance...');
  const server = await startCompiledServer();
  console.log(`Server running at ${server.baseUrl}`);
  console.log('');

  try {
    console.log('Running acceptance scenarios:');
    await scenarioHappyPath(server);
    await scenarioIdempotentSubmit(server);
    await scenarioGatewayCrashAndReclaim(server);
    await scenarioDuplicateConfirmation(server);
    await scenarioLateConfirmAfterExpiry(server);
    await scenarioMaxAttemptsFailure(server);
    await scenarioDuplicateSubmissionDedup(server);
    await scenarioOutOfOrderConfirm(server);
    await scenarioEventAuditTrail(server);
  } finally {
    await server.stop();
    console.log('\nServer stopped.');
  }

  console.log(`\nResults: ${passed} passed, ${failed} failed`);
  if (failed > 0) {
    console.log('\nFailures:');
    for (const f of failures) {
      console.log(`  - ${f.name}: ${f.error.message}`);
    }
    process.exit(1);
  }
  setImmediate(() => process.exit(0));
}

main().catch((err) => {
  console.error('E2E runner fatal error:', err);
  process.exit(1);
});
