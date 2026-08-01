import { ApiClient } from './api-client.js';
import { randomUUID } from 'node:crypto';
import {
  GatewaySimulator,
  immediateAckDevice,
  immediateNackDevice,
  type DeviceBehavior,
} from './gateway.js';

function uniqueKey(prefix: string) {
  return `${prefix}-${Date.now()}-${randomUUID()}`;
}

export interface ScenarioResult {
  name: string;
  passed: boolean;
  details: string[];
  data?: Record<string, unknown>;
}

function ok(name: string, details: string[], data?: Record<string, unknown>): ScenarioResult {
  return { name, passed: true, details, data };
}
function fail(name: string, details: string[], data?: Record<string, unknown>): ScenarioResult {
  return { name, passed: false, details, data };
}

export async function scenarioHappyPath(client: ApiClient): Promise<ScenarioResult> {
  const key = uniqueKey('happy');
  const submit = await client.submit({
    idempotencyKey: key,
    deviceId: 'device-1',
    payload: { type: 'CALIBRATE', params: { target: 42 } },
  });
  const cmdId = (submit.body as { commandId: string }).commandId;
  const gw = new GatewaySimulator('gw-happy', client, immediateAckDevice);
  const leased = await gw.pollOnce(undefined, cmdId);
  if (!leased || leased.commandId !== cmdId) return fail('happy', ['claim did not return command']);
  const ack = await gw.ack(leased);
  if (ack.status !== 200) return fail('happy', ['ack not accepted', String(ack.status)]);
  const final = await client.getCommand(cmdId, true);
  const body = final.body as { state: string; events: unknown[]; attempt: number };
  if (body.state !== 'DELIVERED') return fail('happy', ['state not delivered', body.state]);
  return ok('happy', ['submitted, claimed with attempt=1, acked, delivered'], {
    commandId: cmdId,
    events: body.events.length,
    attempt: body.attempt,
  });
}

export async function scenarioDuplicateSubmit(client: ApiClient): Promise<ScenarioResult> {
  const key = uniqueKey('dup');
  const first = await client.submit({
    idempotencyKey: key,
    deviceId: 'device-1',
    payload: { type: 'RESET' },
  });
  const second = await client.submit({
    idempotencyKey: key,
    deviceId: 'device-1',
    payload: { type: 'RESET' },
  });
  const b1 = first.body as { commandId: string; duplicate: boolean };
  const b2 = second.body as { commandId: string; duplicate: boolean };
  if (b1.commandId !== b2.commandId || !b2.duplicate) {
    return fail('duplicate-submit', ['same idempotency key did not map to stable command id'], {
      firstStatus: first.status,
      secondStatus: second.status,
      first: first.body,
      second: second.body,
      key,
    });
  }
  return ok('duplicate-submit', ['same command id returned on duplicate submission'], {
    commandId: b1.commandId,
  });
}

export async function scenarioLeaseExpiryAndRetry(client: ApiClient): Promise<ScenarioResult> {
  const key = uniqueKey('lease');
  const submit = await client.submit({
    idempotencyKey: key,
    deviceId: 'device-1',
    payload: { type: 'SWITCH_RECIPE', params: { recipe: 'A' } },
  });
  const cmdId = (submit.body as { commandId: string }).commandId;
  const gw1 = new GatewaySimulator('gw-lost', client, immediateAckDevice);
  const leased1 = await gw1.pollOnce(300, cmdId);
  if (!leased1) return fail('lease-retry', ['first claim failed']);
  await new Promise((r) => setTimeout(r, 400));
  const sweep = await client.adminSweep();
  const gw2 = new GatewaySimulator('gw-recovery', client, immediateAckDevice);
  const leased2 = await gw2.pollOnce(5000, cmdId);
  if (!leased2 || leased2.commandId !== cmdId) {
    return fail('lease-retry', ['reclaim after expiry failed']);
  }
  if (leased2.attempt !== 2) {
    return fail('lease-retry', [`expected attempt 2, got ${leased2.attempt}`]);
  }
  const staleAck = await GatewaySimulator.duplicateLeaseAck(
    client,
    gw1.gatewayId,
    cmdId,
    leased1.leaseId
  );
  if (staleAck.status === 200) {
    return fail('lease-retry', ['stale lease ack should have been rejected']);
  }
  const ack = await gw2.ack(leased2);
  const final = await client.getCommand(cmdId, true);
  const body = final.body as { state: string; events: { type: string }[] };
  const types = body.events.map((e) => e.type);
  const hasExpired = types.includes('LeaseExpired');
  if (ack.status !== 200 || body.state !== 'DELIVERED' || !hasExpired) {
    return fail('lease-retry', ['retry state or event chain incorrect'], { types, state: body.state });
  }
  return ok('lease-retry', ['lease expired, same command retried with attempt=2, stale ack rejected'], {
    sweep: sweep.body,
    events: types,
  });
}

export async function scenarioDuplicateAck(client: ApiClient): Promise<ScenarioResult> {
  const key = uniqueKey('dupack');
  const submit = await client.submit({
    idempotencyKey: key,
    deviceId: 'device-2',
    payload: { type: 'CALIBRATE' },
  });
  const cmdId = (submit.body as { commandId: string }).commandId;
  const gw = new GatewaySimulator('gw-dupack', client, immediateAckDevice);
  const leased = await gw.pollOnce(undefined, cmdId);
  if (!leased) return fail('duplicate-ack', ['claim failed']);
  const first = await gw.ack(leased);
  const second = await gw.ack(leased);
  if (first.status !== 200 || second.status !== 409) {
    return fail('duplicate-ack', ['second ack must be rejected'], {
      first: first.status,
      second: second.status,
    });
  }
  const final = await client.getCommand(cmdId);
  if ((final.body as { state: string }).state !== 'DELIVERED') {
    return fail('duplicate-ack', ['state not delivered']);
  }
  return ok('duplicate-ack', ['first ack delivered, duplicate ack rejected']);
}

export async function scenarioOutOfOrderAndWrongLease(client: ApiClient): Promise<ScenarioResult> {
  const key = uniqueKey('ooo');
  await client.submit({
    idempotencyKey: key,
    deviceId: 'device-3',
    payload: { type: 'RESET' },
  });
  const gw = new GatewaySimulator('gw-ooo', client, immediateAckDevice);
  const cmdId = ((await client.getByIdempotencyKey(key)).body as { commandId: string }).commandId;
  const leased = await gw.pollOnce(1000, cmdId);
  if (!leased) return fail('wrong-lease', ['claim failed']);
  const fakeLease = GatewaySimulator.newLeaseId();
  const wrong = await client.ack(gw.gatewayId, leased.commandId, fakeLease, {
    success: true,
    ackCode: 'SPURIOUS',
  });
  if (wrong.status !== 409) return fail('wrong-lease', ['wrong lease should be rejected']);
  const renewWrong = await client.renew(gw.gatewayId, leased.commandId, fakeLease);
  if (renewWrong.status !== 409) return fail('wrong-lease', ['renew wrong lease should be rejected']);
  const good = await gw.ack(leased);
  if (good.status !== 200) return fail('wrong-lease', ['valid ack failed']);
  return ok('wrong-lease', ['wrong/out-of-order lease context rejected; valid lease accepted']);
}

export async function scenarioNackFailure(client: ApiClient): Promise<ScenarioResult> {
  const key = uniqueKey('nack');
  const submit = await client.submit({
    idempotencyKey: key,
    deviceId: 'device-4',
    payload: { type: 'CALIBRATE' },
  });
  const cmdId = (submit.body as { commandId: string }).commandId;
  const gw = new GatewaySimulator('gw-nack', client, immediateNackDevice);
  const leased = await gw.pollOnce(undefined, cmdId);
  if (!leased) return fail('nack', ['claim failed']);
  const ack = await gw.ack(leased);
  if (ack.status !== 200) return fail('nack', ['nack not recorded']);
  const final = await client.getCommand(cmdId, true);
  const body = final.body as { state: string; events: { type: string }[] };
  if (body.state !== 'FAILED' || !body.events.some((e) => e.type === 'DeviceNackRecorded')) {
    return fail('nack', ['nack did not produce terminal FAILED state']);
  }
  return ok('nack', ['negative device ack recorded as terminal FAILED']);
}

export const builtInScenarios: Record<string, (client: ApiClient) => Promise<ScenarioResult>> = {
  happy: scenarioHappyPath,
  'duplicate-submit': scenarioDuplicateSubmit,
  'lease-retry': scenarioLeaseExpiryAndRetry,
  'duplicate-ack': scenarioDuplicateAck,
  'wrong-lease': scenarioOutOfOrderAndWrongLease,
  nack: scenarioNackFailure,
};

export function customDevice(mode: 'ack' | 'nack' | 'slow'): DeviceBehavior {
  if (mode === 'nack') return immediateNackDevice;
  if (mode === 'slow') {
    return {
      async process() {
        await new Promise((r) => setTimeout(r, 50));
        return { success: true, ackCode: 'EXECUTED_SLOW' };
      },
    };
  }
  return immediateAckDevice;
}
