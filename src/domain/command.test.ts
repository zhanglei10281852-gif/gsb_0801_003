import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  createEventFactory,
  decideAck,
  decideClaim,
  decideExpire,
  decideRenew,
  decideSubmit,
  replay,
} from './command.js';
import { InMemoryEventStore } from '../adapters/memory-store.js';
import { CommandService } from '../application/command-service.js';

const factory = createEventFactory();
const baseSubmit = () =>
  decideSubmit(
    undefined,
    {
      commandId: 'cmd-1',
      idempotencyKey: 'idem-1',
      deviceId: 'dev-1',
      payload: { type: 'CALIBRATE' },
      submittedAt: 1000,
    },
    factory
  );

test('submit creates PENDING with stable command id and causation', () => {
  const decision = baseSubmit();
  assert.equal(decision.accepted, true);
  assert.equal(decision.events.length, 1);
  const state = replay(decision.events)!;
  assert.equal(state.state, 'PENDING');
  assert.equal(state.commandId, 'cmd-1');
  assert.equal(state.attempt, 0);
  assert.equal(decision.events[0].causationId, 'idem-1');
});

test('duplicate idempotency key is idempotent, conflicting key rejected', () => {
  const first = baseSubmit();
  const state = replay(first.events)!;
  const duplicate = decideSubmit(
    state,
    {
      commandId: 'cmd-new',
      idempotencyKey: 'idem-1',
      deviceId: 'dev-1',
      payload: { type: 'CALIBRATE' },
      submittedAt: 1001,
    },
    factory
  );
  assert.equal(duplicate.accepted, true);
  assert.equal(duplicate.events.length, 0);
  assert.equal(duplicate.result?.duplicate, true);

  const conflict = decideSubmit(
    state,
    {
      commandId: 'cmd-2',
      idempotencyKey: 'different',
      deviceId: 'dev-1',
      payload: { type: 'CALIBRATE' },
      submittedAt: 1002,
    },
    factory
  );
  assert.equal(conflict.accepted, false);
  assert.equal(conflict.reason, 'IDEMPOTENCY_KEY_CONFLICT');
});

test('claim creates attempt 1 with lease; active lease blocks second claim', () => {
  const stream = baseSubmit().events;
  const state = replay(stream)!;
  const claim = decideClaim(
    state,
    { gatewayId: 'gw-1', leaseId: 'lease-1', leaseDurationMs: 100, claimedAt: 1100, maxAttempts: 3 },
    factory
  );
  assert.equal(claim.accepted, true);
  const claimed = replay([...stream, ...claim.events])!;
  assert.equal(claimed.state, 'CLAIMED');
  assert.equal(claimed.attempt, 1);
  assert.equal(claimed.leaseId, 'lease-1');

  const blocked = decideClaim(
    claimed,
    { gatewayId: 'gw-2', leaseId: 'lease-2', leaseDurationMs: 100, claimedAt: 1150, maxAttempts: 3 },
    factory
  );
  assert.equal(blocked.accepted, false);
  assert.equal(blocked.reason, 'LEASE_ACTIVE');
});

test('lease expiry allows same stable command to be retried with new attempt and new lease', () => {
  const stream = baseSubmit().events;
  const first = decideClaim(
    replay(stream)!,
    { gatewayId: 'gw-1', leaseId: 'lease-1', leaseDurationMs: 100, claimedAt: 1100, maxAttempts: 3 },
    factory
  );
  const afterFirst = [...stream, ...first.events];
  const second = decideClaim(
    replay(afterFirst)!,
    { gatewayId: 'gw-2', leaseId: 'lease-2', leaseDurationMs: 100, claimedAt: 1300, maxAttempts: 3 },
    factory
  );
  assert.equal(second.accepted, true);
  assert.equal(second.events[0].type, 'LeaseExpired');
  assert.equal(second.events[1].type, 'CommandClaimed');
  const afterSecond = replay([...afterFirst, ...second.events])!;
  assert.equal(afterSecond.state, 'CLAIMED');
  assert.equal(afterSecond.attempt, 2);
  assert.equal(afterSecond.commandId, 'cmd-1');
  assert.equal(afterSecond.leaseId, 'lease-2');
});

test('renew requires active matching lease', () => {
  const stream = baseSubmit().events;
  const claim = decideClaim(
    replay(stream)!,
    { gatewayId: 'gw-1', leaseId: 'lease-1', leaseDurationMs: 100, claimedAt: 1100, maxAttempts: 3 },
    factory
  );
  const state = replay([...stream, ...claim.events])!;
  const badGateway = decideRenew(
    state,
    { gatewayId: 'gw-x', leaseId: 'lease-1', leaseDurationMs: 100, renewedAt: 1150 },
    factory
  );
  assert.equal(badGateway.accepted, false);
  assert.equal(badGateway.reason, 'LEASE_MISMATCH');
  const expired = decideRenew(
    state,
    { gatewayId: 'gw-1', leaseId: 'lease-1', leaseDurationMs: 100, renewedAt: 1300 },
    factory
  );
  assert.equal(expired.reason, 'LEASE_EXPIRED');
  const ok = decideRenew(
    state,
    { gatewayId: 'gw-1', leaseId: 'lease-1', leaseDurationMs: 100, renewedAt: 1150 },
    factory
  );
  assert.equal(ok.accepted, true);
});

test('ack is terminal, duplicate/wrong ack rejected, and stale lease cannot revive', () => {
  const stream = baseSubmit().events;
  const claim = decideClaim(
    replay(stream)!,
    { gatewayId: 'gw-1', leaseId: 'lease-1', leaseDurationMs: 100, claimedAt: 1100, maxAttempts: 3 },
    factory
  );
  const state = replay([...stream, ...claim.events])!;
  const stale = decideAck(
    state,
    {
      gatewayId: 'gw-1',
      leaseId: 'old-lease',
      ackCode: 'LATE',
      success: true,
      receivedAt: 1180,
    },
    factory
  );
  assert.equal(stale.accepted, false);
  assert.equal(stale.reason, 'LEASE_MISMATCH');

  const ack = decideAck(
    state,
    {
      gatewayId: 'gw-1',
      leaseId: 'lease-1',
      ackCode: 'EXECUTED',
      success: true,
      receivedAt: 1180,
    },
    factory
  );
  assert.equal(ack.accepted, true);
  const delivered = replay([...stream, ...claim.events, ...ack.events])!;
  assert.equal(delivered.state, 'DELIVERED');

  const duplicate = decideAck(
    delivered,
    {
      gatewayId: 'gw-1',
      leaseId: 'lease-1',
      ackCode: 'DUP',
      success: true,
      receivedAt: 1190,
    },
    factory
  );
  assert.equal(duplicate.accepted, false);
  assert.match(duplicate.reason ?? '', /TERMINAL/);

  const reclaim = decideClaim(
    delivered,
    { gatewayId: 'gw-2', leaseId: 'lease-2', leaseDurationMs: 100, claimedAt: 1500, maxAttempts: 3 },
    factory
  );
  assert.equal(reclaim.accepted, false);
});

test('expire produces LeaseExpired then times out after max attempts', () => {
  const stream = baseSubmit().events;
  const claim1 = decideClaim(
    replay(stream)!,
    { gatewayId: 'gw-1', leaseId: 'lease-1', leaseDurationMs: 100, claimedAt: 1100, maxAttempts: 2 },
    factory
  );
  const afterClaim1 = [...stream, ...claim1.events];
  const expire1 = decideExpire(replay(afterClaim1)!, { now: 1300, maxAttempts: 2 }, factory);
  assert.equal(expire1.accepted, true);
  const afterExpire1 = [...afterClaim1, ...expire1.events];
  assert.equal(replay(afterExpire1)!.state, 'PENDING');

  const claim2 = decideClaim(
    replay(afterExpire1)!,
    { gatewayId: 'gw-2', leaseId: 'lease-2', leaseDurationMs: 100, claimedAt: 1350, maxAttempts: 2 },
    factory
  );
  const afterClaim2 = [...afterExpire1, ...claim2.events];
  const expire2 = decideExpire(replay(afterClaim2)!, { now: 1600, maxAttempts: 2 }, factory);
  assert.equal(expire2.events.at(-1)?.type, 'CommandTimedOut');
  assert.equal(replay([...afterClaim2, ...expire2.events])!.state, 'TIMED_OUT');
});

test('service replay from event store survives a post-commit response loss', () => {
  const store = new InMemoryEventStore();
  const svc1 = new CommandService(
    store,
    { leaseDurationMs: 100, maxAttempts: 2, claimBatchSize: 5 },
    factory
  );
  const submitted = svc1.submit({
    idempotencyKey: 'idem-crash',
    deviceId: 'dev',
    payload: { type: 'RESET' },
    submittedAt: 1000,
  });
  assert.equal(submitted.duplicate, false);
  const claimed = svc1.claimOne({ gatewayId: 'gw', now: 1100 })!;
  svc1.ack({
    commandId: claimed.command.commandId,
    gatewayId: 'gw',
    leaseId: claimed.command.leaseId!,
    ackCode: 'EXECUTED',
    success: true,
    receivedAt: 1150,
  });

  const svc2 = new CommandService(
    store,
    { leaseDurationMs: 100, maxAttempts: 2, claimBatchSize: 5 },
    factory
  );
  const duplicate = svc2.submit({
    idempotencyKey: 'idem-crash',
    deviceId: 'dev',
    payload: { type: 'RESET' },
    submittedAt: 2000,
  });
  assert.equal(duplicate.duplicate, true);
  assert.equal(duplicate.commandId, claimed.command.commandId);
  assert.equal(duplicate.state, 'DELIVERED');
  assert.equal(svc2.claimOne({ gatewayId: 'gw2', now: 2100 }), undefined);
});

test('event apply is deterministic and append enforces optimistic version', () => {
  const store = new InMemoryEventStore();
  const decision = baseSubmit();
  store.append('cmd-1', 0, decision.events, replay(decision.events));
  assert.throws(() => store.append('cmd-1', 0, decision.events, replay(decision.events)));
  const loaded = store.getSnapshot('cmd-1')!;
  assert.deepEqual(loaded, replay(decision.events));
});
