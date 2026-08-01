/**
 * Deterministic service-level tests. Uses the in-memory repository, a manual
 * clock and a counter-based id generator so crash/timeout interleavings are
 * exact and repeatable. No HTTP, no SQLite, no real time.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DispatchService } from '../src/app/dispatchService';
import { InMemoryRepository } from '../src/adapters/inMemoryRepository';
import { Clock, IdGenerator } from '../src/ports';

class ManualClock implements Clock {
  constructor(public t = 1000) {}
  now(): number {
    return this.t;
  }
  advance(ms: number): void {
    this.t += ms;
  }
}

class SeqIds implements IdGenerator {
  private c = 0;
  private l = 0;
  newCommandId(): string {
    return `cmd_${++this.c}`;
  }
  newLeaseId(): string {
    return `lease_${++this.l}`;
  }
}

function makeService(clock = new ManualClock(), maxAttempts = 3) {
  const repo = new InMemoryRepository();
  const svc = new DispatchService(repo, clock, new SeqIds(), {
    leaseDurationMs: 1000,
    maxAttempts,
  });
  return { repo, svc, clock };
}

test('submit is idempotent by business key', () => {
  const { svc } = makeService();
  const a = svc.submit({ idempotencyKey: 'k1', deviceId: 'd', kind: 'calibrate' });
  const b = svc.submit({ idempotencyKey: 'k1', deviceId: 'd', kind: 'calibrate' });
  assert.equal(a.deduped, false);
  assert.equal(b.deduped, true);
  assert.equal(a.command.id, b.command.id);
  // exactly one command, and a dedup event exists in history
  const hist = svc.history(a.command.id);
  assert.ok(hist.some((e) => e.type === 'SUBMITTED'));
  assert.ok(hist.some((e) => e.type === 'SUBMIT_DEDUPED'));
});

test('lease -> confirm success reaches SUCCEEDED with one execution identity', () => {
  const { svc } = makeService();
  const sub = svc.submit({ idempotencyKey: 'k', deviceId: 'd', kind: 'calibrate' });
  const lease = svc.leaseNext('gw1');
  assert.ok(lease);
  assert.equal(lease!.executionId, sub.command.id);
  const conf = svc.confirm(lease!.command.id, lease!.leaseId, 'success', 'rcpt1');
  assert.equal(conf.accepted, true);
  assert.equal(svc.getById(sub.command.id)!.status, 'SUCCEEDED');
});

test('crash-after-persist replay: re-submit + re-lease keeps one executionId', () => {
  // Model: upstream submitted, server crashed before responding, upstream
  // retries with the same key. Then a gateway leased, crashed before confirming
  // (lease expires), and a second gateway re-leases.
  const clock = new ManualClock();
  const { svc } = makeService(clock);
  const first = svc.submit({ idempotencyKey: 'k', deviceId: 'd', kind: 'calibrate' });
  const retry = svc.submit({ idempotencyKey: 'k', deviceId: 'd', kind: 'calibrate' });
  assert.equal(first.command.id, retry.command.id); // no second device action

  const lease1 = svc.leaseNext('gw1');
  assert.ok(lease1);
  const exec1 = lease1!.executionId;

  // gw1 vanishes; lease expires; sweeper requeues.
  clock.advance(1500);
  const swept = svc.sweepExpired();
  assert.equal(swept.requeued, 1);

  const lease2 = svc.leaseNext('gw2');
  assert.ok(lease2);
  assert.equal(lease2!.executionId, exec1); // SAME stable identity across re-lease
  assert.notEqual(lease2!.leaseId, lease1!.leaseId); // fresh fencing token

  const conf = svc.confirm(lease2!.command.id, lease2!.leaseId, 'success');
  assert.equal(conf.accepted, true);
  assert.equal(svc.getById(first.command.id)!.status, 'SUCCEEDED');
});

test('late confirm from an expired lease cannot revive a requeued command', () => {
  const clock = new ManualClock();
  const { svc } = makeService(clock);
  const sub = svc.submit({ idempotencyKey: 'k', deviceId: 'd', kind: 'calibrate' });
  const lease1 = svc.leaseNext('gw1')!;
  clock.advance(1500); // lease expires
  svc.sweepExpired(); // -> PENDING again

  // Old holder's late confirmation must be ignored. After the sweep the command
  // is back to PENDING, so the domain reports "no_active_lease"; the essential
  // guarantee is that it is NOT accepted and the command is not revived.
  const late = svc.confirm(lease1.command.id, lease1.leaseId, 'success');
  assert.equal(late.accepted, false);
  assert.equal(late.reason, 'no_active_lease');
  assert.notEqual(svc.getById(sub.command.id)!.status, 'SUCCEEDED');

  // A fresh lease can still complete it.
  const lease2 = svc.leaseNext('gw2')!;
  const ok = svc.confirm(lease2.command.id, lease2.leaseId, 'success');
  assert.equal(ok.accepted, true);
});

test('confirmation wins the race against an about-to-expire lease reaper', () => {
  // At exactly the lease boundary, a confirmation delivered before expiry
  // sweeps must still succeed; a sweep after the confirmation is a no-op.
  const clock = new ManualClock();
  const { svc } = makeService(clock);
  const sub = svc.submit({ idempotencyKey: 'k', deviceId: 'd', kind: 'calibrate' });
  const lease = svc.leaseNext('gw1')!;
  clock.advance(999); // still within the 1000ms lease
  const ok = svc.confirm(lease.command.id, lease.leaseId, 'success');
  assert.equal(ok.accepted, true);
  clock.advance(100); // now past original expiry
  const swept = svc.sweepExpired();
  assert.equal(swept.requeued, 0); // terminal command is untouched
  assert.equal(svc.getById(sub.command.id)!.status, 'SUCCEEDED');
});

test('retry budget exhaustion fails the command via sweep', () => {
  const clock = new ManualClock();
  const { svc } = makeService(clock, 2); // maxAttempts = 2
  const sub = svc.submit({ idempotencyKey: 'k', deviceId: 'd', kind: 'calibrate' });
  // attempt 1
  svc.leaseNext('gw1');
  clock.advance(1500);
  svc.sweepExpired();
  // attempt 2 (last)
  svc.leaseNext('gw1');
  clock.advance(1500);
  const swept = svc.sweepExpired();
  assert.equal(swept.failed, 1);
  const cmd = svc.getById(sub.command.id)!;
  assert.equal(cmd.status, 'FAILED');
  assert.equal(cmd.terminalReason, 'retries_exhausted');
});

test('two gateways cannot both lease the same command', () => {
  const { svc } = makeService();
  svc.submit({ idempotencyKey: 'k', deviceId: 'd', kind: 'calibrate' });
  const a = svc.leaseNext('gw1');
  const b = svc.leaseNext('gw2');
  assert.ok(a);
  assert.equal(b, null); // only one PENDING command; second gets nothing
});

test('history is a causal chain, not just the final value', () => {
  const clock = new ManualClock();
  const { svc } = makeService(clock);
  const sub = svc.submit({ idempotencyKey: 'k', deviceId: 'd', kind: 'calibrate' });
  const l1 = svc.leaseNext('gw1')!;
  clock.advance(1500);
  svc.sweepExpired();
  const l2 = svc.leaseNext('gw2')!;
  svc.confirm(l2.command.id, l2.leaseId, 'success');
  const types = svc.history(sub.command.id).map((e) => e.type);
  // Expect the full story: submit, lease, expiry, requeue, re-lease, success.
  assert.deepEqual(types, [
    'SUBMITTED',
    'LEASED',
    'LEASE_EXPIRED',
    'REQUEUED',
    'LEASED',
    'CONFIRMED_SUCCESS',
  ]);
  // Epochs increase across re-lease.
  const leaseEvents = svc.history(sub.command.id).filter((e) => e.type === 'LEASED');
  assert.equal(leaseEvents[0]!.attemptEpoch, 1);
  assert.equal(leaseEvents[1]!.attemptEpoch, 2);
});
