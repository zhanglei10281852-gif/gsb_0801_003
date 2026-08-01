/**
 * Deterministic tests for emergency cancellation and superseding safety
 * commands (round 3). Covers the pure transitions and the service-level lineage,
 * with a manual clock so timings are exact. The safety invariant under test:
 * an already device-confirmed action can never be turned into a "successful
 * cancel", and a superseded/cancelled command is fenced against late messages.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as sm from '../src/domain/stateMachine';
import { Command } from '../src/domain/types';
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

function makeService(clock = new ManualClock()) {
  const repo = new InMemoryRepository();
  const svc = new DispatchService(repo, clock, new SeqIds(), {
    leaseDurationMs: 1000,
    maxAttempts: 5,
    ownershipTtlMs: 1000,
  });
  return { repo, svc, clock };
}

function pendingCmd(): Command {
  return sm.create({
    id: 'cmd_1',
    idempotencyKey: 'k',
    payload: { deviceId: 'd', kind: 'calibrate', params: {} },
    now: 1000,
    maxAttempts: 3,
  }).command;
}

// --- pure cancel ---

test('cancel on PENDING moves to CANCELLED (terminal)', () => {
  const t = sm.cancel(pendingCmd(), { now: 2000, reason: 'estop' });
  assert.equal(t.command.status, 'CANCELLED');
  assert.equal(t.changed, true);
  assert.equal(t.events[0]?.type, 'CANCELLED');
  assert.equal(t.command.terminalReason, 'cancelled:estop');
});

test('cancel on LEASED drops the lease so a late confirm is ignored', () => {
  const leased = sm.lease(pendingCmd(), {
    leaseholder: 'gw1',
    leaseId: 'L1',
    now: 1000,
    leaseDurationMs: 1000,
  }).command;
  const t = sm.cancel(leased, { now: 1500 });
  assert.equal(t.command.status, 'CANCELLED');
  assert.equal(t.command.leaseId, null);
  // A late confirm against the old lease is ignored (already terminal).
  const late = sm.confirm(t.command, { leaseId: 'L1', outcome: 'success', now: 1600 });
  assert.equal(late.changed, false);
  assert.equal(late.outcome.kind, 'confirm_ignored');
});

test('cancel of an already-SUCCEEDED action is REJECTED (no fake cancel)', () => {
  const leased = sm.lease(pendingCmd(), {
    leaseholder: 'gw1',
    leaseId: 'L1',
    now: 1000,
    leaseDurationMs: 1000,
  }).command;
  const done = sm.confirm(leased, { leaseId: 'L1', outcome: 'success', now: 1200 }).command;
  const t = sm.cancel(done, { now: 1300 });
  assert.equal(t.changed, false);
  assert.equal(t.command.status, 'SUCCEEDED');
  if (t.outcome.kind === 'cancel_rejected') {
    assert.equal(t.outcome.reason, 'already_succeeded');
  } else {
    assert.fail('expected cancel_rejected');
  }
  assert.equal(t.events[0]?.type, 'CANCEL_REJECTED');
});

// --- pure supersede ---

test('supersede on active command marks it SUPERSEDED', () => {
  const t = sm.supersede(pendingCmd(), { now: 2000, bySupersessorId: 'cmd_2' });
  assert.equal(t.command.status, 'SUPERSEDED');
  assert.equal(t.events[0]?.type, 'SUPERSEDED');
  assert.equal(t.events[0]?.detail.bySupersessorId, 'cmd_2');
});

test('supersede of an already-terminal command is NOTED, not rewritten', () => {
  const leased = sm.lease(pendingCmd(), {
    leaseholder: 'gw1',
    leaseId: 'L1',
    now: 1000,
    leaseDurationMs: 1000,
  }).command;
  const done = sm.confirm(leased, { leaseId: 'L1', outcome: 'success', now: 1200 }).command;
  const t = sm.supersede(done, { now: 1300, bySupersessorId: 'cmd_2' });
  assert.equal(t.changed, false);
  assert.equal(t.command.status, 'SUCCEEDED'); // not rewritten to SUPERSEDED
  assert.equal(t.outcome.kind, 'supersede_noted');
  assert.equal(t.events[0]?.type, 'SUPERSEDE_NOTED');
});

// --- service-level cancel + lineage ---

test('service cancel then a late confirm cannot revive the command', () => {
  const { svc } = makeService();
  const sub = svc.submit({ idempotencyKey: 'k', deviceId: 'd', kind: 'calibrate' });
  const lease = svc.leaseNext('gw1')!;
  const c = svc.cancel(sub.command.id, { reason: 'estop' });
  assert.equal(c.cancelled, true);
  assert.equal(svc.getById(sub.command.id)!.status, 'CANCELLED');
  const late = svc.confirm(lease.command.id, lease.leaseId, 'success');
  assert.equal(late.accepted, false);
  assert.equal(svc.getById(sub.command.id)!.status, 'CANCELLED');
});

test('service cancel of a succeeded command is refused', () => {
  const { svc } = makeService();
  const sub = svc.submit({ idempotencyKey: 'k', deviceId: 'd', kind: 'calibrate' });
  const lease = svc.leaseNext('gw1')!;
  svc.confirm(lease.command.id, lease.leaseId, 'success');
  const c = svc.cancel(sub.command.id);
  assert.equal(c.cancelled, false);
  assert.equal(c.reason, 'already_succeeded');
  assert.equal(svc.getById(sub.command.id)!.status, 'SUCCEEDED');
});

test('supersede via submit terminates the old command atomically and links lineage', () => {
  const { svc } = makeService();
  const oldSub = svc.submit({ idempotencyKey: 'old', deviceId: 'd', kind: 'switch_process' });
  const oldId = oldSub.command.id;
  const newSub = svc.submit({
    idempotencyKey: 'new',
    deviceId: 'd',
    kind: 'switch_process',
    supersedesId: oldId,
    supersedeReason: 'safety',
  });
  const newId = newSub.command.id;

  assert.equal(svc.getById(oldId)!.status, 'SUPERSEDED');
  assert.equal(svc.getById(newId)!.supersedesId, oldId);

  // Lineage from either end returns the same ordered chain [old, new].
  const linFromNew = svc.lineage(newId)!;
  const linFromOld = svc.lineage(oldId)!;
  assert.deepEqual(linFromNew.chain.map((c) => c.id), [oldId, newId]);
  assert.deepEqual(linFromOld.chain.map((c) => c.id), [oldId, newId]);

  // The merged causal chain contains the SUPERSEDED transition and both submits.
  const types = linFromNew.events.map((e) => e.type);
  assert.ok(types.includes('SUBMITTED'));
  assert.ok(types.includes('SUPERSEDED'));
});

test('superseding an already-succeeded command does not rewrite it; both kept in lineage', () => {
  const { svc } = makeService();
  const oldSub = svc.submit({ idempotencyKey: 'old', deviceId: 'd', kind: 'calibrate' });
  const lease = svc.leaseNext('gw1')!;
  svc.confirm(lease.command.id, lease.leaseId, 'success');
  assert.equal(svc.getById(oldSub.command.id)!.status, 'SUCCEEDED');

  const newSub = svc.submit({
    idempotencyKey: 'new',
    deviceId: 'd',
    kind: 'calibrate',
    supersedesId: oldSub.command.id,
  });
  // Old stays SUCCEEDED (never rewritten), a SUPERSEDE_NOTED event is recorded.
  assert.equal(svc.getById(oldSub.command.id)!.status, 'SUCCEEDED');
  const oldEvents = svc.history(oldSub.command.id).map((e) => e.type);
  assert.ok(oldEvents.includes('SUPERSEDE_NOTED'));
  // Lineage still links both.
  const lin = svc.lineage(newSub.command.id)!;
  assert.deepEqual(lin.chain.map((c) => c.id), [oldSub.command.id, newSub.command.id]);
});

test('a cancelled command cannot be leased again', () => {
  const { svc } = makeService();
  const sub = svc.submit({ idempotencyKey: 'k', deviceId: 'd', kind: 'calibrate' });
  svc.cancel(sub.command.id);
  const lease = svc.leaseNext('gw1');
  assert.equal(lease, null); // nothing leasable; command is terminal
});

test('lineage across three generations of replacement is fully ordered', () => {
  const { svc } = makeService();
  const a = svc.submit({ idempotencyKey: 'a', deviceId: 'd', kind: 'calibrate' });
  const b = svc.submit({ idempotencyKey: 'b', deviceId: 'd', kind: 'calibrate', supersedesId: a.command.id });
  const c = svc.submit({ idempotencyKey: 'c', deviceId: 'd', kind: 'calibrate', supersedesId: b.command.id });
  const lin = svc.lineage(b.command.id)!; // query the middle; expect full chain
  assert.deepEqual(lin.chain.map((x) => x.id), [a.command.id, b.command.id, c.command.id]);
  assert.equal(svc.getById(a.command.id)!.status, 'SUPERSEDED');
  assert.equal(svc.getById(b.command.id)!.status, 'SUPERSEDED');
  assert.equal(svc.getById(c.command.id)!.status, 'PENDING');
});
