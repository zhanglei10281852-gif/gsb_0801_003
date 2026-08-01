/**
 * Deterministic tests for line ownership generations (代际) and generation
 * fencing across redundant gateways. Pure ownership state machine + service-
 * level takeover/split-brain interleavings, all with a manual clock so timings
 * are exact and repeatable.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as own from '../src/domain/ownership';
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

function makeService(clock = new ManualClock(), overrides: Partial<{ leaseDurationMs: number; maxAttempts: number; ownershipTtlMs: number }> = {}) {
  const repo = new InMemoryRepository();
  const svc = new DispatchService(repo, clock, new SeqIds(), {
    leaseDurationMs: 1000,
    maxAttempts: 5,
    ownershipTtlMs: 1000,
    ...overrides,
  });
  return { repo, svc, clock };
}

// --- pure ownership state machine ---

test('acquire establishes generation 1', () => {
  const t = own.acquire({ lineId: 'L', claimant: 'gw1', now: 100, ownershipTtlMs: 500 });
  assert.equal(t.ownership.generation, 1);
  assert.equal(t.ownership.owner, 'gw1');
  assert.equal(t.outcome.kind, 'acquired');
  assert.equal(t.events[0]?.type, 'OWNERSHIP_ACQUIRED');
});

test('owner heartbeat renews without bumping generation', () => {
  const g1 = own.acquire({ lineId: 'L', claimant: 'gw1', now: 100, ownershipTtlMs: 500 }).ownership;
  const t = own.claim(g1, { lineId: 'L', claimant: 'gw1', now: 300, ownershipTtlMs: 500 });
  assert.equal(t.outcome.kind, 'renewed');
  assert.equal(t.ownership.generation, 1);
  assert.equal(t.ownership.expiresAt, 800);
});

test('a different gateway cannot take over a healthy owner', () => {
  const g1 = own.acquire({ lineId: 'L', claimant: 'gw1', now: 100, ownershipTtlMs: 500 }).ownership;
  const t = own.claim(g1, { lineId: 'L', claimant: 'gw2', now: 300, ownershipTtlMs: 500 });
  assert.equal(t.changed, false);
  assert.equal(t.outcome.kind, 'rejected');
  if (t.outcome.kind === 'rejected') assert.equal(t.outcome.reason, 'owner_alive');
  assert.equal(t.ownership.owner, 'gw1');
  assert.equal(t.events[0]?.type, 'OWNERSHIP_REJECTED');
});

test('takeover after expiry bumps generation monotonically', () => {
  const g1 = own.acquire({ lineId: 'L', claimant: 'gw1', now: 100, ownershipTtlMs: 500 }).ownership;
  const t = own.claim(g1, { lineId: 'L', claimant: 'gw2', now: 700, ownershipTtlMs: 500 }); // expired at 600
  assert.equal(t.outcome.kind, 'takeover');
  assert.equal(t.ownership.owner, 'gw2');
  assert.equal(t.ownership.generation, 2);
  assert.equal(t.events[0]?.type, 'OWNERSHIP_TAKEOVER');
  assert.equal(t.events[0]?.generation, 2);
});

// --- service-level generation fencing ---

test('lease stamps the ownership generation onto the command', () => {
  const clock = new ManualClock();
  const { svc } = makeService(clock);
  svc.claimOwnership('lineA', 'gw1'); // generation 1
  svc.submit({ idempotencyKey: 'k', deviceId: 'd', kind: 'calibrate', lineId: 'lineA' });
  const lease = svc.leaseNext('gw1', { lineId: 'lineA', callerGeneration: 1 });
  assert.ok(lease);
  assert.equal(lease!.ownerGeneration, 1);
});

test('a stale generation cannot lease work after takeover', () => {
  const clock = new ManualClock();
  const { svc } = makeService(clock);
  svc.claimOwnership('lineA', 'gw1'); // gen 1
  svc.submit({ idempotencyKey: 'k', deviceId: 'd', kind: 'calibrate', lineId: 'lineA' });

  // gw1 lets its ownership lapse; gw2 takes over -> gen 2.
  clock.advance(1500);
  const takeover = svc.claimOwnership('lineA', 'gw2');
  assert.equal(takeover.outcome, 'takeover');
  assert.equal(takeover.ownership.generation, 2);

  // gw1 (still believing it is gen 1) tries to lease -> fenced, nothing leased.
  const fenced = svc.leaseNext('gw1', { lineId: 'lineA', callerGeneration: 1 });
  assert.equal(fenced, null);
  // The audit trail records the fence.
  const cmd = svc.list('PENDING')[0]!;
  const evs = svc.history(cmd.id).map((e) => e.type);
  assert.ok(evs.includes('LEASE_FENCED'));

  // gw2 (gen 2) can lease it.
  const ok = svc.leaseNext('gw2', { lineId: 'lineA', callerGeneration: 2 });
  assert.ok(ok);
  assert.equal(ok!.ownerGeneration, 2);
});

test('split-brain: old generation confirmation is fenced even with a valid lease id', () => {
  const clock = new ManualClock();
  // Long per-command lease, short ownership TTL: ownership can lapse while the
  // command lease is still live — exactly the split-brain window.
  const { svc } = makeService(clock, { leaseDurationMs: 10_000, ownershipTtlMs: 1000 });
  svc.claimOwnership('lineA', 'gw1'); // gen 1
  const sub = svc.submit({ idempotencyKey: 'k', deviceId: 'd', kind: 'calibrate', lineId: 'lineA' });

  // gw1 leases the command under gen 1 and holds a valid, long-lived lease id.
  const lease1 = svc.leaseNext('gw1', { lineId: 'lineA', callerGeneration: 1 })!;

  // Partition: gw1's ownership lapses, gw2 takes over -> gen 2. The per-command
  // lease is STILL within its (10s) window at this instant.
  clock.advance(1500);
  const takeover = svc.claimOwnership('lineA', 'gw2');
  assert.equal(takeover.ownership.generation, 2);

  // gw1 comes back and confirms with its still-valid lease id but stale gen 1.
  const late = svc.confirm(lease1.command.id, lease1.leaseId, 'success', {
    callerGeneration: 1,
  });
  assert.equal(late.accepted, false);
  assert.equal(late.reason, 'stale_generation');
  assert.notEqual(svc.getById(sub.command.id)!.status, 'SUCCEEDED');
});

test('new generation can preempt an in-flight command held by an old generation', () => {
  const clock = new ManualClock();
  const { svc } = makeService(clock, { leaseDurationMs: 10_000 }); // long per-command lease
  svc.claimOwnership('lineA', 'gw1'); // gen 1
  const sub = svc.submit({ idempotencyKey: 'k', deviceId: 'd', kind: 'calibrate', lineId: 'lineA' });
  const lease1 = svc.leaseNext('gw1', { lineId: 'lineA', callerGeneration: 1 })!;
  assert.equal(lease1.ownerGeneration, 1);

  // gw1 ownership lapses (ownershipTtl 1000 < per-command lease 10000); gw2 takes over.
  clock.advance(1500);
  svc.claimOwnership('lineA', 'gw2'); // gen 2

  // gw2 leases: the command's per-command lease is still live, but it is held by
  // a superseded generation, so gw2 preempts it (reusing the stable executionId).
  const lease2 = svc.leaseNext('gw2', { lineId: 'lineA', callerGeneration: 2 });
  assert.ok(lease2);
  assert.equal(lease2!.preempted, true);
  assert.equal(lease2!.executionId, sub.command.id); // stable identity preserved
  assert.equal(lease2!.ownerGeneration, 2);

  // The preemption is causally recorded.
  const evs = svc.history(sub.command.id).map((e) => e.type);
  assert.ok(evs.includes('PREEMPTED'));

  // Only gen 2 can now confirm.
  const ok = svc.confirm(lease2!.command.id, lease2!.leaseId, 'success', { callerGeneration: 2 });
  assert.equal(ok.accepted, true);
  assert.equal(svc.getById(sub.command.id)!.status, 'SUCCEEDED');
});

test('ownership takeover history reconstructs before/after causality', () => {
  const clock = new ManualClock();
  const { svc } = makeService(clock);
  svc.claimOwnership('lineA', 'gw1');
  clock.advance(1500);
  svc.claimOwnership('lineA', 'gw2'); // takeover
  const lineEvents = svc.history('lineA');
  const types = lineEvents.map((e) => e.type);
  assert.deepEqual(types, ['OWNERSHIP_ACQUIRED', 'OWNERSHIP_TAKEOVER']);
  // The takeover event names both the previous and new owner + generations.
  const takeover = lineEvents.find((e) => e.type === 'OWNERSHIP_TAKEOVER')!;
  assert.equal(takeover.detail.previousOwner, 'gw1');
  assert.equal(takeover.detail.newOwner, 'gw2');
  assert.equal(takeover.detail.previousGeneration, 1);
  assert.equal(takeover.generation, 2);
});

test('generation fencing is opt-in: null generations behave like the single-gateway path', () => {
  const clock = new ManualClock();
  const { svc } = makeService(clock);
  // No ownership claimed; lease/confirm with null generation must still work.
  svc.submit({ idempotencyKey: 'k', deviceId: 'd', kind: 'calibrate', lineId: 'lineA' });
  const lease = svc.leaseNext('gw1', { lineId: 'lineA' }); // callerGeneration undefined
  assert.ok(lease);
  const ok = svc.confirm(lease!.command.id, lease!.leaseId, 'success');
  assert.equal(ok.accepted, true);
});
