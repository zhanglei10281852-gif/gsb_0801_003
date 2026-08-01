/**
 * Deterministic unit tests for the pure state machine. No I/O, no real clock:
 * every timestamp is a plain number, so failure interleavings (expiry, stale
 * confirms, re-lease) are reproducible exactly.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as sm from '../src/domain/stateMachine';
import { Command } from '../src/domain/types';

function newCmd(): Command {
  const t = sm.create({
    id: 'cmd_1',
    idempotencyKey: 'key_1',
    payload: { deviceId: 'dev', kind: 'calibrate', params: {} },
    now: 1000,
    maxAttempts: 3,
  });
  return t.command;
}

test('create yields a PENDING command with stable executionId == id', () => {
  const t = sm.create({
    id: 'cmd_x',
    idempotencyKey: 'k',
    payload: { deviceId: 'd', kind: 'k', params: {} },
    now: 5,
    maxAttempts: 2,
  });
  assert.equal(t.command.status, 'PENDING');
  assert.equal(t.command.executionId, 'cmd_x');
  assert.equal(t.command.attemptEpoch, 0);
  assert.equal(t.events[0]?.type, 'SUBMITTED');
});

test('lease bumps epoch/attempts and sets a live lease', () => {
  const cmd = newCmd();
  const t = sm.lease(cmd, {
    leaseholder: 'gw1',
    leaseId: 'L1',
    now: 2000,
    leaseDurationMs: 500,
  });
  assert.equal(t.command.status, 'LEASED');
  assert.equal(t.command.attemptEpoch, 1);
  assert.equal(t.command.attempts, 1);
  assert.equal(t.command.leaseExpiresAt, 2500);
  assert.equal(t.command.executionId, cmd.executionId); // stable identity
});

test('a live lease cannot be leased by someone else', () => {
  const leased = sm.lease(newCmd(), {
    leaseholder: 'gw1',
    leaseId: 'L1',
    now: 2000,
    leaseDurationMs: 500,
  }).command;
  const again = sm.lease(leased, {
    leaseholder: 'gw2',
    leaseId: 'L2',
    now: 2100, // still within lease
    leaseDurationMs: 500,
  });
  assert.equal(again.changed, false);
  assert.equal(again.outcome.kind, 'noop');
});

test('re-lease after expiry reuses executionId and bumps epoch (fencing)', () => {
  const leased = sm.lease(newCmd(), {
    leaseholder: 'gw1',
    leaseId: 'L1',
    now: 2000,
    leaseDurationMs: 500,
  }).command;
  const reacquired = sm.lease(leased, {
    leaseholder: 'gw2',
    leaseId: 'L2',
    now: 3000, // after expiry (2500)
    leaseDurationMs: 500,
  });
  assert.equal(reacquired.command.status, 'LEASED');
  assert.equal(reacquired.command.attemptEpoch, 2);
  assert.equal(reacquired.command.executionId, leased.executionId);
  assert.equal(reacquired.command.leaseId, 'L2');
  // Causal record includes the prior lease expiry.
  assert.ok(reacquired.events.some((e) => e.type === 'LEASE_EXPIRED'));
});

test('confirm only succeeds for the current, live lease', () => {
  const leased = sm.lease(newCmd(), {
    leaseholder: 'gw1',
    leaseId: 'L1',
    now: 2000,
    leaseDurationMs: 500,
  }).command;
  const ok = sm.confirm(leased, { leaseId: 'L1', outcome: 'success', now: 2200 });
  assert.equal(ok.command.status, 'SUCCEEDED');
  assert.equal(ok.command.terminalReason, 'device_success');
  assert.equal(ok.events[0]?.type, 'CONFIRMED_SUCCESS');
});

test('confirm with a stale lease id is ignored (no revival)', () => {
  const leased = sm.lease(newCmd(), {
    leaseholder: 'gw1',
    leaseId: 'L1',
    now: 2000,
    leaseDurationMs: 500,
  }).command;
  const stale = sm.confirm(leased, { leaseId: 'WRONG', outcome: 'success', now: 2200 });
  assert.equal(stale.changed, false);
  assert.equal(stale.command.status, 'LEASED');
  assert.equal(stale.outcome.kind, 'confirm_ignored');
  assert.equal(stale.events[0]?.type, 'CONFIRM_IGNORED');
});

test('confirm after lease expiry is ignored (late message cannot revive)', () => {
  const leased = sm.lease(newCmd(), {
    leaseholder: 'gw1',
    leaseId: 'L1',
    now: 2000,
    leaseDurationMs: 500,
  }).command;
  const late = sm.confirm(leased, { leaseId: 'L1', outcome: 'success', now: 9999 });
  assert.equal(late.changed, false);
  assert.equal(late.command.status, 'LEASED');
  if (late.outcome.kind === 'confirm_ignored') {
    assert.equal(late.outcome.reason, 'lease_expired');
  } else {
    assert.fail('expected confirm_ignored');
  }
});

test('duplicate confirm on a terminal command is idempotent', () => {
  const leased = sm.lease(newCmd(), {
    leaseholder: 'gw1',
    leaseId: 'L1',
    now: 2000,
    leaseDurationMs: 500,
  }).command;
  const done = sm.confirm(leased, { leaseId: 'L1', outcome: 'success', now: 2200 }).command;
  const again = sm.confirm(done, { leaseId: 'L1', outcome: 'success', now: 2300 });
  assert.equal(again.changed, false);
  assert.equal(again.command.status, 'SUCCEEDED');
  assert.equal(again.outcome.kind, 'confirm_ignored');
});

test('expire requeues while retry budget remains, then fails when exhausted', () => {
  let cmd = newCmd(); // maxAttempts = 3
  // attempt 1
  cmd = sm.lease(cmd, { leaseholder: 'g', leaseId: 'L1', now: 1000, leaseDurationMs: 100 }).command;
  cmd = sm.expire(cmd, 2000).command;
  assert.equal(cmd.status, 'PENDING');
  // attempt 2
  cmd = sm.lease(cmd, { leaseholder: 'g', leaseId: 'L2', now: 2000, leaseDurationMs: 100 }).command;
  cmd = sm.expire(cmd, 3000).command;
  assert.equal(cmd.status, 'PENDING');
  // attempt 3 (last allowed)
  cmd = sm.lease(cmd, { leaseholder: 'g', leaseId: 'L3', now: 3000, leaseDurationMs: 100 }).command;
  assert.equal(cmd.attempts, 3);
  const exhausted = sm.expire(cmd, 4000);
  assert.equal(exhausted.command.status, 'FAILED');
  assert.equal(exhausted.command.terminalReason, 'retries_exhausted');
  assert.ok(exhausted.events.some((e) => e.type === 'EXHAUSTED'));
});

test('renew extends only the current live lease', () => {
  const leased = sm.lease(newCmd(), {
    leaseholder: 'gw1',
    leaseId: 'L1',
    now: 2000,
    leaseDurationMs: 500,
  }).command;
  const good = sm.renew(leased, { leaseId: 'L1', now: 2400, leaseDurationMs: 500 });
  assert.equal(good.command.leaseExpiresAt, 2900);
  const bad = sm.renew(leased, { leaseId: 'WRONG', now: 2400, leaseDurationMs: 500 });
  assert.equal(bad.changed, false);
  assert.equal(bad.outcome.kind, 'renew_rejected');
});
