/**
 * 领域状态机的确定性测试:注入时钟与 ID 生成器,精确复现
 * 崩溃/超时/迟到/乱序/重复等故障时序,不依赖任何真实 IO。
 */
import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  Ctx,
  decideAck,
  decideClaim,
  decideRenew,
  decideSubmit,
  expiryDecision,
} from '../src/domain/machine';
import { Attempt, Command, EventType } from '../src/domain/types';

const config = { leaseTtlMs: 1000, maxAttempts: 2, retryBackoffMs: 100 };

function makeCtx(now: number): Ctx {
  let n = 0;
  return { now, newId: () => `id-${++n}`, config, causedBy: 'test' };
}

function submittedCommand(now = 0) {
  const ctx = makeCtx(now);
  const d = decideSubmit(null, { idempotencyKey: 'k1', action: 'calibrate', params: { a: 1 } }, ctx);
  return { command: d.writes.command!, ctx, reply: d.reply };
}

function claimed(command: Command, now: number, gateway = 'gw-1') {
  const ctx = makeCtx(now);
  const d = decideClaim(command, gateway, ctx);
  return { command: d.writes.command!, attempt: d.writes.attempt!, ctx, task: d.reply, writes: d.writes };
}

function types(events: { type: EventType }[]): EventType[] {
  return events.map((e) => e.type);
}

test('提交:相同幂等键去重,不产生第二条设备动作', () => {
  const { command } = submittedCommand();
  const ctx = makeCtx(10);
  const d = decideSubmit(command, { idempotencyKey: 'k1', action: 'calibrate', params: { a: 1 } }, ctx);
  assert.equal(d.reply.deduped, true);
  assert.equal(d.reply.commandId, command.id);
  assert.equal(d.writes.command, undefined, '不得写入新指令');
  assert.deepEqual(types(d.writes.events), ['SUBMISSION_DEDUPED']);
});

test('领取:生成 ACTIVE 租约,executionToken 稳定', () => {
  const { command } = submittedCommand();
  const c1 = claimed(command, 0);
  assert.equal(c1.task.attemptNo, 1);
  assert.equal(c1.task.executionToken, command.executionToken);
  assert.equal(c1.attempt.leaseExpiresAt, 1000);
  assert.deepEqual(types(c1.writes.events), ['DELIVERY_STARTED']);
});

test('确认:只有活跃租约内的确认才能生效并推进到 SUCCEEDED', () => {
  const { command } = submittedCommand();
  const c1 = claimed(command, 0);
  const ctx = makeCtx(500);
  const d = decideAck(c1.command, c1.attempt, { ackId: 'a1', result: { ok: true } }, null, ctx);
  assert.equal(d.reply.outcome, 'applied');
  assert.equal(d.writes.command!.status, 'SUCCEEDED');
  assert.equal(d.writes.ack!.applied, true);
  assert.deepEqual(types(d.writes.events), ['DEVICE_ACK_APPLIED', 'COMMAND_SUCCEEDED']);
});

test('重复确认:相同 ackId 幂等重放,不二次生效', () => {
  const { command } = submittedCommand();
  const c1 = claimed(command, 0);
  const first = decideAck(c1.command, c1.attempt, { ackId: 'a1', result: null }, null, makeCtx(500));
  const succeeded = first.writes.command!;
  const ctx = makeCtx(600);
  const dup = decideAck(succeeded, first.writes.attempt!, { ackId: 'a1', result: null }, first.writes.ack!, ctx);
  assert.equal(dup.reply.duplicate, true);
  assert.equal(dup.reply.outcome, 'applied');
  assert.equal(dup.writes.command, undefined);
  assert.equal(dup.writes.ack, undefined);
  assert.deepEqual(types(dup.writes.events), ['DEVICE_ACK_DUPLICATE']);
});

test('迟到确认:租约过期后到达,先结算过期再忽略,不能标成功', () => {
  const { command } = submittedCommand();
  const c1 = claimed(command, 0);
  const ctx = makeCtx(1500); // 已越过 leaseExpiresAt=1000
  const d = decideAck(c1.command, c1.attempt, { ackId: 'a-late', result: null }, null, ctx);
  assert.equal(d.reply.outcome, 'ignored');
  assert.equal(d.reply.reason, 'lease_expired');
  assert.equal(d.writes.ack!.applied, false, '确认被持久化但不生效');
  assert.equal(d.writes.command!.status, 'PENDING', '回到待派而不是成功');
  assert.deepEqual(types(d.writes.events), ['LEASE_EXPIRED', 'RETRY_SCHEDULED', 'DEVICE_ACK_IGNORED']);
});

test('重新领取:沿用稳定执行身份,attempt 递增', () => {
  const { command } = submittedCommand();
  const c1 = claimed(command, 0);
  const ctx = makeCtx(1500);
  const exp = expiryDecision(c1.command, c1.attempt, ctx)!;
  const c2ctx = makeCtx(exp.command.nextAttemptNotBefore);
  const c2 = decideClaim(exp.command, 'gw-2', c2ctx);
  assert.equal(c2.reply.attemptNo, 2);
  assert.equal(c2.reply.executionToken, command.executionToken, '执行身份沿用');
});

test('终态不可逆:FAILED 后迟到确认持久化但被忽略', () => {
  const { command } = submittedCommand();
  const c1 = claimed(command, 0);
  const e1 = expiryDecision(c1.command, c1.attempt, makeCtx(1500))!;
  const c2 = claimed(e1.command, e1.command.nextAttemptNotBefore);
  const e2 = expiryDecision(c2.command, c2.attempt, makeCtx(c2.attempt.leaseExpiresAt + 1))!;
  assert.equal(e2.command.status, 'FAILED', '重试耗尽');
  assert.deepEqual(types(e2.events), ['LEASE_EXPIRED', 'COMMAND_FAILED']);

  const late = decideAck(e2.command, e2.attempt, { ackId: 'too-late', result: null }, null, makeCtx(99999));
  assert.equal(late.reply.outcome, 'ignored');
  assert.equal(late.reply.reason, 'command_terminal');
  assert.equal(late.writes.command, undefined, '终态不被改写');
});

test('乱序:SUCCEEDED 后到达的旧租约确认被忽略', () => {
  const { command } = submittedCommand();
  const c1 = claimed(command, 0);
  const done = decideAck(c1.command, c1.attempt, { ackId: 'a1', result: null }, null, makeCtx(500));
  const late = decideAck(
    done.writes.command!,
    done.writes.attempt!,
    { ackId: 'a1-shadow', result: null },
    null,
    makeCtx(700),
  );
  assert.equal(late.reply.reason, 'command_already_succeeded');
  assert.equal(late.writes.command, undefined);
});

test('续约:活跃期延长租约;过期后被拒并结算过期', () => {
  const { command } = submittedCommand();
  const c1 = claimed(command, 0);
  const okRenew = decideRenew(c1.command, c1.attempt, makeCtx(800));
  assert.deepEqual(okRenew.reply, { ok: true, leaseExpiresAt: 1800 });

  const lateRenew = decideRenew(c1.command, c1.attempt, makeCtx(1200));
  assert.deepEqual(lateRenew.reply, { ok: false, reason: 'lease_expired' });
  assert.deepEqual(types(lateRenew.writes.events), ['LEASE_EXPIRED', 'RETRY_SCHEDULED', 'RENEW_REJECTED']);
});

test('退避:nextAttemptNotBefore 之前不可再次领取(由存储层查询保证),过期判定幂等', () => {
  const { command } = submittedCommand();
  const c1 = claimed(command, 0);
  const exp = expiryDecision(c1.command, c1.attempt, makeCtx(1500))!;
  assert.equal(exp.command.nextAttemptNotBefore, 1500 + 100 * 1, '线性退避');
  assert.equal(expiryDecision(exp.command, exp.attempt, makeCtx(2000)), null, '已结算则不再重复结算');
});
