/**
 * 领域状态机的确定性测试:注入时钟与 ID 生成器,精确复现
 * 崩溃/超时/网络分区/迟到/乱序/重复等故障时序,不依赖任何真实 IO。
 */
import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  Ctx,
  abortDecision,
  decideAck,
  decideClaim,
  decideHeartbeat,
  decideRenew,
  decideSubmit,
  expiryDecision,
  fenceStatus,
} from '../src/domain/machine';
import { Attempt, Command, EventType, Ownership } from '../src/domain/types';

const config = { leaseTtlMs: 1000, maxAttempts: 2, retryBackoffMs: 100, ownershipLeaseTtlMs: 5000 };

function makeCtx(now: number): Ctx {
  let n = 0;
  return { now, newId: () => `id-${++n}`, config, causedBy: 'test' };
}

/** 构造"gateway 以 gen 持有产线、所有权租约永不过期"的所有权(纯状态机用例聚焦指令流转) */
function own(gateway = 'gw-1', generation = 1, expiresAt = Number.MAX_SAFE_INTEGER): Ownership {
  return { lineId: 'default', ownerGatewayId: gateway, generation, leaseExpiresAt: expiresAt, updatedAt: 0 };
}

function submittedCommand(now = 0) {
  const ctx = makeCtx(now);
  const d = decideSubmit(null, { idempotencyKey: 'k1', action: 'calibrate', params: { a: 1 } }, ctx);
  return { command: d.writes.command!, ctx, reply: d.reply };
}

function claimed(command: Command, now: number, gateway = 'gw-1', generation = 1) {
  const ctx = makeCtx(now);
  const d = decideClaim(command, gateway, generation, ctx);
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

test('领取:生成 ACTIVE 租约并记录所有权代际,executionToken 稳定', () => {
  const { command } = submittedCommand();
  const c1 = claimed(command, 0);
  assert.equal(c1.task.attemptNo, 1);
  assert.equal(c1.task.executionToken, command.executionToken);
  assert.equal(c1.task.generation, 1);
  assert.equal(c1.attempt.generation, 1);
  assert.equal(c1.attempt.leaseExpiresAt, 1000);
  assert.deepEqual(types(c1.writes.events), ['DELIVERY_STARTED']);
});

test('确认:只有活跃租约内且持当前代际的确认才能生效并推进到 SUCCEEDED', () => {
  const { command } = submittedCommand();
  const c1 = claimed(command, 0);
  const ctx = makeCtx(500);
  const d = decideAck(c1.command, c1.attempt, { ackId: 'a1', result: { ok: true }, generation: 1 }, own(), null, ctx);
  assert.equal(d.reply.outcome, 'applied');
  assert.equal(d.writes.command!.status, 'SUCCEEDED');
  assert.equal(d.writes.ack!.applied, true);
  assert.deepEqual(types(d.writes.events), ['DEVICE_ACK_APPLIED', 'COMMAND_SUCCEEDED']);
});

test('重复确认:相同 ackId 幂等重放,不二次生效', () => {
  const { command } = submittedCommand();
  const c1 = claimed(command, 0);
  const first = decideAck(c1.command, c1.attempt, { ackId: 'a1', result: null, generation: 1 }, own(), null, makeCtx(500));
  const succeeded = first.writes.command!;
  const ctx = makeCtx(600);
  const dup = decideAck(succeeded, first.writes.attempt!, { ackId: 'a1', result: null, generation: 1 }, own(), first.writes.ack!, ctx);
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
  const d = decideAck(c1.command, c1.attempt, { ackId: 'a-late', result: null, generation: 1 }, own(), null, ctx);
  assert.equal(d.reply.outcome, 'ignored');
  assert.equal(d.reply.reason, 'lease_expired');
  assert.equal(d.writes.ack!.applied, false, '确认被持久化但不生效');
  assert.equal(d.writes.command!.status, 'PENDING', '回到待派而不是成功');
  assert.deepEqual(types(d.writes.events), ['LEASE_EXPIRED', 'RETRY_SCHEDULED', 'DEVICE_ACK_IGNORED']);
});

test('重新领取:沿用稳定执行身份,attempt 与代际递增', () => {
  const { command } = submittedCommand();
  const c1 = claimed(command, 0);
  const ctx = makeCtx(1500);
  const exp = expiryDecision(c1.command, c1.attempt, ctx)!;
  const c2ctx = makeCtx(exp.command.nextAttemptNotBefore);
  const c2 = decideClaim(exp.command, 'gw-2', 2, c2ctx);
  assert.equal(c2.reply.attemptNo, 2);
  assert.equal(c2.reply.generation, 2, '新代际投递被记录');
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

  const late = decideAck(e2.command, e2.attempt, { ackId: 'too-late', result: null, generation: 1 }, own(), null, makeCtx(99999));
  assert.equal(late.reply.outcome, 'ignored');
  assert.equal(late.reply.reason, 'command_terminal');
  assert.equal(late.writes.command, undefined, '终态不被改写');
});

test('乱序:SUCCEEDED 后到达的旧租约确认被忽略', () => {
  const { command } = submittedCommand();
  const c1 = claimed(command, 0);
  const done = decideAck(c1.command, c1.attempt, { ackId: 'a1', result: null, generation: 1 }, own(), null, makeCtx(500));
  const late = decideAck(
    done.writes.command!,
    done.writes.attempt!,
    { ackId: 'a1-shadow', result: null, generation: 1 },
    own(),
    null,
    makeCtx(700),
  );
  assert.equal(late.reply.reason, 'command_already_succeeded');
  assert.equal(late.writes.command, undefined);
});

test('续约:活跃期延长租约;过期后被拒并结算过期', () => {
  const { command } = submittedCommand();
  const c1 = claimed(command, 0);
  const okRenew = decideRenew(c1.command, c1.attempt, 1, own(), makeCtx(800));
  assert.deepEqual(okRenew.reply, { ok: true, leaseExpiresAt: 1800 });

  const lateRenew = decideRenew(c1.command, c1.attempt, 1, own(), makeCtx(1200));
  assert.deepEqual(lateRenew.reply, { ok: false, reason: 'lease_expired' });
  assert.deepEqual(types(lateRenew.writes.events), ['LEASE_EXPIRED', 'RETRY_SCHEDULED', 'RENEW_REJECTED']);
});

test('退避:过期后按线性退避重派,过期判定幂等', () => {
  const { command } = submittedCommand();
  const c1 = claimed(command, 0);
  const exp = expiryDecision(c1.command, c1.attempt, makeCtx(1500))!;
  assert.equal(exp.command.nextAttemptNotBefore, 1500 + 100 * 1, '线性退避');
  assert.equal(expiryDecision(exp.command, exp.attempt, makeCtx(2000)), null, '已结算则不再重复结算');
});

// ------------------------------------------------------------ 所有权与 fencing

test('心跳:获取 → 续约 → 争抢被拒 → 过期后接管(代际 +1)', () => {
  const ctx1 = makeCtx(0);
  const acq = decideHeartbeat(null, 'gw-1', 'line-1', ctx1);
  assert.equal(acq.reply.acquired, true);
  assert.equal(acq.reply.generation, 1);
  assert.deepEqual(types(acq.writes.events), ['OWNERSHIP_ACQUIRED']);

  const ren = decideHeartbeat(acq.writes.ownership!, 'gw-1', 'line-1', makeCtx(100));
  assert.equal(ren.reply.acquired, true);
  assert.equal(ren.reply.generation, 1, '续约不改变代际');
  assert.deepEqual(types(ren.writes.events), ['OWNERSHIP_RENEWED']);

  const contend = decideHeartbeat(acq.writes.ownership!, 'gw-2', 'line-1', makeCtx(200));
  assert.equal(contend.reply.acquired, false, '他人持有且未过期:争抢被拒');
  assert.equal(contend.reply.owner, 'gw-1');
  assert.deepEqual(types(contend.writes.events), ['OWNERSHIP_HEARTBEAT_REJECTED']);
  assert.equal(contend.writes.ownership, undefined, '争抢不改所有权');

  const takeover = decideHeartbeat(acq.writes.ownership!, 'gw-2', 'line-1', makeCtx(config.ownershipLeaseTtlMs + 1));
  assert.equal(takeover.reply.acquired, true);
  assert.equal(takeover.reply.generation, 2, '接管代际 +1');
  assert.equal(takeover.reply.tookOverFrom, 'gw-1');
  assert.deepEqual(types(takeover.writes.events), ['OWNERSHIP_TAKEN_OVER']);
});

test('心跳:旧属主失联后自行重新获取,代际同样 +1(其在途投递必须作废)', () => {
  const acq = decideHeartbeat(null, 'gw-1', 'line-1', makeCtx(0));
  const reacq = decideHeartbeat(acq.writes.ownership!, 'gw-1', 'line-1', makeCtx(config.ownershipLeaseTtlMs + 1));
  assert.equal(reacq.reply.generation, 2);
  assert.equal(reacq.reply.tookOverFrom, 'gw-1');
});

test('fencing 判定矩阵', () => {
  const o = own('gw-1', 2, 1000);
  assert.equal(fenceStatus(o, 'gw-1', 2, 500), 'ok');
  assert.equal(fenceStatus(null, 'gw-1', 1, 500), 'no_ownership');
  assert.equal(fenceStatus(o, 'gw-2', 2, 500), 'not_owner');
  assert.equal(fenceStatus(o, 'gw-1', 1, 500), 'stale_generation');
  assert.equal(fenceStatus(o, 'gw-1', 2, 1500), 'ownership_lease_expired');
});

test('fencing:旧代际确认被持久化留痕但绝不推进状态', () => {
  const { command } = submittedCommand();
  const c1 = claimed(command, 0, 'gw-1', 1);
  // 接管后:属主变为 gw-2 gen=2;gw-1 用旧代际 1 回传设备确认
  const afterTakeover = own('gw-2', 2);
  const d = decideAck(c1.command, c1.attempt, { ackId: 'partition-ack', result: null, generation: 1 }, afterTakeover, null, makeCtx(500));
  assert.equal(d.reply.outcome, 'ignored');
  assert.equal(d.reply.reason, 'fenced_not_owner');
  assert.equal(d.writes.ack!.applied, false, '确认留痕但不生效');
  assert.equal(d.writes.command, undefined, '指令状态不被旧代际推进');
  assert.deepEqual(types(d.writes.events), ['ACK_FENCED']);
});

test('fencing:同一网关持旧代际(stale_generation)与所有权租约过期都被拒', () => {
  const { command } = submittedCommand();
  const c1 = claimed(command, 0, 'gw-1', 1);
  const stale = decideAck(c1.command, c1.attempt, { ackId: 'x1', result: null, generation: 1 }, own('gw-1', 2), null, makeCtx(500));
  assert.equal(stale.reply.reason, 'fenced_stale_generation');

  const expired = decideAck(c1.command, c1.attempt, { ackId: 'x2', result: null, generation: 1 }, own('gw-1', 1, 100), null, makeCtx(500));
  assert.equal(expired.reply.reason, 'fenced_ownership_lease_expired', '所有权过期后即使是属主也被 fencing');
});

test('fencing:旧代际续约被拒且不改状态', () => {
  const { command } = submittedCommand();
  const c1 = claimed(command, 0, 'gw-1', 1);
  const d = decideRenew(c1.command, c1.attempt, 1, own('gw-2', 2), makeCtx(500));
  assert.deepEqual(d.reply, { ok: false, reason: 'fenced_not_owner' });
  assert.equal(d.writes.attempt, undefined);
  assert.equal(d.writes.command, undefined);
  assert.deepEqual(types(d.writes.events), ['RENEW_FENCED']);
});

test('接管中止:在途投递作废并立即(不退避)回到队列,执行身份沿用', () => {
  const { command } = submittedCommand();
  const c1 = claimed(command, 0, 'gw-1', 1);
  const ctx = makeCtx(300); // 租约远未到期,但所有权已被接管
  const ab = abortDecision(c1.command, c1.attempt, ctx, 'ownership_takeover')!;
  assert.equal(ab.command.status, 'PENDING');
  assert.equal(ab.command.nextAttemptNotBefore, 300, '接管中止不退避,新属主可立即领取');
  assert.equal(ab.attempt.status, 'EXPIRED');
  assert.deepEqual(types(ab.events), ['ATTEMPT_ABORTED', 'RETRY_SCHEDULED']);

  // 新属主以新代际重投:执行身份沿用
  const c2 = decideClaim(ab.command, 'gw-2', 2, makeCtx(300));
  assert.equal(c2.reply.executionToken, command.executionToken);
  assert.equal(c2.reply.attemptNo, 2);

  // 中止幂等:已非 ACTIVE 的尝试不再重复中止
  assert.equal(abortDecision(ab.command, ab.attempt, makeCtx(400), 'ownership_takeover'), null);
});
