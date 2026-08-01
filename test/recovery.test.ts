/**
 * 崩溃恢复集成测试:真实 SQLite 文件,通过"关闭仓库 → 同库重新建服务"
 * 模拟进程崩溃重启,验证恢复边界——服务不依赖任何内存状态,
 * 包括所有权代际:fencing 状态跨重启保持。
 */
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { DispatchService } from '../src/service/dispatchService';
import { SqliteRepository } from '../src/storage/sqlite';

const config = { leaseTtlMs: 1000, maxAttempts: 2, retryBackoffMs: 100, ownershipLeaseTtlMs: 800 };

function boot(dbPath: string, now: () => number) {
  const repo = new SqliteRepository(dbPath);
  const service = new DispatchService(repo, config, now);
  return { repo, service };
}

function harness() {
  const dir = mkdtempSync(join(tmpdir(), 'dispatch-test-'));
  const dbPath = join(dir, 'test.db');
  let now = 0;
  return {
    dbPath,
    dir,
    now: () => now,
    advance: (ms: number) => {
      now += ms;
    },
    cleanup: () => rmSync(dir, { recursive: true, force: true }),
  };
}

test('恢复:持久化后"崩溃"(未拿到响应),重启后同幂等键重提不生成第二条指令', () => {
  const h = harness();
  try {
    let a = boot(h.dbPath, h.now);
    const first = a.service.submit({ idempotencyKey: 'biz-1', action: 'calibrate', params: {}, lineId: 'line-1' }, 'req-1');
    // 模拟"持久化已提交、响应未送达"的崩溃:直接丢弃进程状态
    a.repo.close();

    const b = boot(h.dbPath, h.now);
    const retry = b.service.submit({ idempotencyKey: 'biz-1', action: 'calibrate', params: {}, lineId: 'line-1' }, 'req-2');
    assert.equal(retry.commandId, first.commandId, '同一业务请求只有一条指令');
    assert.equal(retry.deduped, true);

    const events = b.service.listEvents(first.commandId);
    assert.equal(events.filter((e) => e.type === 'COMMAND_ACCEPTED').length, 1);
    b.repo.close();
  } finally {
    h.cleanup();
  }
});

test('恢复:网关领走后崩溃,重启后租约与所有权仍在;接管后可被其他网关领取且沿用执行身份', () => {
  const h = harness();
  try {
    let a = boot(h.dbPath, h.now);
    a.service.submit({ idempotencyKey: 'biz-2', action: 'switch', params: {}, lineId: 'line-1' }, 'req-1');
    a.service.heartbeat('gw-1', 'line-1', 'req-h1');
    const r1 = a.service.claim('gw-1', 'line-1', 1, 1, 'req-2');
    assert.ok('tasks' in r1);
    const t1 = r1.tasks[0];
    a.repo.close(); // 服务崩溃

    const b = boot(h.dbPath, h.now);
    // 所有权未易主:其他网关领取被 fencing
    const fenced = b.service.claim('gw-2', 'line-1', 1, 1, 'req-3');
    assert.ok('fenced' in fenced && fenced.fenced === 'not_owner');
    // 主网关心跳中断 → 所有权到期 → gw-2 接管(代际+1,在途投递被中止)
    h.advance(1500);
    const hb = b.service.heartbeat('gw-2', 'line-1', 'req-h2');
    assert.equal(hb.acquired, true);
    assert.equal(hb.generation, 2);
    assert.equal(hb.tookOverFrom, 'gw-1');
    const r2 = b.service.claim('gw-2', 'line-1', 2, 1, 'req-4');
    assert.ok('tasks' in r2 && r2.tasks.length === 1);
    assert.equal(r2.tasks[0].executionToken, t1.executionToken, '重投沿用稳定执行身份');
    assert.equal(r2.tasks[0].attemptNo, 2);
    b.repo.close();
  } finally {
    h.cleanup();
  }
});

test('恢复:旧代际迟到确认在重启后仍被 fencing;只有当前代际的持久化确认能标成功', () => {
  const h = harness();
  try {
    let a = boot(h.dbPath, h.now);
    a.service.submit({ idempotencyKey: 'biz-3', action: 'calibrate', params: {}, lineId: 'line-1' }, 'req-1');
    a.service.heartbeat('gw-1', 'line-1', 'req-h1');
    const r1 = a.service.claim('gw-1', 'line-1', 1, 1, 'req-2');
    const t1 = 'tasks' in r1 ? r1.tasks[0] : null;
    h.advance(1500);
    a.service.heartbeat('gw-2', 'line-1', 'req-h2'); // 接管 gen=2,中止 t1
    const r2 = a.service.claim('gw-2', 'line-1', 2, 1, 'req-3');
    const t2 = 'tasks' in r2 ? r2.tasks[0] : null;
    a.repo.close(); // 崩溃

    const b = boot(h.dbPath, h.now);
    // 旧网关用旧代际回传确认:持久化留痕但被 fencing,不得推进状态
    const late = b.service.ack(t1!.leaseId, 'gw-1', { ackId: 'late-1', result: null, generation: 1 }, 'req-5');
    assert.equal(late.outcome, 'ignored');
    assert.ok(late.reason!.startsWith('fenced'));
    assert.equal(b.service.getCommand(t2!.commandId)!.status, 'DISPATCHED', '旧代际不得推进状态');
    // 当前属主、当前代际的确认:生效并标成功
    const ok = b.service.ack(t2!.leaseId, 'gw-2', { ackId: 'ok-1', result: { done: true }, generation: 2 }, 'req-6');
    assert.equal(ok.outcome, 'applied');
    assert.equal(b.service.getCommand(t2!.commandId)!.status, 'SUCCEEDED');
    // 重启后重复确认仍幂等
    b.repo.close();
    const c = boot(h.dbPath, h.now);
    const dup = c.service.ack(t2!.leaseId, 'gw-2', { ackId: 'ok-1', result: { done: true }, generation: 2 }, 'req-7');
    assert.equal(dup.duplicate, true);
    assert.equal(c.service.getCommand(t2!.commandId)!.status, 'SUCCEEDED');
    // 因果链完整(指令视角)
    const types = c.service.listEvents(t2!.commandId).map((e) => e.type);
    assert.deepEqual(types, [
      'COMMAND_ACCEPTED',
      'DELIVERY_STARTED',
      'ATTEMPT_ABORTED',
      'RETRY_SCHEDULED',
      'DELIVERY_STARTED',
      'ACK_FENCED',
      'DEVICE_ACK_APPLIED',
      'COMMAND_SUCCEEDED',
      'DEVICE_ACK_DUPLICATE',
    ]);
    // 产线视角可还原接管因果
    const lineTypes = c.service.listLineEvents('line-1').map((e) => e.type);
    for (const t of ['OWNERSHIP_ACQUIRED', 'OWNERSHIP_TAKEN_OVER', 'ATTEMPT_ABORTED', 'ACK_FENCED', 'COMMAND_SUCCEEDED']) {
      assert.ok(lineTypes.includes(t), `产线事件缺少 ${t}`);
    }
    c.repo.close();
  } finally {
    h.cleanup();
  }
});

test('恢复:重试耗尽进入 FAILED 后重启,任何确认都不能复活指令', () => {
  const h = harness();
  try {
    let a = boot(h.dbPath, h.now);
    a.service.submit({ idempotencyKey: 'biz-4', action: 'switch', params: {}, lineId: 'line-1' }, 'req-1');
    a.service.heartbeat('gw-1', 'line-1', 'req-h1');
    const r1 = a.service.claim('gw-1', 'line-1', 1, 1, 'req-2');
    const t1 = 'tasks' in r1 ? r1.tasks[0] : null;
    h.advance(1500);
    a.service.heartbeat('gw-1', 'line-1', 'req-h2'); // 所有权过期,自行重新获取 gen=2,中止在途投递
    const r2 = a.service.claim('gw-1', 'line-1', 2, 1, 'req-3');
    const t2 = 'tasks' in r2 ? r2.tasks[0] : null;
    h.advance(1500);
    a.service.expireDue(); // 结算第二次过期 → FAILED
    assert.equal(a.service.getCommand(t1!.commandId)!.status, 'FAILED');
    a.repo.close();

    const b = boot(h.dbPath, h.now);
    // 旧代际(gen=2,所有权已过期)的确认:fencing
    const fenced = b.service.ack(t2!.leaseId, 'gw-1', { ackId: 'ghost-1', result: null, generation: 2 }, 'req-8');
    assert.equal(fenced.outcome, 'ignored');
    assert.ok(fenced.reason!.startsWith('fenced'));
    // 重新获取所有权(gen=3)后的确认:终态忽略
    b.service.heartbeat('gw-1', 'line-1', 'req-h3');
    const late = b.service.ack(t2!.leaseId, 'gw-1', { ackId: 'ghost-2', result: null, generation: 3 }, 'req-9');
    assert.equal(late.outcome, 'ignored');
    assert.equal(late.reason, 'command_terminal');
    assert.equal(b.service.getCommand(t1!.commandId)!.status, 'FAILED');
    b.repo.close();
  } finally {
    h.cleanup();
  }
});

test('恢复:所有权代际跨重启保持——接管后崩溃重启,旧代际依旧被 fencing', () => {
  const h = harness();
  try {
    let a = boot(h.dbPath, h.now);
    a.service.submit({ idempotencyKey: 'biz-5', action: 'calibrate', params: {}, lineId: 'line-1' }, 'req-1');
    a.service.heartbeat('gw-1', 'line-1', 'req-h1');
    const r1 = a.service.claim('gw-1', 'line-1', 1, 1, 'req-2');
    const t1 = 'tasks' in r1 ? r1.tasks[0] : null;
    a.repo.close(); // 崩溃①

    const b = boot(h.dbPath, h.now);
    // 重启后所有权未丢:gw-2 仍被 fencing
    assert.ok('fenced' in b.service.claim('gw-2', 'line-1', 1, 1, 'req-3'));
    h.advance(1500);
    const hb = b.service.heartbeat('gw-2', 'line-1', 'req-h2'); // 接管 gen=2
    assert.equal(hb.generation, 2);
    b.repo.close(); // 崩溃②:接管刚落库即宕机

    const c = boot(h.dbPath, h.now);
    const own = c.service.getOwnership('line-1')!;
    assert.equal(own.ownerGatewayId, 'gw-2', '接管结果跨重启保持');
    assert.equal(own.generation, 2);
    // 旧网关持旧代际的一切操作依旧被 fencing
    const fencedAck = c.service.ack(t1!.leaseId, 'gw-1', { ackId: 'partition-1', result: null, generation: 1 }, 'req-4');
    assert.ok(fencedAck.reason!.startsWith('fenced'));
    assert.ok('fenced' in c.service.claim('gw-1', 'line-1', 1, 1, 'req-5'));
    // 新属主继续重投:执行身份沿用
    const r2 = c.service.claim('gw-2', 'line-1', 2, 1, 'req-6');
    assert.ok('tasks' in r2 && r2.tasks[0].executionToken === t1!.executionToken);
    c.repo.close();
  } finally {
    h.cleanup();
  }
});

test('恢复:撤回跨重启保持——重复撤回去重,迟到确认不能复活指令', () => {
  const h = harness();
  try {
    let a = boot(h.dbPath, h.now);
    a.service.submit({ idempotencyKey: 'biz-6', action: 'switch', params: {}, lineId: 'line-1' }, 'req-1');
    a.service.heartbeat('gw-1', 'line-1', 'req-h1');
    const r1 = a.service.claim('gw-1', 'line-1', 1, 1, 'req-2');
    const t1 = 'tasks' in r1 ? r1.tasks[0] : null;
    // 撤回 DISPATCHED 中的指令后立即"崩溃"(响应未送达)
    const cancelled = a.service.cancel('biz-6', { reason: 'e-stop' }, 'req-3');
    assert.deepEqual(cancelled, { cancelled: true, deduped: false });
    a.repo.close();

    const b = boot(h.dbPath, h.now);
    // 重启后:撤回保持;客户端盲目重试撤回 → 幂等去重
    assert.equal(b.service.getCommand('biz-6')!.status, 'CANCELLED');
    const dup = b.service.cancel('biz-6', {}, 'req-4');
    assert.deepEqual(dup, { cancelled: true, deduped: true });
    // 已撤回的指令不可被领取
    const empty = b.service.claim('gw-1', 'line-1', 1, 1, 'req-5');
    assert.ok('tasks' in empty && empty.tasks.length === 0);
    // 网关迟到确认(即便持当前代际):留痕但 command_cancelled,不能复活
    const late = b.service.ack(t1!.leaseId, 'gw-1', { ackId: 'after-cancel', result: null, generation: 1 }, 'req-6');
    assert.equal(late.outcome, 'ignored');
    assert.equal(late.reason, 'command_cancelled');
    assert.equal(b.service.getCommand('biz-6')!.status, 'CANCELLED');
    // 因果链
    const types = b.service.listEvents(b.service.getCommand('biz-6')!.id).map((e) => e.type);
    assert.deepEqual(types, [
      'COMMAND_ACCEPTED',
      'DELIVERY_STARTED',
      'ATTEMPT_ABORTED',
      'COMMAND_CANCELLED',
      'CANCEL_DEDUPED',
      'DEVICE_ACK_IGNORED',
    ]);
    b.repo.close();
  } finally {
    h.cleanup();
  }
});

test('恢复:已成功的指令重启后仍不能被撤回或取代', () => {
  const h = harness();
  try {
    let a = boot(h.dbPath, h.now);
    a.service.submit({ idempotencyKey: 'biz-7', action: 'calibrate', params: {}, lineId: 'line-1' }, 'req-1');
    a.service.heartbeat('gw-1', 'line-1', 'req-h1');
    const r1 = a.service.claim('gw-1', 'line-1', 1, 1, 'req-2');
    const t1 = 'tasks' in r1 ? r1.tasks[0] : null;
    a.service.ack(t1!.leaseId, 'gw-1', { ackId: 'ok', result: null, generation: 1 }, 'req-3');
    assert.equal(a.service.getCommand('biz-7')!.status, 'SUCCEEDED');
    a.repo.close();

    const b = boot(h.dbPath, h.now);
    const c = b.service.cancel('biz-7', {}, 'req-4');
    assert.deepEqual(c, { cancelled: false, reason: 'already_succeeded' });
    const s = b.service.submit({ idempotencyKey: 'biz-7-new', action: 'calibrate', params: {}, supersedesKey: 'biz-7' }, 'req-5');
    assert.deepEqual(s, { accepted: false, reason: 'already_succeeded' });
    assert.equal(b.service.getCommand('biz-7-new'), null, '被拒的取代不生成新指令');
    assert.equal(b.service.getCommand('biz-7')!.status, 'SUCCEEDED');
    b.repo.close();
  } finally {
    h.cleanup();
  }
});

test('恢复:取代原子性跨重启——旧指令退役与新指令生成同时可见,重试幂等', () => {
  const h = harness();
  try {
    let a = boot(h.dbPath, h.now);
    a.service.submit({ idempotencyKey: 'biz-8', action: 'calibrate', params: { v: 1 }, lineId: 'line-1' }, 'req-1');
    a.service.heartbeat('gw-1', 'line-1', 'req-h1');
    const r1 = a.service.claim('gw-1', 'line-1', 1, 1, 'req-2');
    const t1 = 'tasks' in r1 ? r1.tasks[0] : null;
    // 取代提交落库后立即"崩溃"(响应未送达)
    const first = a.service.submit({ idempotencyKey: 'biz-8-safe', action: 'calibrate', params: { v: 2 }, supersedesKey: 'biz-8' }, 'req-3');
    assert.ok('accepted' in first && first.accepted);
    a.repo.close();

    const b = boot(h.dbPath, h.now);
    // 原子性:旧 SUPERSEDED + 新 PENDING 同时可见
    assert.equal(b.service.getCommand('biz-8')!.status, 'SUPERSEDED');
    const newCmd = b.service.getCommand('biz-8-safe')!;
    assert.equal(newCmd.status, 'PENDING');
    assert.equal(newCmd.supersedesCommandId, b.service.getCommand('biz-8')!.id);
    assert.equal(b.service.getCommand('biz-8')!.supersededByCommandId, newCmd.id, '取代链双向可查');
    // 客户端崩溃重试:幂等重放,不生成第二条替代指令
    const retry = b.service.submit({ idempotencyKey: 'biz-8-safe', action: 'calibrate', params: { v: 2 }, supersedesKey: 'biz-8' }, 'req-4');
    assert.ok('accepted' in retry && retry.accepted && retry.deduped);
    // 旧指令迟到确认:command_superseded;新指令可被当前属主领取并走通成功边界
    const late = b.service.ack(t1!.leaseId, 'gw-1', { ackId: 'late-old', result: null, generation: 1 }, 'req-5');
    assert.equal(late.reason, 'command_superseded');
    const r2 = b.service.claim('gw-1', 'line-1', 1, 1, 'req-6');
    assert.ok('tasks' in r2 && r2.tasks.length === 1 && r2.tasks[0].attemptNo === 1);
    const ok = b.service.ack(r2.tasks[0].leaseId, 'gw-1', { ackId: 'ok-new', result: null, generation: 1 }, 'req-7');
    assert.equal(ok.outcome, 'applied');
    assert.equal(b.service.getCommand('biz-8-safe')!.status, 'SUCCEEDED');
    b.repo.close();
  } finally {
    h.cleanup();
  }
});
