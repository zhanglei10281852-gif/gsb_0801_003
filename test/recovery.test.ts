/**
 * 崩溃恢复集成测试:真实 SQLite 文件,通过"关闭仓库 → 同库重新建服务"
 * 模拟进程崩溃重启,验证恢复边界——服务不依赖任何内存状态。
 */
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { DispatchService } from '../src/service/dispatchService';
import { SqliteRepository } from '../src/storage/sqlite';

const config = { leaseTtlMs: 1000, maxAttempts: 2, retryBackoffMs: 100 };

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
    const first = a.service.submit({ idempotencyKey: 'biz-1', action: 'calibrate', params: {} }, 'req-1');
    // 模拟"持久化已提交、响应未送达"的崩溃:直接丢弃进程状态
    a.repo.close();

    const b = boot(h.dbPath, h.now);
    const retry = b.service.submit({ idempotencyKey: 'biz-1', action: 'calibrate', params: {} }, 'req-2');
    assert.equal(retry.commandId, first.commandId, '同一业务请求只有一条指令');
    assert.equal(retry.deduped, true);

    const events = b.service.listEvents(first.commandId);
    assert.equal(events.filter((e) => e.type === 'COMMAND_ACCEPTED').length, 1);
    b.repo.close();
  } finally {
    h.cleanup();
  }
});

test('恢复:网关领走后崩溃,重启后租约仍在;过期后可被其他网关领取且沿用执行身份', () => {
  const h = harness();
  try {
    let a = boot(h.dbPath, h.now);
    a.service.submit({ idempotencyKey: 'biz-2', action: 'switch', params: {} }, 'req-1');
    const [t1] = a.service.claim('gw-1', 1, 'req-2');
    a.repo.close(); // 服务崩溃

    const b = boot(h.dbPath, h.now);
    // 租约未过期:其他网关领不到
    assert.equal(b.service.claim('gw-2', 1, 'req-3').length, 0);
    // 越过租约:结算过期并进入退避;越过退避后 gw-2 领到同一执行身份的第 2 次尝试
    h.advance(1500);
    b.service.expireDue();
    h.advance(200);
    const [t2] = b.service.claim('gw-2', 1, 'req-4');
    assert.equal(t2.executionToken, t1.executionToken);
    assert.equal(t2.attemptNo, 2);
    b.repo.close();
  } finally {
    h.cleanup();
  }
});

test('恢复:迟到确认在重启后仍被忽略;只有持久化确认能标成功', () => {
  const h = harness();
  try {
    let a = boot(h.dbPath, h.now);
    a.service.submit({ idempotencyKey: 'biz-3', action: 'calibrate', params: {} }, 'req-1');
    const [t1] = a.service.claim('gw-1', 1, 'req-2');
    h.advance(1500);
    a.service.expireDue();
    h.advance(200);
    const [t2] = a.service.claim('gw-2', 1, 'req-3');
    a.repo.close(); // 崩溃

    const b = boot(h.dbPath, h.now);
    // 旧租约的迟到确认:持久化但忽略
    const late = b.service.ack(t1.leaseId, 'gw-1', { ackId: 'late-1', result: null }, 'req-5');
    assert.equal(late.outcome, 'ignored');
    // 当前租约的确认:生效并标成功
    const ok = b.service.ack(t2.leaseId, 'gw-2', { ackId: 'ok-1', result: { done: true } }, 'req-6');
    assert.equal(ok.outcome, 'applied');
    assert.equal(b.service.getCommand(t2.commandId)!.status, 'SUCCEEDED');
    // 重启后重复确认仍幂等
    b.repo.close();
    const c = boot(h.dbPath, h.now);
    const dup = c.service.ack(t2.leaseId, 'gw-2', { ackId: 'ok-1', result: { done: true } }, 'req-7');
    assert.equal(dup.duplicate, true);
    assert.equal(c.service.getCommand(t2.commandId)!.status, 'SUCCEEDED');
    // 因果链完整
    const types = c.service.listEvents(t2.commandId).map((e) => e.type);
    assert.deepEqual(types, [
      'COMMAND_ACCEPTED',
      'DELIVERY_STARTED',
      'LEASE_EXPIRED',
      'RETRY_SCHEDULED',
      'DELIVERY_STARTED',
      'DEVICE_ACK_IGNORED',
      'DEVICE_ACK_APPLIED',
      'COMMAND_SUCCEEDED',
      'DEVICE_ACK_DUPLICATE',
    ]);
    c.repo.close();
  } finally {
    h.cleanup();
  }
});

test('恢复:重试耗尽进入 FAILED 后重启,迟到确认不能复活指令', () => {
  const h = harness();
  try {
    let a = boot(h.dbPath, h.now);
    a.service.submit({ idempotencyKey: 'biz-4', action: 'switch', params: {} }, 'req-1');
    const [t1] = a.service.claim('gw-1', 1, 'req-2');
    h.advance(1500);
    a.service.expireDue();
    h.advance(200);
    const [t2] = a.service.claim('gw-1', 1, 'req-3');
    h.advance(1500);
    a.service.expireDue(); // 结算第二次过期 → FAILED
    assert.equal(a.service.getCommand(t1.commandId)!.status, 'FAILED');
    a.repo.close();

    const b = boot(h.dbPath, h.now);
    const late = b.service.ack(t2.leaseId, 'gw-1', { ackId: 'ghost', result: null }, 'req-9');
    assert.equal(late.outcome, 'ignored');
    assert.equal(late.reason, 'command_terminal');
    assert.equal(b.service.getCommand(t1.commandId)!.status, 'FAILED');
    b.repo.close();
  } finally {
    h.cleanup();
  }
});
