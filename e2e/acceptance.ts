/**
 * 端到端验收:真实启动【编译后】的服务进程(dist/src/server.js),
 * 并驱动【编译后】的模拟器(dist/simulator/cli.js)复现故障场景。
 *
 * 运行方式:npm run e2e(会先执行 npm run build)
 *
 * 验收内容:
 *  A. 崩溃注入:服务在"提交已落库、响应未发出"时崩溃 → 重启后同幂等键
 *     重提仍只有一条指令、只有一个 COMMAND_ACCEPTED。
 *  B. 子进程方式运行模拟器默认场景:双网关争抢、主失联后备用接管、旧代际
 *     被 fencing、跨代际重投沿用执行身份、重复/乱序确认、重试耗尽。
 *  C. 再次重启服务:状态完整恢复,重复提交仍被去重。
 *  D. 双冗余网关:争抢 → 主领取 → 调度进程重启(fencing 状态保持)→
 *     主沉默 → 备用接管 → 旧代际领取/确认被拒 → 新代际重投并确认成功,
 *     产线事件流可还原完整因果。
 */
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DispatchClient } from '../simulator/client';

const ROOT = join(__dirname, '..');
const SERVER_JS = join(ROOT, 'src', 'server.js');
const SIMULATOR_JS = join(ROOT, 'simulator', 'cli.js');
const PORT = 18080;
const BASE_URL = `http://127.0.0.1:${PORT}`;

const SERVER_ENV = {
  ...process.env,
  PORT: String(PORT),
  LEASE_TTL_MS: '800',
  MAX_ATTEMPTS: '3',
  RETRY_BACKOFF_MS: '50',
  OWNERSHIP_LEASE_TTL_MS: '2500',
  REAPER_INTERVAL_MS: '150',
};

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
let failures = 0;

function check(name: string, cond: boolean, detail = ''): void {
  if (cond) console.log(`  ✔ ${name}`);
  else {
    failures++;
    console.error(`  ✘ ${name} ${detail}`);
  }
}

interface ServerHandle {
  stop: () => Promise<void>;
}

async function startServer(dbPath: string, extraEnv: NodeJS.ProcessEnv = {}): Promise<ServerHandle> {
  const child = spawn(process.execPath, [SERVER_JS], {
    env: { ...SERVER_ENV, DB_PATH: dbPath, ...extraEnv },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  child.stderr.on('data', (d) => process.stderr.write(`[server] ${d}`));
  const client = new DispatchClient({ baseUrl: BASE_URL, timeoutMs: 1000 });
  const deadline = Date.now() + 15000;
  for (;;) {
    try {
      const h = await client.health();
      if (h.status === 200) break;
    } catch {
      if (child.exitCode !== null) throw new Error(`服务进程提前退出, exit=${child.exitCode}`);
      if (Date.now() > deadline) throw new Error('等待服务就绪超时');
      await sleep(150);
    }
  }
  return {
    stop: () =>
      new Promise((resolve) => {
        if (child.exitCode !== null) return resolve();
        child.once('exit', () => resolve());
        child.kill();
        setTimeout(() => {
          if (child.exitCode === null) child.kill('SIGKILL');
        }, 3000).unref();
      }),
  };
}

async function waitExit(portUpCheck: () => Promise<boolean>, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!(await portUpCheck())) return;
    await sleep(100);
  }
  throw new Error('等待进程退出超时');
}

function subsequence(types: string[], includes: string[]): boolean {
  let idx = 0;
  for (const t of types) {
    if (idx < includes.length && t === includes[idx]) idx++;
  }
  return idx === includes.length;
}

async function partA(dbPath: string): Promise<void> {
  console.log('\n[A] 崩溃注入:持久化后、响应前崩溃,重启后幂等重提');
  const server = await startServer(dbPath, { CRASH_AFTER_SUBMIT_COMMIT: '1' });
  const client = new DispatchClient({ baseUrl: BASE_URL, timeoutMs: 3000 });

  let crashed = false;
  try {
    await client.submit('biz-crash-1', 'calibrate', { axis: 'z' });
  } catch {
    crashed = true; // 连接被掐断:进程在提交后退出
  }
  check('首次提交后服务崩溃(响应未送达)', crashed);
  await waitExit(async () => {
    try {
      await client.health();
      return true;
    } catch {
      return false;
    }
  }, 5000).catch(() => server.stop());

  const restarted = await startServer(dbPath);
  const found = await client.getCommand('biz-crash-1');
  check('重启后指令存在(提交已持久化)', found.status === 200);
  const retry = await client.submit('biz-crash-1', 'calibrate', { axis: 'z' });
  check(
    '同幂等键重提被去重且为同一指令',
    retry.status === 200 && retry.body.deduped === true && retry.body.commandId === found.body.id,
  );
  const events = await client.getEvents(found.body.id);
  const accepted = events.body.events.filter((e: any) => e.type === 'COMMAND_ACCEPTED').length;
  check('COMMAND_ACCEPTED 仅一次(未生成第二条设备动作)', accepted === 1, `实际 ${accepted}`);
  await restarted.stop();
}

async function partB(dbPath: string): Promise<void> {
  console.log('\n[B] 驱动编译后的模拟器:争抢/接管/fencing/乱序与重复确认/重试耗尽');
  const server = await startServer(dbPath);
  const code = await new Promise<number>((resolve) => {
    const sim = spawn(process.execPath, [SIMULATOR_JS], {
      env: { ...process.env, BASE_URL, SIM_SLEEP_MS: '1200', SIM_TAKEOVER_SLEEP_MS: '3000', SIM_MAX_ATTEMPTS: '3' },
      stdio: 'inherit',
    });
    sim.once('exit', (c) => resolve(c ?? 1));
  });
  check('模拟器场景全部通过', code === 0, `exit=${code}`);
  await server.stop();
}

async function partC(dbPath: string): Promise<void> {
  console.log('\n[C] 再次重启:状态恢复与幂等保持');
  const server = await startServer(dbPath);
  const client = new DispatchClient({ baseUrl: BASE_URL, timeoutMs: 3000 });
  const cmd = await client.getCommand('cal-2026-0001');
  check('指令状态恢复为 SUCCEEDED', cmd.status === 200 && cmd.body.status === 'SUCCEEDED');
  const dup = await client.submit('cal-2026-0001', 'calibrate', { axis: 'x', target: 42 }, 'line-1');
  check('重启后重复提交仍去重', dup.status === 200 && dup.body.deduped === true && dup.body.commandId === cmd.body.id);
  const failed = await client.getCommand('switch-0007');
  check('FAILED 指令保持终态', failed.status === 200 && failed.body.status === 'FAILED');
  const own = await client.getOwnership('line-1');
  check('产线所有权跨重启保持(gw-2 gen=2)', own.status === 200 && own.body.ownerGatewayId === 'gw-2' && own.body.generation === 2);
  const events = await client.getEvents(cmd.body.id);
  const seqs = events.body.events.map((e: any) => e.seq);
  const monotonic = seqs.every((s: number, i: number) => i === 0 || s > seqs[i - 1]);
  check('事件因果序号单调递增', monotonic);
  await server.stop();
}

async function partD(dbPath: string): Promise<void> {
  console.log('\n[D] 双冗余网关:争抢 → 主领取 → 进程重启 → 备用接管 → 旧代际 fencing');
  let server = await startServer(dbPath);
  const client = new DispatchClient({ baseUrl: BASE_URL, timeoutMs: 3000 });
  const LINE = 'line-a';

  // ① 双网关争抢:主获得,备用被拒并看到当前代际
  const h1 = await client.heartbeat('gw-primary', LINE);
  check('主网关获得所有权 gen=1', h1.body.acquired === true && h1.body.generation === 1);
  const h2 = await client.heartbeat('gw-standby', LINE);
  check('备用网关争抢被拒并看到当前代际', h2.body.acquired === false && h2.body.owner === 'gw-primary' && h2.body.generation === 1);

  // ② 提交并由主网关(gen=1)领取
  await client.submit('cmd-dual-1', 'calibrate', { axis: 'x' }, LINE);
  const c1 = await client.claim('gw-primary', LINE, 1, 1);
  const task1 = c1.body?.tasks?.[0];
  check('主网关领到任务 attempt=1 gen=1', !!task1 && task1.attemptNo === 1 && task1.generation === 1);

  // ③ 调度进程重启(模拟分区期间调度服务本身也重启)
  await server.stop();
  server = await startServer(dbPath);
  const own1 = await client.getOwnership(LINE);
  check('重启后所有权与代际保持(fencing 状态不丢)', own1.body.ownerGatewayId === 'gw-primary' && own1.body.generation === 1);

  // ④ 主网关持续沉默(心跳中断)→ 所有权到期 → 备用接管
  await sleep(3000); // > OWNERSHIP_LEASE_TTL_MS(2500)
  const h3 = await client.heartbeat('gw-standby', LINE);
  check('备用网关接管 gen=2', h3.body.acquired === true && h3.body.generation === 2 && h3.body.tookOverFrom === 'gw-primary');

  // ⑤ 旧代际(网络分区恢复后的旧网关)一切操作被 fencing
  const fencedClaim = await client.claim('gw-primary', LINE, 1, 1);
  check('旧代际领取被拒(fenced)', fencedClaim.status === 403 && fencedClaim.body?.error?.code === 'fenced');
  const fencedAck = await client.ack('gw-primary', task1.leaseId, 1, 'dev-dual-late', { done: true });
  check('旧代际确认被忽略(fenced)', fencedAck.status === 200 && fencedAck.body.outcome === 'ignored' && String(fencedAck.body.reason).startsWith('fenced'));
  const mid = await client.getCommand('cmd-dual-1');
  check('旧代际确认未推进状态(仍 PENDING)', mid.body.status === 'PENDING', `实际 ${mid.body.status}`);

  // ⑥ 新代际重投:执行身份沿用首轮;当前代际确认后才成功
  const c2 = await client.claim('gw-standby', LINE, 2, 1);
  const task2 = c2.body?.tasks?.[0];
  check('备用重投 attempt=2 且 executionToken 沿用', !!task2 && task2.attemptNo === 2 && task2.executionToken === task1.executionToken);
  const dup = await client.ack('gw-primary', task1.leaseId, 1, 'dev-dual-late', { done: true });
  check('同一迟到确认再次送达:幂等重放(duplicate)', dup.body.duplicate === true && dup.body.outcome === 'ignored');
  const ok = await client.ack('gw-standby', task2.leaseId, 2, 'dev-dual-ok', { done: true });
  check('新代际确认生效 → SUCCEEDED', ok.body.outcome === 'applied' && ok.body.commandStatus === 'SUCCEEDED');

  // ⑦ 产线事件流还原接管前后的完整因果
  const lineEvents = await client.getLineEvents(LINE);
  const types = lineEvents.body.events.map((e: any) => e.type as string);
  const expected = [
    'OWNERSHIP_ACQUIRED',
    'OWNERSHIP_HEARTBEAT_REJECTED',
    'COMMAND_ACCEPTED',
    'DELIVERY_STARTED',
    'OWNERSHIP_TAKEN_OVER',
    'CLAIM_FENCED',
    'ACK_FENCED',
    'DEVICE_ACK_DUPLICATE',
    'DEVICE_ACK_APPLIED',
    'COMMAND_SUCCEEDED',
  ];
  check('产线事件流还原接管因果', subsequence(types, expected), types.join(','));
  await server.stop();
}

async function main(): Promise<void> {
  const dir = mkdtempSync(join(tmpdir(), 'dispatch-e2e-'));
  // A 段独立库(其残留指令不影响 B 段领取顺序);B/C 段共享同一库以验证重启恢复
  const dbA = join(dir, 'a.db');
  const dbB = join(dir, 'b.db');
  const dbD = join(dir, 'd.db');
  console.log(`[e2e] server=${SERVER_JS}`);
  try {
    await partA(dbA);
    await partB(dbB);
    await partC(dbB);
    await partD(dbD);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
  if (failures > 0) {
    console.error(`\n[e2e] 失败 ${failures} 项`);
    process.exit(1);
  }
  console.log('\n[e2e] 全部验收通过');
}

main().catch((err) => {
  console.error('[e2e] fatal:', err);
  process.exit(2);
});
