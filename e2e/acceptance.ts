/**
 * 端到端验收:真实启动【编译后】的服务进程(dist/src/server.js),
 * 并驱动【编译后】的模拟器(dist/simulator/cli.js)复现故障场景。
 *
 * 运行方式:npm run e2e(会先执行 npm run build)
 *
 * 验收内容:
 *  A. 崩溃注入:服务在"提交已落库、响应未发出"时崩溃 → 重启后同幂等键
 *     重提仍只有一条指令、只有一个 COMMAND_ACCEPTED。
 *  B. 子进程方式运行模拟器默认场景:断网失联、跨网关重派、迟到/重复/
 *     乱序确认、重试耗尽,全部断言通过。
 *  C. 再次重启服务:状态完整恢复,重复提交仍被去重。
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
  console.log('\n[B] 驱动编译后的模拟器复现:断网/重复提交/乱序与重复确认/重试耗尽');
  const server = await startServer(dbPath);
  const code = await new Promise<number>((resolve) => {
    const sim = spawn(process.execPath, [SIMULATOR_JS], {
      env: { ...process.env, BASE_URL, SIM_SLEEP_MS: '1200', SIM_MAX_ATTEMPTS: '3' },
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
  const dup = await client.submit('cal-2026-0001', 'calibrate', { axis: 'x', target: 42 });
  check('重启后重复提交仍去重', dup.status === 200 && dup.body.deduped === true && dup.body.commandId === cmd.body.id);
  const failed = await client.getCommand('switch-0007');
  check('FAILED 指令保持终态', failed.status === 200 && failed.body.status === 'FAILED');
  const events = await client.getEvents(cmd.body.id);
  const seqs = events.body.events.map((e: any) => e.seq);
  const monotonic = seqs.every((s: number, i: number) => i === 0 || s > seqs[i - 1]);
  check('事件因果序号单调递增', monotonic);
  await server.stop();
}

async function main(): Promise<void> {
  const dir = mkdtempSync(join(tmpdir(), 'dispatch-e2e-'));
  // A 段独立库(其残留指令不影响 B 段领取顺序);B/C 段共享同一库以验证重启恢复
  const dbA = join(dir, 'a.db');
  const dbB = join(dir, 'b.db');
  console.log(`[e2e] server=${SERVER_JS}`);
  try {
    await partA(dbA);
    await partB(dbB);
    await partC(dbB);
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
