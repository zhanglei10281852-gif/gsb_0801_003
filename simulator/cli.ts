/**
 * 模拟器 CLI:
 *   npm run simulate -- scenarios/offline-redelivery.json   # 运行指定场景
 *   npm run simulate                                        # 运行内建默认场景
 *
 * 环境变量:
 *   BASE_URL                调度服务地址(默认 http://127.0.0.1:8080)
 *   SIM_SLEEP_MS            普通等待(应大于服务端 LEASE_TTL_MS,默认 1200)
 *   SIM_TAKEOVER_SLEEP_MS   接管等待(应大于服务端 OWNERSHIP_LEASE_TTL_MS,默认 3000)
 *   SIM_MAX_ATTEMPTS        应等于服务端 MAX_ATTEMPTS(默认 3)
 *
 * 内建默认场景一次性复现:重复提交、双网关争抢、主网关失联后备用接管、
 * 旧代际被 fencing(领取与迟到确认都被拒)、跨代际重投沿用稳定执行身份、
 * 重复确认、重试耗尽、终态后迟到确认。
 */
import { readFileSync } from 'node:fs';
import { DispatchClient } from './client';
import { Step, runScenario } from './scenario';

export interface DefaultScenarioOpts {
  /** 应大于服务端 LEASE_TTL_MS,用于复现"网关失联→投递租约过期" */
  sleepMs: number;
  /** 应大于服务端 OWNERSHIP_LEASE_TTL_MS,用于复现"主网关心跳中断→备用接管" */
  takeoverSleepMs: number;
  /** 应等于服务端 MAX_ATTEMPTS,用于复现"重试耗尽→FAILED" */
  maxAttempts: number;
}

export function buildDefaultScenario(opts: DefaultScenarioOpts): Step[] {
  const { sleepMs, takeoverSleepMs, maxAttempts } = opts;
  const exhaustSteps: Step[] = [];
  for (let i = 1; i <= maxAttempts; i++) {
    exhaustSteps.push({ op: 'heartbeat', gateway: 'gw-3', line: 'line-2', expect: 'acquired' });
    exhaustSteps.push({
      op: 'claim',
      gateway: 'gw-3',
      line: 'line-2',
      as: `dx${i}`,
      expectAttemptNo: i,
      ...(i > 1 ? { expectExecutionTokenOf: 'dx1' } : {}),
    });
    exhaustSteps.push({ op: 'sleep', ms: sleepMs });
  }

  return [
    { op: 'log', message: '① 上游重复提交(模拟"持久化后响应前崩溃"导致的客户端重试)' },
    { op: 'submit', as: 's1', key: 'cal-2026-0001', action: 'calibrate', params: { axis: 'x', target: 42 }, line: 'line-1' },
    { op: 'submit', as: 's2', key: 'cal-2026-0001', action: 'calibrate', params: { axis: 'x', target: 42 }, line: 'line-1', expectDeduped: true, expectSameCommandAs: 's1' },

    { op: 'log', message: '② 双网关争抢:gw-1 获得 line-1 所有权,备用 gw-2 争抢被拒' },
    { op: 'heartbeat', gateway: 'gw-1', line: 'line-1', expect: 'acquired', expectGeneration: 1 },
    { op: 'heartbeat', gateway: 'gw-2', line: 'line-1', expect: 'rejected', expectGeneration: 1 },

    { op: 'log', message: '③ gw-1 领取任务后失联(不心跳、不续约、不确认),备用等待接管' },
    { op: 'claim', gateway: 'gw-1', line: 'line-1', as: 'd1', expectCommandKey: 's1', expectAttemptNo: 1, expectGeneration: 1 },
    { op: 'sleep', ms: takeoverSleepMs },

    { op: 'log', message: '④ 所有权租约到期,gw-2 接管(代际 +1);旧代际领取被 fencing' },
    { op: 'heartbeat', gateway: 'gw-2', line: 'line-1', expect: 'acquired', expectGeneration: 2 },
    { op: 'claim', gateway: 'gw-1', line: 'line-1', generation: 1, expect: 'fenced' },
    { op: 'claim', gateway: 'gw-2', line: 'line-1', as: 'd2', expectCommandKey: 's1', expectAttemptNo: 2, expectGeneration: 2, expectExecutionTokenOf: 'd1' },

    { op: 'log', message: '⑤ 网络分区恢复,失联的 gw-1 用旧代际回传确认:必须被 fencing,不能推进状态' },
    { op: 'ack', delivery: 'd1', generation: 1, ackId: 'dev-ack-1', expect: 'fenced' },
    { op: 'assertStatus', key: 'cal-2026-0001', status: 'DISPATCHED' },

    { op: 'log', message: '⑥ gw-2(当前代际)回传设备确认:生效;重复确认:幂等去重' },
    { op: 'ack', delivery: 'd2', ackId: 'dev-ack-2', expect: 'applied' },
    { op: 'ack', delivery: 'd2', ackId: 'dev-ack-2', expect: 'duplicate' },

    { op: 'log', message: '⑦ gw-1 在指令成功后又送来旧代际确认,仍然只能被 fencing' },
    { op: 'ack', delivery: 'd1', generation: 1, ackId: 'dev-ack-3-late', expect: 'fenced' },

    { op: 'assertStatus', key: 'cal-2026-0001', status: 'SUCCEEDED' },
    { op: 'assertOwnership', line: 'line-1', owner: 'gw-2', generation: 2 },
    {
      op: 'assertEvents',
      key: 'cal-2026-0001',
      includes: [
        'COMMAND_ACCEPTED',
        'SUBMISSION_DEDUPED',
        'DELIVERY_STARTED',
        'DELIVERY_STARTED',
        'ACK_FENCED',
        'DEVICE_ACK_APPLIED',
        'COMMAND_SUCCEEDED',
        'DEVICE_ACK_DUPLICATE',
        'ACK_FENCED',
      ],
      countOf: { COMMAND_ACCEPTED: 1, COMMAND_SUCCEEDED: 1, DEVICE_ACK_APPLIED: 1, ACK_FENCED: 2 },
    },
    {
      op: 'assertLineEvents',
      line: 'line-1',
      includes: [
        'OWNERSHIP_ACQUIRED',
        'OWNERSHIP_HEARTBEAT_REJECTED',
        'OWNERSHIP_TAKEN_OVER',
        'CLAIM_FENCED',
        'ACK_FENCED',
        'COMMAND_SUCCEEDED',
      ],
      countOf: { OWNERSHIP_TAKEN_OVER: 1, CLAIM_FENCED: 1 },
    },

    { op: 'log', message: `⑧ 另一条产线:网关反复失联,${maxAttempts} 次尝试耗尽进入终态 FAILED` },
    { op: 'submit', as: 's3', key: 'switch-0007', action: 'switch_recipe', params: { recipe: 'B' }, line: 'line-2' },
    ...exhaustSteps,
    { op: 'waitStatus', key: 'switch-0007', status: 'FAILED', timeoutMs: 8000 },

    { op: 'log', message: '⑨ 终态后的迟到确认:持久化但忽略,指令绝不能复活' },
    { op: 'ack', delivery: `dx${maxAttempts}`, ackId: 'dev-ack-too-late', expect: 'ignored' },
    { op: 'assertStatus', key: 'switch-0007', status: 'FAILED' },

    { op: 'log', message: '⑩ 紧急撤回:未完成的动作可撤回;已成功的动作绝不能伪装成撤回成功' },
    { op: 'heartbeat', gateway: 'gw-4', line: 'line-3', expect: 'acquired', expectGeneration: 1 },
    { op: 'submit', as: 'sc1', key: 'cancel-1', action: 'switch_recipe', params: { recipe: 'C' }, line: 'line-3' },
    { op: 'cancel', key: 'cancel-1', reason: '工艺临时变更', expect: 'cancelled' },
    { op: 'cancel', key: 'cancel-1', expect: 'deduped' },
    { op: 'claim', gateway: 'gw-4', line: 'line-3', expect: 'empty' },
    { op: 'cancel', key: 'cal-2026-0001', expect: 'rejected' },
    { op: 'assertStatus', key: 'cancel-1', status: 'CANCELLED' },
    { op: 'assertStatus', key: 'cal-2026-0001', status: 'SUCCEEDED' },
    {
      op: 'assertEvents',
      key: 'cancel-1',
      includes: ['COMMAND_ACCEPTED', 'COMMAND_CANCELLED', 'CANCEL_DEDUPED'],
      countOf: { COMMAND_CANCELLED: 1 },
    },

    { op: 'log', message: '⑪ 替代指令:旧指令原子退役,在途迟到确认留痕但不生效;已成功的不能被取代' },
    { op: 'submit', as: 'sOld', key: 'old-300', action: 'calibrate', params: { axis: 'z', target: 10 }, line: 'line-3' },
    { op: 'claim', gateway: 'gw-4', line: 'line-3', as: 'dOld', expectCommandKey: 'sOld', expectAttemptNo: 1 },
    { op: 'submit', as: 'sNew', key: 'new-300', action: 'calibrate', params: { axis: 'z', target: 20 }, line: 'line-3', supersedes: 'old-300', expect: 'accepted' },
    { op: 'assertStatus', key: 'old-300', status: 'SUPERSEDED' },
    { op: 'ack', delivery: 'dOld', ackId: 'dev-old-300', expect: 'ignored' },
    { op: 'claim', gateway: 'gw-4', line: 'line-3', as: 'dNew', expectCommandKey: 'sNew', expectAttemptNo: 1 },
    { op: 'ack', delivery: 'dNew', ackId: 'dev-new-300', expect: 'applied' },
    { op: 'submit', key: 'newer-300', action: 'calibrate', params: { axis: 'z', target: 30 }, line: 'line-3', supersedes: 'new-300', expect: 'rejected' },
    { op: 'assertStatus', key: 'new-300', status: 'SUCCEEDED' },
    {
      op: 'assertEvents',
      key: 'old-300',
      includes: ['COMMAND_ACCEPTED', 'DELIVERY_STARTED', 'ATTEMPT_ABORTED', 'COMMAND_SUPERSEDED', 'DEVICE_ACK_IGNORED'],
      countOf: { COMMAND_SUPERSEDED: 1, DEVICE_ACK_IGNORED: 1 },
    },
    {
      op: 'assertLineEvents',
      line: 'line-3',
      includes: ['COMMAND_SUPERSEDED', 'DEVICE_ACK_IGNORED', 'DEVICE_ACK_APPLIED', 'COMMAND_SUCCEEDED'],
    },

    { op: 'log', message: '⑫ 失联旧网关重连:沿用上一轮代际的操作依旧全部被 fencing' },
    { op: 'claim', gateway: 'gw-1', line: 'line-1', generation: 1, expect: 'fenced' },
    { op: 'ack', delivery: 'd1', generation: 1, ackId: 'dev-ack-reconnect', expect: 'fenced' },
    { op: 'assertStatus', key: 'cal-2026-0001', status: 'SUCCEEDED' },
  ];
}

/** 与 e2e 验收使用的服务参数一致;对自建服务运行时可用 SIM_* 覆盖 */
export const defaultScenario: Step[] = buildDefaultScenario({
  sleepMs: Number(process.env.SIM_SLEEP_MS ?? 1200),
  takeoverSleepMs: Number(process.env.SIM_TAKEOVER_SLEEP_MS ?? 3000),
  maxAttempts: Number(process.env.SIM_MAX_ATTEMPTS ?? 3),
});

async function main(): Promise<void> {
  const baseUrl = process.env.BASE_URL ?? 'http://127.0.0.1:8080';
  const scenarioPath = process.argv[2];
  const steps: Step[] = scenarioPath
    ? JSON.parse(readFileSync(scenarioPath, 'utf8')).steps
    : defaultScenario;

  console.log(`[simulator] target=${baseUrl} scenario=${scenarioPath ?? '<内建默认场景>'}`);
  const client = new DispatchClient({ baseUrl, timeoutMs: 5000 });
  const result = await runScenario(client, steps);
  process.exit(result.failed === 0 ? 0 : 1);
}

/* 入口守卫:被 e2e import 时不执行 */
if (require.main === module) {
  main().catch((err) => {
    console.error('[simulator] fatal:', err);
    process.exit(2);
  });
}
