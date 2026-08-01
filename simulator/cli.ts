/**
 * 模拟器 CLI:
 *   npm run simulate -- scenarios/offline-redelivery.json   # 运行指定场景
 *   npm run simulate                                        # 运行内建默认场景
 *
 * 环境变量:
 *   BASE_URL  调度服务地址(默认 http://127.0.0.1:8080)
 *
 * 内建默认场景一次性复现:重复提交、网关领走后失联、跨网关重派并沿用
 * 稳定执行身份、迟到确认、重复确认、乱序确认、重试耗尽后迟到确认。
 */
import { readFileSync } from 'node:fs';
import { DispatchClient } from './client';
import { Step, runScenario } from './scenario';

export interface DefaultScenarioOpts {
  /** 应大于服务端 LEASE_TTL_MS,用于复现"网关失联→租约过期" */
  sleepMs: number;
  /** 应等于服务端 MAX_ATTEMPTS,用于复现"重试耗尽→FAILED" */
  maxAttempts: number;
}

export function buildDefaultScenario(opts: DefaultScenarioOpts): Step[] {
  const { sleepMs, maxAttempts } = opts;
  const exhaustSteps: Step[] = [];
  for (let i = 1; i <= maxAttempts; i++) {
    exhaustSteps.push({
      op: 'claim',
      gateway: 'gw-3',
      as: `dx${i}`,
      expectAttemptNo: i,
      ...(i > 1 ? { expectExecutionTokenOf: 'dx1' } : {}),
    });
    exhaustSteps.push({ op: 'sleep', ms: sleepMs });
  }

  return [
    { op: 'log', message: '① 上游重复提交(模拟"持久化后响应前崩溃"导致的客户端重试)' },
    { op: 'submit', as: 's1', key: 'cal-2026-0001', action: 'calibrate', params: { axis: 'x', target: 42 } },
    { op: 'submit', as: 's2', key: 'cal-2026-0001', action: 'calibrate', params: { axis: 'x', target: 42 }, expectDeduped: true, expectSameCommandAs: 's1' },

    { op: 'log', message: '② 网关 gw-1 领取任务后失联(不续约、不确认)' },
    { op: 'claim', gateway: 'gw-1', as: 'd1', expectCommandKey: 's1', expectAttemptNo: 1 },
    { op: 'sleep', ms: sleepMs },

    { op: 'log', message: '③ 租约过期,网关 gw-2 重新领取:执行身份必须沿用,attempt 递增' },
    { op: 'claim', gateway: 'gw-2', as: 'd2', expectCommandKey: 's1', expectAttemptNo: 2, expectExecutionTokenOf: 'd1' },

    { op: 'log', message: '④ 失联的 gw-1 迟到确认旧租约:必须被忽略,不能复活/抢功' },
    { op: 'ack', delivery: 'd1', ackId: 'dev-ack-1', expect: 'ignored' },

    { op: 'log', message: '⑤ gw-2 回传设备确认:生效;重复确认:幂等去重' },
    { op: 'ack', delivery: 'd2', ackId: 'dev-ack-2', expect: 'applied' },
    { op: 'ack', delivery: 'd2', ackId: 'dev-ack-2', expect: 'duplicate' },

    { op: 'log', message: '⑥ 乱序:gw-1 在指令成功后又送来旧确认,仍然只能被忽略' },
    { op: 'ack', delivery: 'd1', ackId: 'dev-ack-3-late', expect: 'ignored' },

    { op: 'assertStatus', key: 'cal-2026-0001', status: 'SUCCEEDED' },
    {
      op: 'assertEvents',
      key: 'cal-2026-0001',
      includes: [
        'COMMAND_ACCEPTED',
        'SUBMISSION_DEDUPED',
        'DELIVERY_STARTED',
        'LEASE_EXPIRED',
        'RETRY_SCHEDULED',
        'DELIVERY_STARTED',
        'DEVICE_ACK_IGNORED',
        'DEVICE_ACK_APPLIED',
        'COMMAND_SUCCEEDED',
        'DEVICE_ACK_DUPLICATE',
        'DEVICE_ACK_IGNORED',
      ],
      countOf: { COMMAND_ACCEPTED: 1, COMMAND_SUCCEEDED: 1, DEVICE_ACK_APPLIED: 1 },
    },

    { op: 'log', message: `⑦ 另一条指令:网关反复失联,${maxAttempts} 次尝试耗尽进入终态 FAILED` },
    { op: 'submit', as: 's3', key: 'switch-0007', action: 'switch_recipe', params: { recipe: 'B' } },
    ...exhaustSteps,
    { op: 'waitStatus', key: 'switch-0007', status: 'FAILED', timeoutMs: 8000 },

    { op: 'log', message: '⑧ 终态后的迟到确认:持久化但忽略,指令绝不能复活' },
    { op: 'ack', delivery: `dx${maxAttempts}`, ackId: 'dev-ack-too-late', expect: 'ignored' },
    { op: 'assertStatus', key: 'switch-0007', status: 'FAILED' },
  ];
}

/** 与 e2e 验收使用的服务参数一致;对自建服务运行时可用 SIM_* 覆盖 */
export const defaultScenario: Step[] = buildDefaultScenario({
  sleepMs: Number(process.env.SIM_SLEEP_MS ?? 1200),
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

/* istanbul ignore next -- 入口守卫:被 e2e import 时不执行 */
if (require.main === module) {
  main().catch((err) => {
    console.error('[simulator] fatal:', err);
    process.exit(2);
  });
}
