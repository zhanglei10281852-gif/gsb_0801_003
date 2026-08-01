/**
 * 场景脚本运行器:按 JSON 场景逐步驱动模拟网关/设备,并校验期望。
 *
 * 支持的操作(op):
 *  - log           { message }                                    打印说明
 *  - submit        { as, key, action, params, expectDeduped, expectSameCommandAs }
 *  - claim         { gateway, as, expectEmpty, expectCommandKey, expectAttemptNo, expectExecutionTokenOf }
 *  - renew         { gateway(默认取投递时的网关), delivery, expect: 'ok'|'rejected' }
 *  - ack           { delivery, ackId, result, expect: 'applied'|'ignored'|'duplicate'|'rejected' }
 *  - sleep         { ms }
 *  - assertStatus  { key|command, status }
 *  - assertEvents  { key|command, includes: [...按顺序包含...], countOf: {TYPE: n} }
 *  - waitStatus    { key|command, status, timeoutMs }             轮询直到状态出现
 *
 * 断网复现方式:领取后不再 renew/ack(或 sleep 超过租约),服务侧租约到期即视同失联。
 * 重复提交:两次 submit 使用相同 key。乱序/重复确认:任意顺序多次 ack。
 */
import { DispatchClient } from './client';

export interface Step {
  op: string;
  [k: string]: unknown;
}

export interface ScenarioResult {
  passed: number;
  failed: number;
  failures: string[];
}

interface Submission {
  commandId: string;
  key: string;
}

interface Delivery {
  gateway: string;
  commandId: string;
  executionToken: string;
  attemptNo: number;
  leaseId: string;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function runScenario(
  client: DispatchClient,
  steps: Step[],
  opts: { verbose?: boolean } = {},
): Promise<ScenarioResult> {
  const submissions = new Map<string, Submission>();
  const deliveries = new Map<string, Delivery>();
  const result: ScenarioResult = { passed: 0, failed: 0, failures: [] };

  const log = (msg: string) => {
    if (opts.verbose !== false) console.log(msg);
  };
  const ok = (msg: string) => {
    result.passed++;
    log(`  ✔ ${msg}`);
  };
  const fail = (msg: string) => {
    result.failed++;
    result.failures.push(msg);
    log(`  ✘ ${msg}`);
  };
  // 服务端 getCommand 同时兼容指令 id 与幂等键
  const resolveCommandId = async (step: Step): Promise<string | null> => {
    const idOrKey = String(step.command ?? step.key ?? '');
    if (!idOrKey) return null;
    const r = await client.getCommand(idOrKey).catch(() => null);
    return r && r.status === 200 ? r.body.id : null;
  };

  for (let i = 0; i < steps.length; i++) {
    const step = steps[i];
    const label = `step ${i + 1} [${step.op}]`;
    log(`${label}`);

    try {
      switch (step.op) {
        case 'log': {
          log(`  ${String(step.message ?? '')}`);
          break;
        }

        case 'submit': {
          const r = await client.submit(String(step.key), String(step.action), step.params ?? null);
          if (r.status !== 200 && r.status !== 201) {
            fail(`submit ${step.key}: HTTP ${r.status} ${JSON.stringify(r.body)}`);
            break;
          }
          const sub: Submission = { commandId: r.body.commandId, key: String(step.key) };
          submissions.set(String(step.as ?? step.key), sub);
          if (step.expectDeduped !== undefined && r.body.deduped !== step.expectDeduped) {
            fail(`submit ${step.key}: deduped=${r.body.deduped},期望 ${step.expectDeduped}`);
            break;
          }
          if (step.expectSameCommandAs) {
            const prev = submissions.get(String(step.expectSameCommandAs));
            if (!prev || prev.commandId !== sub.commandId) {
              fail(`submit ${step.key}: 与 ${String(step.expectSameCommandAs)} 不是同一指令`);
              break;
            }
          }
          ok(`submit ${step.key} → ${sub.commandId} (deduped=${r.body.deduped})`);
          break;
        }

        case 'claim': {
          const gateway = String(step.gateway);
          const r = await client.claim(gateway, 1);
          if (r.status !== 200) {
            fail(`claim: HTTP ${r.status}`);
            break;
          }
          const tasks = r.body.tasks as any[];
          if (step.expectEmpty) {
            if (tasks.length === 0) ok('claim 返回空(无可领取任务)');
            else fail(`claim 期望为空,实际拿到 ${tasks.length} 个任务`);
            break;
          }
          if (tasks.length === 0) {
            fail('claim 期望拿到任务,实际为空');
            break;
          }
          const t = tasks[0];
          const d: Delivery = {
            gateway,
            commandId: t.commandId,
            executionToken: t.executionToken,
            attemptNo: t.attemptNo,
            leaseId: t.leaseId,
          };
          deliveries.set(String(step.as), d);
          if (step.expectCommandKey) {
            const sub = submissions.get(String(step.expectCommandKey));
            if (!sub || sub.commandId !== t.commandId) {
              fail(`claim: 拿到的是 ${t.commandId},期望 ${String(step.expectCommandKey)} 的指令`);
              break;
            }
          }
          if (step.expectAttemptNo !== undefined && t.attemptNo !== step.expectAttemptNo) {
            fail(`claim: attemptNo=${t.attemptNo},期望 ${step.expectAttemptNo}`);
            break;
          }
          if (step.expectExecutionTokenOf) {
            const prev = deliveries.get(String(step.expectExecutionTokenOf));
            if (!prev || prev.executionToken !== t.executionToken) {
              fail(`claim: executionToken 未沿用 ${String(step.expectExecutionTokenOf)} 的执行身份`);
              break;
            }
          }
          ok(`claim → cmd=${t.commandId} attempt=${t.attemptNo} token=${t.executionToken}`);
          break;
        }

        case 'renew': {
          const d = deliveries.get(String(step.delivery));
          if (!d) {
            fail(`renew: 未知投递 ${String(step.delivery)}`);
            break;
          }
          const gateway = String(step.gateway ?? d.gateway);
          const r = await client.renew(gateway, d.leaseId);
          const expect = step.expect ?? 'ok';
          if (expect === 'ok' && r.status === 200 && r.body.ok) ok(`renew 成功,租约延至 ${r.body.leaseExpiresAt}`);
          else if (expect === 'rejected' && (r.status === 409 || r.body.ok === false)) ok(`renew 被拒(${r.body.reason ?? r.status})`);
          else fail(`renew: 期望 ${expect},实际 HTTP ${r.status} ${JSON.stringify(r.body)}`);
          break;
        }

        case 'ack': {
          const d = deliveries.get(String(step.delivery));
          if (!d) {
            fail(`ack: 未知投递 ${String(step.delivery)}`);
            break;
          }
          const r = await client.ack(d.gateway, d.leaseId, String(step.ackId), step.result ?? { device: 'ok' });
          const expect = String(step.expect ?? 'applied');
          if (r.status !== 200) {
            if (expect === 'rejected') ok(`ack 被拒(HTTP ${r.status})`);
            else fail(`ack: HTTP ${r.status} ${JSON.stringify(r.body)}`);
            break;
          }
          const actual = r.body.duplicate ? 'duplicate' : r.body.outcome;
          if (actual === expect) {
            ok(`ack ${step.ackId} → ${actual}${r.body.reason ? ` (${r.body.reason})` : ''}`);
          } else {
            fail(`ack ${step.ackId}: 期望 ${expect},实际 ${actual} ${JSON.stringify(r.body)}`);
          }
          break;
        }

        case 'sleep': {
          await sleep(Number(step.ms));
          log(`  等待 ${Number(step.ms)}ms`);
          break;
        }

        case 'waitStatus': {
          const id = await resolveCommandId(step);
          if (!id) {
            fail('waitStatus: 找不到指令');
            break;
          }
          const deadline = Date.now() + Number(step.timeoutMs ?? 5000);
          let current = '';
          while (Date.now() < deadline) {
            const r = await client.getCommand(id);
            if (r.status === 200) {
              current = r.body.status;
              if (current === step.status) break;
            }
            await sleep(50);
          }
          if (current === step.status) ok(`状态达到 ${String(step.status)}`);
          else fail(`waitStatus: 期望 ${String(step.status)},实际 ${current}`);
          break;
        }

        case 'assertStatus': {
          const id = await resolveCommandId(step);
          if (!id) {
            fail('assertStatus: 找不到指令');
            break;
          }
          const r = await client.getCommand(id);
          if (r.status === 200 && r.body.status === step.status) ok(`状态为 ${String(step.status)}`);
          else fail(`assertStatus: 期望 ${String(step.status)},实际 ${r.status === 200 ? r.body.status : `HTTP ${r.status}`}`);
          break;
        }

        case 'assertEvents': {
          const id = await resolveCommandId(step);
          if (!id) {
            fail('assertEvents: 找不到指令');
            break;
          }
          const r = await client.getEvents(id);
          if (r.status !== 200) {
            fail(`assertEvents: HTTP ${r.status}`);
            break;
          }
          const types = (r.body.events as any[]).map((e) => e.type as string);
          const includes = (step.includes as string[] | undefined) ?? [];
          let idx = 0;
          for (const t of types) {
            if (idx < includes.length && t === includes[idx]) idx++;
          }
          if (idx !== includes.length) {
            fail(`assertEvents: 事件序列 ${types.join(',')} 未按序包含 ${includes.join(',')}`);
            break;
          }
          const countOf = (step.countOf as Record<string, number> | undefined) ?? {};
          for (const [t, n] of Object.entries(countOf)) {
            const c = types.filter((x) => x === t).length;
            if (c !== n) {
              fail(`assertEvents: ${t} 出现 ${c} 次,期望 ${n} 次`);
              idx = -1;
              break;
            }
          }
          if (idx === includes.length) ok(`事件因果链符合预期(${types.join(' → ')})`);
          break;
        }

        default:
          fail(`未知 op: ${step.op}`);
      }
    } catch (err) {
      fail(`${label} 异常: ${String(err)}`);
    }
  }

  log(`\n结果: ${result.passed} 通过, ${result.failed} 失败`);
  return result;
}
