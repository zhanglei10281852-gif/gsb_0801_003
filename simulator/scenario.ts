/**
 * 场景脚本运行器:按 JSON 场景逐步驱动模拟网关/设备,并校验期望。
 *
 * 支持的操作(op):
 *  - log             { message }
 *  - submit          { as, key, action, params, line, supersedes?,
 *                      expect(带 supersedes 时): 'accepted'|'rejected'|'deduped',
 *                      expectDeduped, expectSameCommandAs }
 *  - cancel          { key|command, reason?, expect: 'cancelled'|'deduped'|'rejected' }
 *  - heartbeat       { gateway, line, expect: 'acquired'|'rejected', expectGeneration }
 *  - claim           { gateway, line, as, generation?, expect: 'tasks'|'empty'|'fenced',
 *                      expectCommandKey, expectAttemptNo, expectExecutionTokenOf, expectGeneration }
 *  - renew           { delivery, gateway?, generation?, expect: 'ok'|'rejected'|'fenced' }
 *  - ack             { delivery, generation?, ackId, result,
 *                      expect: 'applied'|'ignored'|'duplicate'|'rejected'|'fenced' }
 *  - sleep           { ms }
 *  - assertStatus    { key|command, status }
 *  - assertEvents    { key|command, includes: [...按顺序包含...], countOf: {TYPE: n} }
 *  - assertLineEvents{ line, includes: [...], countOf: {TYPE: n} }   产线维度因果断言
 *  - assertOwnership { line, owner, generation }
 *  - waitStatus      { key|command, status, timeoutMs }
 *
 * generation 缺省时使用该网关最近一次心跳获得的代际;显式给一个旧数字即可
 * 复现"网络分区期间旧网关仍在发送"。
 * 断网复现:领取后不再 heartbeat/renew/ack(或 sleep 超过租约)。
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
  lineId: string;
  commandId: string;
  executionToken: string;
  attemptNo: number;
  generation: number;
  leaseId: string;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** 检查 includes 是否为 types 的保序子序列 */
function isSubsequence(types: string[], includes: string[]): boolean {
  let idx = 0;
  for (const t of types) {
    if (idx < includes.length && t === includes[idx]) idx++;
  }
  return idx === includes.length;
}

export async function runScenario(
  client: DispatchClient,
  steps: Step[],
  opts: { verbose?: boolean } = {},
): Promise<ScenarioResult> {
  const submissions = new Map<string, Submission>();
  const deliveries = new Map<string, Delivery>();
  /** 每个网关最近一次心跳获得的代际 */
  const generations = new Map<string, number>();
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
  const genOf = (step: Step, gateway: string): number =>
    step.generation !== undefined ? Number(step.generation) : generations.get(gateway) ?? -1;

  const assertEventView = async (
    label: string,
    fetchTypes: () => Promise<string[] | null>,
    includes: string[],
    countOf: Record<string, number>,
  ): Promise<void> => {
    const types = await fetchTypes();
    if (!types) {
      fail(`${label}: 无法获取事件`);
      return;
    }
    if (!isSubsequence(types, includes)) {
      fail(`${label}: 事件序列 ${types.join(',')} 未按序包含 ${includes.join(',')}`);
      return;
    }
    for (const [t, n] of Object.entries(countOf)) {
      const c = types.filter((x) => x === t).length;
      if (c !== n) {
        fail(`${label}: ${t} 出现 ${c} 次,期望 ${n} 次`);
        return;
      }
    }
    ok(`${label} 因果链符合预期(${types.join(' → ')})`);
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
          const r = await client.submit(
            String(step.key),
            String(step.action),
            step.params ?? null,
            String(step.line ?? 'default'),
            step.supersedes ? String(step.supersedes) : undefined,
          );
          // 替代指令提交的期望语义
          if (step.supersedes) {
            const expect = String(step.expect ?? 'accepted');
            if (expect === 'rejected') {
              if (r.status === 409 && r.body?.error?.code === 'supersede_rejected') {
                ok(`supersede ${step.key} 被拒(${r.body.error.message})`);
              } else {
                fail(`supersede ${step.key}: 期望 rejected,实际 HTTP ${r.status} ${JSON.stringify(r.body)}`);
              }
              break;
            }
            if (r.status !== 200 && r.status !== 201) {
              fail(`supersede ${step.key}: HTTP ${r.status} ${JSON.stringify(r.body)}`);
              break;
            }
            if (expect === 'deduped' && r.body.deduped !== true) {
              fail(`supersede ${step.key}: 期望 deduped,实际 ${JSON.stringify(r.body)}`);
              break;
            }
            submissions.set(String(step.as ?? step.key), { commandId: r.body.commandId, key: String(step.key) });
            ok(`supersede ${step.key} → ${r.body.commandId} 取代 ${r.body.supersededCommandId} (deduped=${r.body.deduped})`);
            break;
          }
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
          ok(`submit ${step.key} → ${sub.commandId} (line=${r.body.lineId}, deduped=${r.body.deduped})`);
          break;
        }

        case 'cancel': {
          const idOrKey = String(step.command ?? step.key ?? '');
          const r = await client.cancel(idOrKey, step.reason ? String(step.reason) : undefined);
          const expect = String(step.expect ?? 'cancelled');
          if (expect === 'rejected') {
            if (r.status === 409 && r.body?.error?.code === 'cancel_rejected') {
              ok(`cancel ${idOrKey} 被拒(${r.body.error.message})`);
            } else {
              fail(`cancel ${idOrKey}: 期望 rejected,实际 HTTP ${r.status} ${JSON.stringify(r.body)}`);
            }
            break;
          }
          if (r.status !== 200) {
            fail(`cancel ${idOrKey}: HTTP ${r.status} ${JSON.stringify(r.body)}`);
            break;
          }
          if (expect === 'deduped' && r.body.deduped !== true) {
            fail(`cancel ${idOrKey}: 期望 deduped,实际 ${JSON.stringify(r.body)}`);
            break;
          }
          ok(`cancel ${idOrKey} → cancelled (deduped=${r.body.deduped})`);
          break;
        }

        case 'heartbeat': {
          const gateway = String(step.gateway);
          const line = String(step.line ?? 'line-1');
          const r = await client.heartbeat(gateway, line);
          if (r.status !== 200) {
            fail(`heartbeat: HTTP ${r.status}`);
            break;
          }
          const expect = String(step.expect ?? 'acquired');
          if (expect === 'acquired' && !r.body.acquired) {
            fail(`heartbeat: 期望获得所有权,实际被拒(当前属主 ${r.body.owner} gen=${r.body.generation})`);
            break;
          }
          if (expect === 'rejected' && r.body.acquired) {
            fail(`heartbeat: 期望被拒,实际获得 gen=${r.body.generation}`);
            break;
          }
          if (step.expectGeneration !== undefined && r.body.generation !== Number(step.expectGeneration)) {
            fail(`heartbeat: 代际=${r.body.generation},期望 ${step.expectGeneration}`);
            break;
          }
          if (r.body.acquired) generations.set(gateway, r.body.generation);
          ok(`heartbeat ${gateway}@${line} → ${r.body.acquired ? `属主 gen=${r.body.generation}` : `被拒(属主 ${r.body.owner} gen=${r.body.generation})`}`);
          break;
        }

        case 'claim': {
          const gateway = String(step.gateway);
          const line = String(step.line ?? 'line-1');
          const generation = genOf(step, gateway);
          const r = await client.claim(gateway, line, generation, Number(step.limit ?? 1));
          const expect = step.expect !== undefined ? String(step.expect) : step.expectEmpty ? 'empty' : 'tasks';
          if (expect === 'fenced') {
            if (r.status === 403 && r.body?.error?.code === 'fenced') ok(`claim 被 fencing(${r.body.fenced})`);
            else fail(`claim: 期望 fenced,实际 HTTP ${r.status} ${JSON.stringify(r.body)}`);
            break;
          }
          if (r.status !== 200) {
            fail(`claim: HTTP ${r.status} ${JSON.stringify(r.body)}`);
            break;
          }
          const tasks = r.body.tasks as any[];
          if (expect === 'empty') {
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
            lineId: t.lineId,
            commandId: t.commandId,
            executionToken: t.executionToken,
            attemptNo: t.attemptNo,
            generation: t.generation,
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
          if (step.expectAttemptNo !== undefined && t.attemptNo !== Number(step.expectAttemptNo)) {
            fail(`claim: attemptNo=${t.attemptNo},期望 ${step.expectAttemptNo}`);
            break;
          }
          if (step.expectGeneration !== undefined && t.generation !== Number(step.expectGeneration)) {
            fail(`claim: generation=${t.generation},期望 ${step.expectGeneration}`);
            break;
          }
          if (step.expectExecutionTokenOf) {
            const prev = deliveries.get(String(step.expectExecutionTokenOf));
            if (!prev || prev.executionToken !== t.executionToken) {
              fail(`claim: executionToken 未沿用 ${String(step.expectExecutionTokenOf)} 的执行身份`);
              break;
            }
          }
          ok(`claim → cmd=${t.commandId} attempt=${t.attemptNo} gen=${t.generation} token=${t.executionToken}`);
          break;
        }

        case 'renew': {
          const d = deliveries.get(String(step.delivery));
          if (!d) {
            fail(`renew: 未知投递 ${String(step.delivery)}`);
            break;
          }
          const gateway = String(step.gateway ?? d.gateway);
          const generation = genOf(step, gateway);
          const r = await client.renew(gateway, d.leaseId, generation);
          const expect = String(step.expect ?? 'ok');
          const fenced = r.status === 409 && String(r.body?.reason ?? '').startsWith('fenced');
          if (expect === 'ok' && r.status === 200 && r.body.ok) ok(`renew 成功,租约延至 ${r.body.leaseExpiresAt}`);
          else if (expect === 'rejected' && r.status === 409 && !fenced) ok(`renew 被拒(${r.body.reason ?? r.status})`);
          else if (expect === 'fenced' && fenced) ok(`renew 被 fencing(${r.body.reason})`);
          else fail(`renew: 期望 ${expect},实际 HTTP ${r.status} ${JSON.stringify(r.body)}`);
          break;
        }

        case 'ack': {
          const d = deliveries.get(String(step.delivery));
          if (!d) {
            fail(`ack: 未知投递 ${String(step.delivery)}`);
            break;
          }
          const generation = genOf(step, d.gateway);
          const r = await client.ack(d.gateway, d.leaseId, generation, String(step.ackId), step.result ?? { device: 'ok' });
          const expect = String(step.expect ?? 'applied');
          if (r.status !== 200) {
            if (expect === 'rejected') ok(`ack 被拒(HTTP ${r.status})`);
            else fail(`ack: HTTP ${r.status} ${JSON.stringify(r.body)}`);
            break;
          }
          const actual = r.body.duplicate
            ? 'duplicate'
            : String(r.body.reason ?? '').startsWith('fenced')
              ? 'fenced'
              : r.body.outcome;
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
          await assertEventView(
            `指令 ${String(step.key ?? step.command)} 事件`,
            async () => {
              const r = await client.getEvents(id!);
              return r.status === 200 ? (r.body.events as any[]).map((e) => e.type as string) : null;
            },
            (step.includes as string[] | undefined) ?? [],
            (step.countOf as Record<string, number> | undefined) ?? {},
          );
          break;
        }

        case 'assertLineEvents': {
          const line = String(step.line ?? 'line-1');
          await assertEventView(
            `产线 ${line} 事件`,
            async () => {
              const r = await client.getLineEvents(line);
              return r.status === 200 ? (r.body.events as any[]).map((e) => e.type as string) : null;
            },
            (step.includes as string[] | undefined) ?? [],
            (step.countOf as Record<string, number> | undefined) ?? {},
          );
          break;
        }

        case 'assertOwnership': {
          const line = String(step.line ?? 'line-1');
          const r = await client.getOwnership(line);
          if (r.status !== 200) {
            fail(`assertOwnership: HTTP ${r.status}`);
            break;
          }
          const problems: string[] = [];
          if (step.owner !== undefined && r.body.ownerGatewayId !== step.owner) problems.push(`owner=${r.body.ownerGatewayId},期望 ${step.owner}`);
          if (step.generation !== undefined && r.body.generation !== Number(step.generation)) problems.push(`generation=${r.body.generation},期望 ${step.generation}`);
          if (problems.length) fail(`assertOwnership: ${problems.join(';')}`);
          else ok(`产线 ${line} 属主=${r.body.ownerGatewayId} gen=${r.body.generation}`);
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
