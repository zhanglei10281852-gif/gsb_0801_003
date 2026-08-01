/**
 * 领域状态机:一组纯函数。
 *
 * 输入:当前持久化状态(行数据)+ 一条消息 + 上下文(时钟/ID 生成器/配置)
 * 输出:需要写入的状态变更 + 因果事件 + 给调用方的回复
 *
 * 不触碰 HTTP、Socket、SQLite,也不读真实时钟——故障时序(崩溃、超时、
 * 网络分区、迟到消息)可以在测试中通过注入时钟与消息顺序被确定性地复现。
 *
 * 核心不变量:
 * 1. executionToken 在指令创建后永不变化:重试、跨网关、跨代际重投都沿用同一执行身份。
 * 2. 只有"被持久化且被应用"的设备确认,才能把指令推进到 SUCCEEDED。
 * 3. 终态(SUCCEEDED/FAILED)不可逆:迟到的确认/续约不能悄悄复活指令。
 * 4. fencing:只有当前属主网关、持当前代际、且所有权租约未过期,才能
 *    领取/续约/推进确认;其余一律拒绝,且不得改变领域状态。
 * 5. 每次状态变化都附带因果事件。
 */

import {
  AckRecord,
  Attempt,
  Command,
  DomainEvent,
  EventType,
  Ownership,
} from './types';

export interface MachineConfig {
  leaseTtlMs: number;
  maxAttempts: number;
  retryBackoffMs: number;
  ownershipLeaseTtlMs: number;
}

/** 决策上下文:全部外部性都从这里注入 */
export interface Ctx {
  now: number;
  newId: () => string;
  config: MachineConfig;
  causedBy: string;
}

/** 状态机产出的写集合(由存储适配器在单个事务中落库) */
export interface Writes {
  command?: Command;
  /** 被取代而退役的旧指令(仅 supersede 决策产生,与新指令原子落库) */
  retiredCommand?: Command;
  attempt?: Attempt;
  ack?: AckRecord;
  ownership?: Ownership;
  events: DomainEvent[];
}

export interface Decision<TReply> {
  writes: Writes;
  reply: TReply;
}

function ev(
  ctx: Ctx,
  commandId: string | null,
  lineId: string | null,
  attemptId: string | null,
  type: EventType,
  data: Record<string, unknown>,
): DomainEvent {
  return { commandId, lineId, attemptId, type, data, causedBy: ctx.causedBy, at: ctx.now };
}

function cmdEv(
  ctx: Ctx,
  command: Command,
  attemptId: string | null,
  type: EventType,
  data: Record<string, unknown>,
): DomainEvent {
  return ev(ctx, command.id, command.lineId, attemptId, type, data);
}

function lineEv(
  ctx: Ctx,
  lineId: string,
  type: EventType,
  data: Record<string, unknown>,
): DomainEvent {
  return ev(ctx, null, lineId, null, type, data);
}

// ---------------------------------------------------------------- ownership

export type FenceStatus =
  | 'ok'
  | 'no_ownership'              // 产线尚无属主(未心跳)
  | 'not_owner'                 // 当前属主是别的网关
  | 'stale_generation'          // 是属主但代际已旧(被接管过)
  | 'ownership_lease_expired';  // 所有权租约已过期,必须重新心跳

/**
 * fencing 判定:网关是否能以给定代际操作该产线。
 * 注意:即使仍是属主,所有权租约过期后同样被 fencing——它可能已被分区,
 * 必须先心跳重新获取(代际 +1)才能继续操作。
 */
export function fenceStatus(
  ownership: Ownership | null,
  gatewayId: string,
  generation: number,
  now: number,
): FenceStatus {
  if (!ownership) return 'no_ownership';
  if (ownership.ownerGatewayId !== gatewayId) return 'not_owner';
  if (ownership.generation !== generation) return 'stale_generation';
  if (now > ownership.leaseExpiresAt) return 'ownership_lease_expired';
  return 'ok';
}

export interface HeartbeatReply {
  acquired: boolean;
  lineId: string;
  gatewayId: string;
  /** 当前代际(无论本次是否获得所有权) */
  generation: number;
  leaseExpiresAt: number | null;
  owner: string;
  /** 本次心跳发生接管时,被取代的前属主(供服务层中止其在途投递) */
  tookOverFrom: string | null;
}

/**
 * 网关心跳:获取/续约产线所有权。
 * - 无所有权 → 获得,代际 1
 * - 本人持有且未过期 → 续约,代际不变
 * - 租约已过期(无论谁持有)→ 接管,代际 +1;旧代际自此被 fencing
 * - 他人持有且未过期 → 拒绝(争抢留痕),调用方据此知道自己不是属主
 */
export function decideHeartbeat(
  ownership: Ownership | null,
  gatewayId: string,
  lineId: string,
  ctx: Ctx,
): Decision<HeartbeatReply> {
  const leaseExpiresAt = ctx.now + ctx.config.ownershipLeaseTtlMs;

  if (!ownership) {
    const next: Ownership = { lineId, ownerGatewayId: gatewayId, generation: 1, leaseExpiresAt, updatedAt: ctx.now };
    return {
      writes: {
        ownership: next,
        events: [lineEv(ctx, lineId, 'OWNERSHIP_ACQUIRED', { gatewayId, generation: 1, leaseExpiresAt })],
      },
      reply: { acquired: true, lineId, gatewayId, generation: 1, leaseExpiresAt, owner: gatewayId, tookOverFrom: null },
    };
  }

  if (ctx.now > ownership.leaseExpiresAt) {
    const next: Ownership = {
      lineId,
      ownerGatewayId: gatewayId,
      generation: ownership.generation + 1,
      leaseExpiresAt,
      updatedAt: ctx.now,
    };
    return {
      writes: {
        ownership: next,
        events: [
          lineEv(ctx, lineId, 'OWNERSHIP_TAKEN_OVER', {
            previousGateway: ownership.ownerGatewayId,
            previousGeneration: ownership.generation,
            gatewayId,
            generation: next.generation,
            reason: 'ownership_lease_expired',
          }),
        ],
      },
      reply: { acquired: true, lineId, gatewayId, generation: next.generation, leaseExpiresAt, owner: gatewayId, tookOverFrom: ownership.ownerGatewayId },
    };
  }

  if (ownership.ownerGatewayId === gatewayId) {
    const next: Ownership = { ...ownership, leaseExpiresAt, updatedAt: ctx.now };
    return {
      writes: {
        ownership: next,
        events: [lineEv(ctx, lineId, 'OWNERSHIP_RENEWED', { gatewayId, generation: next.generation, leaseExpiresAt })],
      },
      reply: { acquired: true, lineId, gatewayId, generation: next.generation, leaseExpiresAt, owner: gatewayId, tookOverFrom: null },
    };
  }

  return {
    writes: {
      events: [
        lineEv(ctx, lineId, 'OWNERSHIP_HEARTBEAT_REJECTED', {
          owner: ownership.ownerGatewayId,
          generation: ownership.generation,
          contender: gatewayId,
        }),
      ],
    },
    reply: {
      acquired: false,
      lineId,
      gatewayId,
      generation: ownership.generation,
      leaseExpiresAt: ownership.leaseExpiresAt,
      owner: ownership.ownerGatewayId,
      tookOverFrom: null,
    },
  };
}

// ---------------------------------------------------------------- submit

export interface SubmitRequest {
  idempotencyKey: string;
  action: string;
  params: unknown;
  lineId?: string;
}

export interface SubmitReply {
  commandId: string;
  idempotencyKey: string;
  lineId: string;
  status: Command['status'];
  executionToken: string;
  deduped: boolean;
}

/**
 * 提交指令。若幂等键已存在(包括"上次持久化后、响应前崩溃"导致的客户端重试),
 * 直接返回已存在的指令,绝不生成第二条设备动作。
 */
export function decideSubmit(
  existing: Command | null,
  req: SubmitRequest,
  ctx: Ctx,
): Decision<SubmitReply> {
  if (existing) {
    return {
      writes: {
        events: [cmdEv(ctx, existing, null, 'SUBMISSION_DEDUPED', { idempotencyKey: req.idempotencyKey })],
      },
      reply: {
        commandId: existing.id,
        idempotencyKey: existing.idempotencyKey,
        lineId: existing.lineId,
        status: existing.status,
        executionToken: existing.executionToken,
        deduped: true,
      },
    };
  }
  const id = ctx.newId();
  const lineId = req.lineId ?? 'default';
  const command: Command = {
    id,
    idempotencyKey: req.idempotencyKey,
    lineId,
    action: req.action,
    params: req.params ?? null,
    status: 'PENDING',
    executionToken: `exec-${id}`,
    attemptCount: 0,
    activeAttemptId: null,
    nextAttemptNotBefore: ctx.now,
    supersedesCommandId: null,
    supersededByCommandId: null,
    createdAt: ctx.now,
    updatedAt: ctx.now,
  };
  return {
    writes: {
      command,
      events: [cmdEv(ctx, command, null, 'COMMAND_ACCEPTED', { idempotencyKey: req.idempotencyKey, action: req.action, lineId })],
    },
    reply: { commandId: id, idempotencyKey: req.idempotencyKey, lineId, status: 'PENDING', executionToken: command.executionToken, deduped: false },
  };
}

// ---------------------------------------------------------------- expire / abort

export interface TransitionWrites {
  command: Command;
  attempt: Attempt;
  events: DomainEvent[];
}

/** 过期/中止后的公共收尾:重派(沿用执行身份)或终结 */
function rescheduleOrFail(
  command: Command,
  attempt: Attempt,
  ctx: Ctx,
  events: DomainEvent[],
  opts: { backoff: boolean },
): TransitionWrites {
  const doneAttempt: Attempt = { ...attempt, status: 'EXPIRED' };
  if (command.attemptCount >= ctx.config.maxAttempts) {
    const next: Command = { ...command, status: 'FAILED', activeAttemptId: null, updatedAt: ctx.now };
    events.push(
      cmdEv(ctx, command, attempt.id, 'COMMAND_FAILED', { reason: 'attempts_exhausted', attempts: command.attemptCount }),
    );
    return { command: next, attempt: doneAttempt, events };
  }
  // 接管中止不退避(应立即由新属主重投);普通过期按线性退避
  const notBefore = opts.backoff ? ctx.now + ctx.config.retryBackoffMs * command.attemptCount : ctx.now;
  const next: Command = {
    ...command,
    status: 'PENDING',
    activeAttemptId: null,
    nextAttemptNotBefore: notBefore,
    updatedAt: ctx.now,
  };
  events.push(cmdEv(ctx, command, attempt.id, 'RETRY_SCHEDULED', { attemptNo: attempt.attemptNo + 1, notBefore }));
  return { command: next, attempt: doneAttempt, events };
}

/**
 * 租约超期判定(纯函数):仅当指令在 DISPATCHED、尝试为 ACTIVE 且 now 已越过
 * 租约截止时间时生效。返回 null 表示未过期。
 *
 * 过期语义只取决于时钟,不取决于巡检器何时运行——因此"迟到消息"与
 * "巡检器先跑"产生完全一致的状态迁移,消除时序竞态。
 */
export function expiryDecision(
  command: Command,
  attempt: Attempt,
  ctx: Ctx,
): TransitionWrites | null {
  if (command.status !== 'DISPATCHED') return null;
  if (attempt.status !== 'ACTIVE') return null;
  if (ctx.now <= attempt.leaseExpiresAt) return null;

  const events: DomainEvent[] = [
    cmdEv(ctx, command, attempt.id, 'LEASE_EXPIRED', {
      attemptNo: attempt.attemptNo,
      gatewayId: attempt.gatewayId,
      generation: attempt.generation,
      leaseExpiredAt: attempt.leaseExpiresAt,
    }),
  ];
  return rescheduleOrFail(command, attempt, ctx, events, { backoff: true });
}

/**
 * 接管中止:所有权易主时,前属主仍在飞的投递立即作废(不等其自然过期),
 * 指令立刻回到 PENDING 供新属主重投。旧属主后续对该租约的任何操作都会被 fencing。
 */
export function abortDecision(
  command: Command,
  attempt: Attempt,
  ctx: Ctx,
  reason: string,
): TransitionWrites | null {
  if (command.status !== 'DISPATCHED') return null;
  if (attempt.status !== 'ACTIVE') return null;

  const events: DomainEvent[] = [
    cmdEv(ctx, command, attempt.id, 'ATTEMPT_ABORTED', {
      reason,
      attemptNo: attempt.attemptNo,
      gatewayId: attempt.gatewayId,
      generation: attempt.generation,
    }),
  ];
  return rescheduleOrFail(command, attempt, ctx, events, { backoff: false });
}

// ---------------------------------------------------------------- claim

export interface ClaimTask {
  commandId: string;
  lineId: string;
  executionToken: string;
  attemptNo: number;
  generation: number;
  leaseId: string;
  leaseExpiresAt: number;
  action: string;
  params: unknown;
}

/**
 * 领取任务(fencing 校验由服务层在调用前完成)。
 * 新尝试 attemptNo 递增,记录当前所有权代际;executionToken 沿用指令创建时的身份。
 */
export function decideClaim(
  command: Command,
  gatewayId: string,
  generation: number,
  ctx: Ctx,
): Decision<ClaimTask> {
  const attemptNo = command.attemptCount + 1;
  const attemptId = ctx.newId();
  const leaseId = ctx.newId();
  const attempt: Attempt = {
    id: attemptId,
    commandId: command.id,
    attemptNo,
    gatewayId,
    generation,
    leaseId,
    status: 'ACTIVE',
    leasedAt: ctx.now,
    leaseExpiresAt: ctx.now + ctx.config.leaseTtlMs,
    renewedCount: 0,
  };
  const next: Command = {
    ...command,
    status: 'DISPATCHED',
    attemptCount: attemptNo,
    activeAttemptId: attemptId,
    updatedAt: ctx.now,
  };
  return {
    writes: {
      command: next,
      attempt,
      events: [
        cmdEv(ctx, command, attemptId, 'DELIVERY_STARTED', {
          attemptNo,
          gatewayId,
          generation,
          executionToken: command.executionToken,
          leaseExpiresAt: attempt.leaseExpiresAt,
        }),
      ],
    },
    reply: {
      commandId: command.id,
      lineId: command.lineId,
      executionToken: command.executionToken,
      attemptNo,
      generation,
      leaseId,
      leaseExpiresAt: attempt.leaseExpiresAt,
      action: command.action,
      params: command.params,
    },
  };
}

// ---------------------------------------------------------------- renew

export type RenewReply =
  | { ok: true; leaseExpiresAt: number }
  | { ok: false; reason: string };

export function decideRenew(
  command: Command,
  attempt: Attempt,
  generation: number,
  ownership: Ownership | null,
  ctx: Ctx,
): Decision<RenewReply> {
  // fencing 优先:旧代际/非属主/所有权过期,一律拒绝且不改状态
  const fence = fenceStatus(ownership, attempt.gatewayId, generation, ctx.now);
  if (fence !== 'ok') {
    return {
      writes: {
        events: [cmdEv(ctx, command, attempt.id, 'RENEW_FENCED', { reason: fence, gatewayId: attempt.gatewayId, generation })],
      },
      reply: { ok: false, reason: `fenced_${fence}` },
    };
  }

  // 时钟已越过租约:先按过期结算(与巡检器路径完全一致),再拒绝本次续约。
  const expired = expiryDecision(command, attempt, ctx);
  if (expired) {
    return {
      writes: {
        command: expired.command,
        attempt: expired.attempt,
        events: [
          ...expired.events,
          cmdEv(ctx, command, attempt.id, 'RENEW_REJECTED', { reason: 'lease_expired' }),
        ],
      },
      reply: { ok: false, reason: 'lease_expired' },
    };
  }
  if (command.status !== 'DISPATCHED' || attempt.status !== 'ACTIVE') {
    return {
      writes: {
        events: [
          cmdEv(ctx, command, attempt.id, 'RENEW_REJECTED', {
            reason: 'attempt_not_active',
            commandStatus: command.status,
            attemptStatus: attempt.status,
          }),
        ],
      },
      reply: { ok: false, reason: 'attempt_not_active' },
    };
  }
  const renewed: Attempt = {
    ...attempt,
    leaseExpiresAt: ctx.now + ctx.config.leaseTtlMs,
    renewedCount: attempt.renewedCount + 1,
  };
  return {
    writes: {
      attempt: renewed,
      events: [
        cmdEv(ctx, command, attempt.id, 'LEASE_RENEWED', {
          attemptNo: attempt.attemptNo,
          generation: attempt.generation,
          leaseExpiresAt: renewed.leaseExpiresAt,
          renewedCount: renewed.renewedCount,
        }),
      ],
    },
    reply: { ok: true, leaseExpiresAt: renewed.leaseExpiresAt },
  };
}

// ---------------------------------------------------------------- ack

export interface AckRequest {
  ackId: string;
  result: unknown;
  /** 网关所持所有权代际 */
  generation: number;
}

export interface AckReply {
  outcome: 'applied' | 'ignored';
  reason: string | null;
  duplicate: boolean;
  commandStatus: Command['status'];
}

function ignoredAck(
  command: Command,
  attempt: Attempt,
  req: AckRequest,
  reason: string,
  eventType: 'DEVICE_ACK_IGNORED' | 'ACK_FENCED',
  ctx: Ctx,
  extraEvents: DomainEvent[],
  extraWrites: Partial<Writes>,
): Decision<AckReply> {
  // 被 fencing 的确认同样持久化留痕(设备可能真的执行了),但绝不推进状态
  const ack: AckRecord = {
    id: req.ackId,
    commandId: command.id,
    attemptId: attempt.id,
    result: req.result ?? null,
    applied: false,
    ignoreReason: reason,
    receivedAt: ctx.now,
  };
  return {
    writes: {
      ...extraWrites,
      ack,
      events: [
        ...extraEvents,
        cmdEv(ctx, command, attempt.id, eventType, { ackId: req.ackId, reason, attemptNo: attempt.attemptNo, generation: req.generation }),
      ],
    },
    reply: { outcome: 'ignored', reason, duplicate: false, commandStatus: command.status },
  };
}

/**
 * 设备确认。判定顺序保证不变量:
 * 1. ackId 已存在 → 幂等重放,返回首次记录的结果,绝不二次生效。
 * 2. fencing:非属主/旧代际/所有权过期 → 持久化留痕但忽略,不得推进状态。
 * 3. 指令已终态 → 持久化但忽略,迟到消息不能复活指令。
 * 4. 尝试已非 ACTIVE(被取代/已过期/被中止)→ 持久化但忽略。
 * 5. 时钟已越过租约 → 先按过期结算,再忽略(与巡检器语义一致)。
 * 6. 全部通过 → 持久化确认并把指令推进到 SUCCEEDED。
 */
export function decideAck(
  command: Command,
  attempt: Attempt,
  req: AckRequest,
  ownership: Ownership | null,
  existingAck: AckRecord | null,
  ctx: Ctx,
): Decision<AckReply> {
  if (existingAck) {
    return {
      writes: {
        events: [
          cmdEv(ctx, command, attempt.id, 'DEVICE_ACK_DUPLICATE', {
            ackId: req.ackId,
            originalApplied: existingAck.applied,
            originalIgnoreReason: existingAck.ignoreReason,
          }),
        ],
      },
      reply: {
        outcome: existingAck.applied ? 'applied' : 'ignored',
        reason: existingAck.ignoreReason,
        duplicate: true,
        commandStatus: command.status,
      },
    };
  }

  const fence = fenceStatus(ownership, attempt.gatewayId, req.generation, ctx.now);
  if (fence !== 'ok') {
    return ignoredAck(command, attempt, req, `fenced_${fence}`, 'ACK_FENCED', ctx, [], {});
  }

  if (command.status === 'SUCCEEDED') {
    return ignoredAck(command, attempt, req, 'command_already_succeeded', 'DEVICE_ACK_IGNORED', ctx, [], {});
  }
  if (command.status === 'FAILED') {
    return ignoredAck(command, attempt, req, 'command_terminal', 'DEVICE_ACK_IGNORED', ctx, [], {});
  }
  if (command.status === 'CANCELLED') {
    return ignoredAck(command, attempt, req, 'command_cancelled', 'DEVICE_ACK_IGNORED', ctx, [], {});
  }
  if (command.status === 'SUPERSEDED') {
    return ignoredAck(command, attempt, req, 'command_superseded', 'DEVICE_ACK_IGNORED', ctx, [], {});
  }
  if (attempt.status !== 'ACTIVE') {
    return ignoredAck(command, attempt, req, 'attempt_not_active', 'DEVICE_ACK_IGNORED', ctx, [], {});
  }

  const expired = expiryDecision(command, attempt, ctx);
  if (expired) {
    return ignoredAck(expired.command, expired.attempt, req, 'lease_expired', 'DEVICE_ACK_IGNORED', ctx, expired.events, {
      command: expired.command,
      attempt: expired.attempt,
    });
  }

  const ack: AckRecord = {
    id: req.ackId,
    commandId: command.id,
    attemptId: attempt.id,
    result: req.result ?? null,
    applied: true,
    ignoreReason: null,
    receivedAt: ctx.now,
  };
  const ackedAttempt: Attempt = { ...attempt, status: 'ACKED' };
  const succeeded: Command = { ...command, status: 'SUCCEEDED', activeAttemptId: null, updatedAt: ctx.now };
  const ackEvent = cmdEv(ctx, command, attempt.id, 'DEVICE_ACK_APPLIED', { ackId: req.ackId, attemptNo: attempt.attemptNo, generation: req.generation });
  return {
    writes: {
      command: succeeded,
      attempt: ackedAttempt,
      ack,
      events: [
        ackEvent,
        // COMMAND_SUCCEEDED 由已持久化的确认事件因果触发
        { ...cmdEv(ctx, command, attempt.id, 'COMMAND_SUCCEEDED', { ackId: req.ackId }), causedBy: `event:${ackEvent.type}:${req.ackId}` },
      ],
    },
    reply: { outcome: 'applied', reason: null, duplicate: false, commandStatus: 'SUCCEEDED' },
  };
}

// ---------------------------------------------------------------- cancel

export interface CancelRequest {
  requestedBy?: string;
  reason?: string;
}

export type CancelReply =
  | { cancelled: true; deduped: boolean }
  | { cancelled: false; reason: string };

/**
 * 紧急撤回:仅对尚未完成的指令(PENDING/DISPATCHED)生效。
 * - 已被设备确认执行(SUCCEEDED)的指令绝不能伪装成撤回成功:拒绝并留痕。
 * - FAILED/SUPERSEDED 已是终态:拒绝。
 * - 重复撤回(含"撤回落库后、响应前崩溃"的重试):幂等去重。
 * - DISPATCHED 中的指令:在途投递一并中止,网关随后的确认/续约
 *   只会得到 command_cancelled / attempt_not_active,不能复活指令。
 */
export function decideCancel(
  command: Command,
  activeAttempt: Attempt | null,
  req: CancelRequest,
  ctx: Ctx,
): Decision<CancelReply> {
  if (command.status === 'CANCELLED') {
    return {
      writes: { events: [cmdEv(ctx, command, null, 'CANCEL_DEDUPED', {})] },
      reply: { cancelled: true, deduped: true },
    };
  }
  if (command.status === 'SUCCEEDED') {
    return {
      writes: { events: [cmdEv(ctx, command, null, 'CANCEL_REJECTED', { reason: 'already_succeeded', requestedBy: req.requestedBy ?? null })] },
      reply: { cancelled: false, reason: 'already_succeeded' },
    };
  }
  if (command.status !== 'PENDING' && command.status !== 'DISPATCHED') {
    return {
      writes: { events: [cmdEv(ctx, command, null, 'CANCEL_REJECTED', { reason: 'already_terminal', status: command.status })] },
      reply: { cancelled: false, reason: 'already_terminal' },
    };
  }

  const events: DomainEvent[] = [];
  let attempt: Attempt | undefined;
  if (command.status === 'DISPATCHED' && activeAttempt && activeAttempt.status === 'ACTIVE') {
    attempt = { ...activeAttempt, status: 'EXPIRED' };
    events.push(
      cmdEv(ctx, command, attempt.id, 'ATTEMPT_ABORTED', {
        reason: 'command_cancelled',
        attemptNo: attempt.attemptNo,
        gatewayId: attempt.gatewayId,
        generation: attempt.generation,
      }),
    );
  }
  const next: Command = { ...command, status: 'CANCELLED', activeAttemptId: null, updatedAt: ctx.now };
  events.push(
    cmdEv(ctx, command, attempt?.id ?? null, 'COMMAND_CANCELLED', {
      requestedBy: req.requestedBy ?? null,
      reason: req.reason ?? null,
    }),
  );
  return { writes: { command: next, attempt, events }, reply: { cancelled: true, deduped: false } };
}

// ---------------------------------------------------------------- supersede

export interface SupersedeRequest extends SubmitRequest {
  /** 被取代指令的幂等键或指令 id */
  supersedesKey: string;
}

export type SupersedeReply =
  | { accepted: true; deduped: boolean; commandId: string; supersededCommandId: string; executionToken: string; lineId: string }
  | { accepted: false; reason: string };

/**
 * 替代指令:原子完成"旧指令退役(SUPERSEDED)+ 新指令生成(PENDING)"。
 * - 旧指令已成功:拒绝整个提交(不生成新指令),不能把已执行的动作伪装成被取代;
 * - 旧指令已终态(FAILED/CANCELLED/SUPERSEDED):拒绝(无活物可退役,可直接普通提交);
 * - 崩溃重试(新指令幂等键已存在):幂等重放,绝不生成第二条替代指令;
 * - 旧指令在途投递一并中止,迟到确认只能得到 command_superseded;
 * - 新指令是新的业务请求,获得自己的 executionToken(首轮稳定身份自首轮起算);
 *   lineId 缺省沿用旧指令产线。取代链经 supersedesCommandId / supersededByCommandId 双向可查。
 */
export function decideSupersede(
  oldCommand: Command,
  oldActiveAttempt: Attempt | null,
  existingNew: Command | null,
  req: SupersedeRequest,
  ctx: Ctx,
): Decision<SupersedeReply> {
  if (existingNew) {
    return {
      writes: {
        events: [cmdEv(ctx, existingNew, null, 'SUBMISSION_DEDUPED', { idempotencyKey: req.idempotencyKey, supersedesKey: req.supersedesKey })],
      },
      reply: {
        accepted: true,
        deduped: true,
        commandId: existingNew.id,
        supersededCommandId: existingNew.supersedesCommandId ?? oldCommand.id,
        executionToken: existingNew.executionToken,
        lineId: existingNew.lineId,
      },
    };
  }

  if (oldCommand.status === 'SUCCEEDED') {
    return {
      writes: {
        events: [cmdEv(ctx, oldCommand, null, 'SUPERSEDE_REJECTED', { reason: 'already_succeeded', byIdempotencyKey: req.idempotencyKey })],
      },
      reply: { accepted: false, reason: 'already_succeeded' },
    };
  }
  if (oldCommand.status !== 'PENDING' && oldCommand.status !== 'DISPATCHED') {
    return {
      writes: {
        events: [cmdEv(ctx, oldCommand, null, 'SUPERSEDE_REJECTED', { reason: 'already_terminal', status: oldCommand.status })],
      },
      reply: { accepted: false, reason: 'already_terminal' },
    };
  }

  const events: DomainEvent[] = [];
  let attempt: Attempt | undefined;
  if (oldCommand.status === 'DISPATCHED' && oldActiveAttempt && oldActiveAttempt.status === 'ACTIVE') {
    attempt = { ...oldActiveAttempt, status: 'EXPIRED' };
    events.push(
      cmdEv(ctx, oldCommand, attempt.id, 'ATTEMPT_ABORTED', {
        reason: 'command_superseded',
        attemptNo: attempt.attemptNo,
        gatewayId: attempt.gatewayId,
        generation: attempt.generation,
      }),
    );
  }

  const newId = ctx.newId();
  const lineId = req.lineId ?? oldCommand.lineId;
  const retired: Command = {
    ...oldCommand,
    status: 'SUPERSEDED',
    activeAttemptId: null,
    supersededByCommandId: newId,
    updatedAt: ctx.now,
  };
  const created: Command = {
    id: newId,
    idempotencyKey: req.idempotencyKey,
    lineId,
    action: req.action,
    params: req.params ?? null,
    status: 'PENDING',
    executionToken: `exec-${newId}`,
    attemptCount: 0,
    activeAttemptId: null,
    nextAttemptNotBefore: ctx.now,
    supersedesCommandId: oldCommand.id,
    supersededByCommandId: null,
    createdAt: ctx.now,
    updatedAt: ctx.now,
  };
  events.push(
    cmdEv(ctx, oldCommand, attempt?.id ?? null, 'COMMAND_SUPERSEDED', { byCommandId: newId, byIdempotencyKey: req.idempotencyKey }),
    cmdEv(ctx, created, null, 'COMMAND_ACCEPTED', {
      idempotencyKey: req.idempotencyKey,
      action: req.action,
      lineId,
      supersedesCommandId: oldCommand.id,
      supersedesKey: req.supersedesKey,
    }),
  );
  return {
    writes: { command: created, retiredCommand: retired, attempt, events },
    reply: { accepted: true, deduped: false, commandId: newId, supersededCommandId: oldCommand.id, executionToken: created.executionToken, lineId },
  };
}
