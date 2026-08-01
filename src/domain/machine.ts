/**
 * 领域状态机:一组纯函数。
 *
 * 输入:当前持久化状态(行数据)+ 一条消息 + 上下文(时钟/ID 生成器/配置)
 * 输出:需要写入的状态变更 + 因果事件 + 给调用方的回复
 *
 * 不触碰 HTTP、Socket、SQLite,也不读真实时钟——故障时序(崩溃、超时、
 * 迟到消息)可以在测试中通过注入时钟与消息顺序被确定性地复现。
 *
 * 核心不变量:
 * 1. executionToken 在指令创建后永不变化,重试/重新领取沿用同一执行身份。
 * 2. 只有"被持久化且被应用"的设备确认,才能把指令推进到 SUCCEEDED。
 * 3. 终态(SUCCEEDED/FAILED)不可逆:迟到的确认/续约不能悄悄复活指令。
 * 4. 每次状态变化都附带因果事件。
 */

import {
  AckRecord,
  Attempt,
  Command,
  DomainEvent,
  EventType,
} from './types';

export interface MachineConfig {
  leaseTtlMs: number;
  maxAttempts: number;
  retryBackoffMs: number;
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
  attempt?: Attempt;
  ack?: AckRecord;
  events: DomainEvent[];
}

export interface Decision<TReply> {
  writes: Writes;
  reply: TReply;
}

function ev(
  ctx: Ctx,
  commandId: string,
  attemptId: string | null,
  type: EventType,
  data: Record<string, unknown>,
): DomainEvent {
  return { commandId, attemptId, type, data, causedBy: ctx.causedBy, at: ctx.now };
}

// ---------------------------------------------------------------- submit

export interface SubmitRequest {
  idempotencyKey: string;
  action: string;
  params: unknown;
}

export interface SubmitReply {
  commandId: string;
  idempotencyKey: string;
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
        events: [
          ev(ctx, existing.id, null, 'SUBMISSION_DEDUPED', {
            idempotencyKey: req.idempotencyKey,
          }),
        ],
      },
      reply: {
        commandId: existing.id,
        idempotencyKey: existing.idempotencyKey,
        status: existing.status,
        executionToken: existing.executionToken,
        deduped: true,
      },
    };
  }
  const id = ctx.newId();
  const command: Command = {
    id,
    idempotencyKey: req.idempotencyKey,
    action: req.action,
    params: req.params ?? null,
    status: 'PENDING',
    executionToken: `exec-${id}`,
    attemptCount: 0,
    activeAttemptId: null,
    nextAttemptNotBefore: ctx.now,
    createdAt: ctx.now,
    updatedAt: ctx.now,
  };
  return {
    writes: {
      command,
      events: [
        ev(ctx, id, null, 'COMMAND_ACCEPTED', {
          idempotencyKey: req.idempotencyKey,
          action: req.action,
        }),
      ],
    },
    reply: {
      commandId: id,
      idempotencyKey: req.idempotencyKey,
      status: 'PENDING',
      executionToken: command.executionToken,
      deduped: false,
    },
  };
}

// ---------------------------------------------------------------- expire

export interface ExpireWrites {
  command: Command;
  attempt: Attempt;
  events: DomainEvent[];
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
): ExpireWrites | null {
  if (command.status !== 'DISPATCHED') return null;
  if (attempt.status !== 'ACTIVE') return null;
  if (ctx.now <= attempt.leaseExpiresAt) return null;

  const expiredAttempt: Attempt = { ...attempt, status: 'EXPIRED' };
  const events: DomainEvent[] = [
    ev(ctx, command.id, attempt.id, 'LEASE_EXPIRED', {
      attemptNo: attempt.attemptNo,
      gatewayId: attempt.gatewayId,
      leaseExpiredAt: attempt.leaseExpiresAt,
    }),
  ];

  let next: Command;
  if (command.attemptCount >= ctx.config.maxAttempts) {
    next = {
      ...command,
      status: 'FAILED',
      activeAttemptId: null,
      updatedAt: ctx.now,
    };
    events.push(
      ev(ctx, command.id, attempt.id, 'COMMAND_FAILED', {
        reason: 'attempts_exhausted',
        attempts: command.attemptCount,
      }),
    );
  } else {
    const notBefore = ctx.now + ctx.config.retryBackoffMs * command.attemptCount;
    next = {
      ...command,
      status: 'PENDING',
      activeAttemptId: null,
      nextAttemptNotBefore: notBefore,
      updatedAt: ctx.now,
    };
    events.push(
      ev(ctx, command.id, attempt.id, 'RETRY_SCHEDULED', {
        attemptNo: attempt.attemptNo + 1,
        notBefore,
      }),
    );
  }
  return { command: next, attempt: expiredAttempt, events };
}

// ---------------------------------------------------------------- claim

export interface ClaimTask {
  commandId: string;
  executionToken: string;
  attemptNo: number;
  leaseId: string;
  leaseExpiresAt: number;
  action: string;
  params: unknown;
}

/**
 * 领取任务:仅限 PENDING 且到达可领取时间的指令。
 * 新尝试 attemptNo 递增,但 executionToken 沿用指令创建时的身份。
 */
export function decideClaim(
  command: Command,
  gatewayId: string,
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
        ev(ctx, command.id, attemptId, 'DELIVERY_STARTED', {
          attemptNo,
          gatewayId,
          executionToken: command.executionToken,
          leaseExpiresAt: attempt.leaseExpiresAt,
        }),
      ],
    },
    reply: {
      commandId: command.id,
      executionToken: command.executionToken,
      attemptNo,
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
  ctx: Ctx,
): Decision<RenewReply> {
  // 时钟已越过租约:先按过期结算(与巡检器路径完全一致),再拒绝本次续约。
  const expired = expiryDecision(command, attempt, ctx);
  if (expired) {
    return {
      writes: {
        command: expired.command,
        attempt: expired.attempt,
        events: [
          ...expired.events,
          ev(ctx, command.id, attempt.id, 'RENEW_REJECTED', { reason: 'lease_expired' }),
        ],
      },
      reply: { ok: false, reason: 'lease_expired' },
    };
  }
  if (command.status !== 'DISPATCHED' || attempt.status !== 'ACTIVE') {
    return {
      writes: {
        events: [
          ev(ctx, command.id, attempt.id, 'RENEW_REJECTED', {
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
        ev(ctx, command.id, attempt.id, 'LEASE_RENEWED', {
          attemptNo: attempt.attemptNo,
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
  ctx: Ctx,
  extraEvents: DomainEvent[],
  extraWrites: Partial<Writes>,
): Decision<AckReply> {
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
        ev(ctx, command.id, attempt.id, 'DEVICE_ACK_IGNORED', {
          ackId: req.ackId,
          reason,
          attemptNo: attempt.attemptNo,
        }),
      ],
    },
    reply: { outcome: 'ignored', reason, duplicate: false, commandStatus: command.status },
  };
}

/**
 * 设备确认。判定顺序保证不变量:
 * 1. ackId 已存在 → 幂等重放,返回首次记录的结果,绝不二次生效。
 * 2. 指令已终态 → 持久化但忽略,迟到消息不能复活指令。
 * 3. 尝试已非 ACTIVE(被取代/已过期)→ 持久化但忽略。
 * 4. 时钟已越过租约 → 先按过期结算,再忽略(与巡检器语义一致)。
 * 5. 全部通过 → 持久化确认并把指令推进到 SUCCEEDED。
 */
export function decideAck(
  command: Command,
  attempt: Attempt,
  req: AckRequest,
  existingAck: AckRecord | null,
  ctx: Ctx,
): Decision<AckReply> {
  if (existingAck) {
    return {
      writes: {
        events: [
          ev(ctx, command.id, attempt.id, 'DEVICE_ACK_DUPLICATE', {
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

  if (command.status === 'SUCCEEDED') {
    return ignoredAck(command, attempt, req, 'command_already_succeeded', ctx, [], {});
  }
  if (command.status === 'FAILED') {
    return ignoredAck(command, attempt, req, 'command_terminal', ctx, [], {});
  }
  if (attempt.status !== 'ACTIVE') {
    return ignoredAck(command, attempt, req, 'attempt_not_active', ctx, [], {});
  }

  const expired = expiryDecision(command, attempt, ctx);
  if (expired) {
    return ignoredAck(expired.command, expired.attempt, req, 'lease_expired', ctx, expired.events, {
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
  const succeeded: Command = {
    ...command,
    status: 'SUCCEEDED',
    activeAttemptId: null,
    updatedAt: ctx.now,
  };
  const ackEvent = ev(ctx, command.id, attempt.id, 'DEVICE_ACK_APPLIED', {
    ackId: req.ackId,
    attemptNo: attempt.attemptNo,
  });
  return {
    writes: {
      command: succeeded,
      attempt: ackedAttempt,
      ack,
      events: [
        ackEvent,
        // COMMAND_SUCCEEDED 由已持久化的确认事件因果触发
        { ...ev(ctx, command.id, attempt.id, 'COMMAND_SUCCEEDED', { ackId: req.ackId }), causedBy: `event:${ackEvent.type}:${req.ackId}` },
      ],
    },
    reply: { outcome: 'applied', reason: null, duplicate: false, commandStatus: 'SUCCEEDED' },
  };
}
