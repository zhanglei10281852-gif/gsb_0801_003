/**
 * 应用服务层:装载持久化状态 → 调用纯状态机 → 单事务落库。
 * 不持有任何领域内存状态(含所有权),崩溃重启等价于"换一个实例继续读库",
 * fencing 代际因此跨重启保持。
 */
import { randomUUID } from 'node:crypto';
import {
  AckReply,
  AckRequest,
  CancelReply,
  CancelRequest,
  ClaimTask,
  Ctx,
  Decision,
  FenceStatus,
  HeartbeatReply,
  MachineConfig,
  RenewReply,
  SubmitReply,
  SubmitRequest,
  SupersedeReply,
  SupersedeRequest,
  abortDecision,
  decideAck,
  decideCancel,
  decideClaim,
  decideHeartbeat,
  decideRenew,
  decideSubmit,
  decideSupersede,
  expiryDecision,
  fenceStatus,
} from '../domain/machine';
import { Command, Ownership, StoredEvent } from '../domain/types';
import { SqliteRepository } from '../storage/sqlite';

export class HttpError extends Error {
  constructor(
    public status: number,
    public code: string,
    message: string,
  ) {
    super(message);
  }
}

export type Clock = () => number;
export type IdGen = () => string;

/** 领取结果:要么被 fencing(并留痕),要么拿到任务列表 */
export type ClaimResult = { fenced: FenceStatus } | { tasks: ClaimTask[] };

export class DispatchService {
  constructor(
    private repo: SqliteRepository,
    private config: MachineConfig,
    private clock: Clock = Date.now,
    private ids: IdGen = randomUUID,
  ) {}

  private ctx(causedBy: string): Ctx {
    return { now: this.clock(), newId: this.ids, config: this.config, causedBy };
  }

  private apply<T>(d: Decision<T>): T {
    const w = d.writes;
    if (w.command) this.repo.upsertCommand(w.command);
    if (w.retiredCommand) this.repo.upsertCommand(w.retiredCommand);
    if (w.attempt) this.repo.upsertAttempt(w.attempt);
    if (w.ack) this.repo.insertAck(w.ack);
    if (w.ownership) this.repo.upsertOwnership(w.ownership);
    for (const e of w.events) this.repo.insertEvent(e);
    return d.reply;
  }

  submit(req: SubmitRequest | SupersedeRequest, requestId: string): SubmitReply | SupersedeReply {
    if (!req.idempotencyKey || !req.action) {
      throw new HttpError(400, 'bad_request', 'idempotencyKey 与 action 必填');
    }
    const supersedesKey = (req as SupersedeRequest).supersedesKey;
    return this.repo.tx(() => {
      if (supersedesKey) {
        // 替代指令:旧指令退役与新指令生成在同一事务中原子完成
        const old = this.repo.findCommandByKey(supersedesKey) ?? this.repo.findCommandById(supersedesKey);
        if (!old) throw new HttpError(404, 'supersede_target_not_found', `被取代指令 ${supersedesKey} 不存在`);
        const existingNew = this.repo.findCommandByKey(req.idempotencyKey);
        const oldAttempt = old.activeAttemptId ? this.repo.findAttemptById(old.activeAttemptId) : null;
        return this.apply(decideSupersede(old, oldAttempt, existingNew, req as SupersedeRequest, this.ctx(requestId)));
      }
      const existing = this.repo.findCommandByKey(req.idempotencyKey);
      return this.apply(decideSubmit(existing, req, this.ctx(requestId)));
    });
  }

  /** 紧急撤回:仅对未完成指令生效;已成功的指令拒绝(不能伪装成撤回成功) */
  cancel(idOrKey: string, req: CancelRequest, requestId: string): CancelReply {
    return this.repo.tx(() => {
      const command = this.repo.findCommandById(idOrKey) ?? this.repo.findCommandByKey(idOrKey);
      if (!command) throw new HttpError(404, 'not_found', `指令 ${idOrKey} 不存在`);
      const attempt = command.activeAttemptId ? this.repo.findAttemptById(command.activeAttemptId) : null;
      return this.apply(decideCancel(command, attempt, req, this.ctx(requestId)));
    });
  }

  /**
   * 网关心跳:获取/续约产线所有权。
   * 发生接管时,在同一事务内中止前属主在该产线上的在途投递,
   * 使其立即(不退避)回到队列供新属主重投——接管因果完整落库。
   */
  heartbeat(gatewayId: string, lineId: string, requestId: string): HeartbeatReply {
    if (!gatewayId || !lineId) throw new HttpError(400, 'bad_request', 'gatewayId 与 lineId 必填');
    return this.repo.tx(() => {
      const ctx = this.ctx(requestId);
      const reply = this.apply(decideHeartbeat(this.repo.getOwnership(lineId), gatewayId, lineId, ctx));
      if (reply.tookOverFrom) {
        const inflight = this.repo.listActiveAttemptsByGatewayOnLine(lineId, reply.tookOverFrom);
        for (const attempt of inflight) {
          const command = this.mustCommand(attempt.commandId);
          const w = abortDecision(command, attempt, ctx, 'ownership_takeover');
          if (w) this.apply({ writes: w, reply: undefined });
        }
      }
      return reply;
    });
  }

  getOwnership(lineId: string): Ownership | null {
    return this.repo.getOwnership(lineId);
  }

  /**
   * 领取任务。先做 fencing(旧代际/非属主/所有权过期一律拒绝并留痕,不改状态),
   * 再在同一事务内惰性结算已过期租约,最后挑选该产线可领取指令。
   */
  claim(gatewayId: string, lineId: string, generation: number, limit: number, requestId: string): ClaimResult {
    if (!gatewayId || !lineId) throw new HttpError(400, 'bad_request', 'gatewayId 与 lineId 必填');
    return this.repo.tx(() => {
      const ctx = this.ctx(requestId);
      const ownership = this.repo.getOwnership(lineId);
      const fence = fenceStatus(ownership, gatewayId, generation, ctx.now);
      if (fence !== 'ok') {
        this.apply({
          writes: {
            events: [
              { commandId: null, lineId, attemptId: null, type: 'CLAIM_FENCED' as const, data: { gatewayId, generation, reason: fence }, causedBy: requestId, at: ctx.now },
            ],
          },
          reply: undefined,
        });
        return { fenced: fence };
      }
      this.settleExpired(ctx);
      const claimable = this.repo.listClaimable(lineId, ctx.now, Math.max(1, Math.min(limit, 100)));
      return { tasks: claimable.map((c) => this.apply(decideClaim(c, gatewayId, generation, ctx))) };
    });
  }

  renew(leaseId: string, gatewayId: string, generation: number, requestId: string): RenewReply {
    return this.repo.tx(() => {
      const attempt = this.repo.findAttemptByLease(leaseId);
      if (!attempt) throw new HttpError(404, 'lease_not_found', `未知租约 ${leaseId}`);
      const command = this.mustCommand(attempt.commandId);
      if (attempt.gatewayId !== gatewayId) {
        throw new HttpError(403, 'gateway_mismatch', '租约不属于该网关');
      }
      const ownership = this.repo.getOwnership(command.lineId);
      return this.apply(decideRenew(command, attempt, generation, ownership, this.ctx(requestId)));
    });
  }

  ack(leaseId: string, gatewayId: string, req: AckRequest, requestId: string): AckReply {
    if (!req.ackId) throw new HttpError(400, 'bad_request', 'ackId 必填');
    return this.repo.tx(() => {
      const attempt = this.repo.findAttemptByLease(leaseId);
      if (!attempt) throw new HttpError(404, 'lease_not_found', `未知租约 ${leaseId}`);
      const command = this.mustCommand(attempt.commandId);
      if (attempt.gatewayId !== gatewayId) {
        throw new HttpError(403, 'gateway_mismatch', '租约不属于该网关');
      }
      const ownership = this.repo.getOwnership(command.lineId);
      const existingAck = this.repo.findAckById(req.ackId);
      return this.apply(decideAck(command, attempt, req, ownership, existingAck, this.ctx(requestId)));
    });
  }

  /** 巡检器:结算所有过期租约(与领取/续约/确认路径共享同一判定函数) */
  expireDue(causedBy = 'reaper'): number {
    return this.repo.tx(() => this.settleExpired(this.ctx(causedBy)));
  }

  private settleExpired(ctx: Ctx): number {
    const expired = this.repo.listExpiredDispatched(ctx.now);
    for (const command of expired) {
      const attempt = this.repo.findAttemptById(command.activeAttemptId!);
      if (!attempt) continue;
      const w = expiryDecision(command, attempt, ctx);
      if (w) this.apply({ writes: w, reply: undefined });
    }
    return expired.length;
  }

  getCommand(idOrKey: string): (Command & { events?: StoredEvent[] }) | null {
    return this.repo.findCommandById(idOrKey) ?? this.repo.findCommandByKey(idOrKey);
  }

  listEvents(commandId?: string): StoredEvent[] {
    return this.repo.listEvents(commandId);
  }

  listLineEvents(lineId: string): StoredEvent[] {
    return this.repo.listEventsByLine(lineId);
  }

  private mustCommand(id: string): Command {
    const c = this.repo.findCommandById(id);
    if (!c) throw new HttpError(500, 'corrupt_state', `指令 ${id} 不存在`);
    return c;
  }
}
