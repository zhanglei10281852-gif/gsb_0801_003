/**
 * 应用服务层:装载持久化状态 → 调用纯状态机 → 单事务落库。
 * 不持有任何领域内存状态,崩溃重启等价于"换一个实例继续读库"。
 */
import { randomUUID } from 'node:crypto';
import {
  AckReply,
  AckRequest,
  ClaimTask,
  Ctx,
  Decision,
  MachineConfig,
  RenewReply,
  SubmitReply,
  SubmitRequest,
  decideAck,
  decideClaim,
  decideRenew,
  decideSubmit,
  expiryDecision,
} from '../domain/machine';
import { Command, StoredEvent } from '../domain/types';
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
    if (w.attempt) this.repo.upsertAttempt(w.attempt);
    if (w.ack) this.repo.insertAck(w.ack);
    for (const e of w.events) this.repo.insertEvent(e);
    return d.reply;
  }

  submit(req: SubmitRequest, requestId: string): SubmitReply {
    if (!req.idempotencyKey || !req.action) {
      throw new HttpError(400, 'bad_request', 'idempotencyKey 与 action 必填');
    }
    return this.repo.tx(() => {
      const existing = this.repo.findCommandByKey(req.idempotencyKey);
      return this.apply(decideSubmit(existing, req, this.ctx(requestId)));
    });
  }

  /**
   * 领取任务。先在同一事务内惰性结算所有已过期的租约(语义只依赖时钟,
   * 与巡检器谁先跑无关),再挑选可领取指令。
   */
  claim(gatewayId: string, limit: number, requestId: string): ClaimTask[] {
    if (!gatewayId) throw new HttpError(400, 'bad_request', 'gatewayId 必填');
    return this.repo.tx(() => {
      const ctx = this.ctx(requestId);
      this.settleExpired(ctx);
      const claimable = this.repo.listClaimable(ctx.now, Math.max(1, Math.min(limit, 100)));
      return claimable.map((c) => this.apply(decideClaim(c, gatewayId, ctx)));
    });
  }

  renew(leaseId: string, gatewayId: string, requestId: string): RenewReply {
    return this.repo.tx(() => {
      const attempt = this.repo.findAttemptByLease(leaseId);
      if (!attempt) throw new HttpError(404, 'lease_not_found', `未知租约 ${leaseId}`);
      const command = this.mustCommand(attempt.commandId);
      if (attempt.gatewayId !== gatewayId) {
        throw new HttpError(403, 'gateway_mismatch', '租约不属于该网关');
      }
      return this.apply(decideRenew(command, attempt, this.ctx(requestId)));
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
      const existingAck = this.repo.findAckById(req.ackId);
      return this.apply(decideAck(command, attempt, req, existingAck, this.ctx(requestId)));
    });
  }

  /** 巡检器:结算所有过期租约(与领取/续约/确认路径共享同一判定函数) */
  expireDue(causedBy = 'reaper'): number {
    return this.repo.tx(() => {
      const n = this.settleExpired(this.ctx(causedBy));
      return n;
    });
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
    return (
      this.repo.findCommandById(idOrKey) ?? this.repo.findCommandByKey(idOrKey)
    );
  }

  listEvents(commandId?: string): StoredEvent[] {
    return this.repo.listEvents(commandId);
  }

  private mustCommand(id: string): Command {
    const c = this.repo.findCommandById(id);
    if (!c) throw new HttpError(500, 'corrupt_state', `指令 ${id} 不存在`);
    return c;
  }
}
