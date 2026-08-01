/**
 * 领域类型定义。
 * 本模块不依赖 HTTP、传输协议或任何具体存储，只描述领域概念。
 */

/** 指令(业务请求)的生命周期状态 */
export type CommandStatus = 'PENDING' | 'DISPATCHED' | 'SUCCEEDED' | 'FAILED';

/** 单次投递(租约)的生命周期状态 */
export type AttemptStatus = 'ACTIVE' | 'ACKED' | 'EXPIRED';

/**
 * 一条设备指令。
 * executionToken 是面向设备的稳定执行身份:创建时生成,终身不变;
 * 无论重试多少次、被哪个网关重新领取,设备看到的都是同一个 executionToken,
 * 设备侧应据此去重,避免同一业务请求被执行两次。
 */
export interface Command {
  id: string;
  idempotencyKey: string;
  action: string;
  params: unknown;
  status: CommandStatus;
  executionToken: string;
  attemptCount: number;
  activeAttemptId: string | null;
  /** 下一次允许被领取的最早时间(退避),毫秒时间戳 */
  nextAttemptNotBefore: number;
  createdAt: number;
  updatedAt: number;
}

/** 一次投递尝试(租约)。leaseId 是网关续约/回传确认的凭据。 */
export interface Attempt {
  id: string;
  commandId: string;
  attemptNo: number;
  gatewayId: string;
  leaseId: string;
  status: AttemptStatus;
  leasedAt: number;
  leaseExpiresAt: number;
  renewedCount: number;
}

/**
 * 设备确认记录。ackId 由设备/网关侧生成且唯一,用于去重重复确认。
 * applied=false 表示该确认被持久化但未生效(迟到/乱序/重复),ignoreReason 说明原因。
 */
export interface AckRecord {
  id: string;
  commandId: string;
  attemptId: string;
  result: unknown;
  applied: boolean;
  ignoreReason: string | null;
  receivedAt: number;
}

/** 因果事件类型:每一次状态变化都有对应事件,而不是只保存最终值 */
export type EventType =
  | 'COMMAND_ACCEPTED'      // 业务请求首次被接受(持久化)
  | 'SUBMISSION_DEDUPED'    // 相同幂等键的重复提交被去重
  | 'DELIVERY_STARTED'      // 任务被领取,租约生效
  | 'LEASE_RENEWED'         // 租约续约
  | 'RENEW_REJECTED'        // 续约被拒绝(迟到/状态不符)
  | 'DEVICE_ACK_APPLIED'    // 设备确认已持久化并生效
  | 'DEVICE_ACK_IGNORED'    // 设备确认已持久化但被忽略(带原因)
  | 'DEVICE_ACK_DUPLICATE'  // 相同 ackId 的重复确认被去重
  | 'COMMAND_SUCCEEDED'     // 指令被标记成功(仅由已持久化的确认触发)
  | 'LEASE_EXPIRED'         // 租约超期(网关失联)
  | 'RETRY_SCHEDULED'       // 安排重试(沿用稳定执行身份)
  | 'COMMAND_FAILED';       // 重试耗尽,指令终结失败

/** 因果事件。seq 由存储层分配,提供全局单调的因果顺序。 */
export interface DomainEvent {
  commandId: string;
  attemptId: string | null;
  type: EventType;
  data: Record<string, unknown>;
  /** 触发来源:请求 id / 巡检器 / 另一条事件 */
  causedBy: string;
  at: number;
}

export interface StoredEvent extends DomainEvent {
  seq: number;
}
