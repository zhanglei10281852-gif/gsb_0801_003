/**
 * 领域类型定义。
 * 本模块不依赖 HTTP、传输协议或任何具体存储,只描述领域概念。
 */

/** 指令(业务请求)的生命周期状态 */
export type CommandStatus = 'PENDING' | 'DISPATCHED' | 'SUCCEEDED' | 'FAILED';

/** 单次投递(租约)的生命周期状态 */
export type AttemptStatus = 'ACTIVE' | 'ACKED' | 'EXPIRED';

/**
 * 产线所有权(带租约的 fencing token)。
 * 同一产线可挂多台冗余网关,但任意时刻只有一个属主。
 * generation 是单调递增的代际号:每次接管(含旧属主失联后重新获取)都 +1,
 * 旧代际的网关在网络分区恢复后不得再领取任务或推进状态。
 */
export interface Ownership {
  lineId: string;
  ownerGatewayId: string;
  generation: number;
  leaseExpiresAt: number;
  updatedAt: number;
}

/**
 * 一条设备指令。
 * executionToken 是面向设备的稳定执行身份:创建时生成,终身不变;
 * 无论重试多少次、被哪个网关、以哪一代际重新领取,设备看到的都是同一个
 * executionToken,设备侧应据此去重,避免同一业务请求被执行两次。
 */
export interface Command {
  id: string;
  idempotencyKey: string;
  /** 产线标识:决定由哪条产线的属主网关投递 */
  lineId: string;
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

/**
 * 一次投递尝试(租约)。leaseId 是网关续约/回传确认的凭据;
 * generation 记录本次投递颁发时的所有权代际,供审计还原接管前后关系。
 */
export interface Attempt {
  id: string;
  commandId: string;
  attemptNo: number;
  gatewayId: string;
  generation: number;
  leaseId: string;
  status: AttemptStatus;
  leasedAt: number;
  leaseExpiresAt: number;
  renewedCount: number;
}

/**
 * 设备确认记录。ackId 由设备/网关侧生成且唯一,用于去重重复确认。
 * applied=false 表示该确认被持久化但未生效(迟到/乱序/重复/被 fencing),ignoreReason 说明原因。
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

/** 因果事件类型:每一次状态变化(含所有权变迁与 fencing 拒绝)都有事件 */
export type EventType =
  | 'COMMAND_ACCEPTED'            // 业务请求首次被接受(持久化)
  | 'SUBMISSION_DEDUPED'          // 相同幂等键的重复提交被去重
  | 'OWNERSHIP_ACQUIRED'          // 网关获得产线所有权(代际起始)
  | 'OWNERSHIP_RENEWED'           // 属主心跳续约
  | 'OWNERSHIP_TAKEN_OVER'        // 所有权易主(代际 +1,含旧属主失联后被接管)
  | 'OWNERSHIP_HEARTBEAT_REJECTED'// 非属主网关心跳争抢被拒(双网关争抢留痕)
  | 'CLAIM_FENCED'                // 旧代际/非属主领取被拒
  | 'RENEW_FENCED'                // 旧代际/非属主续约被拒
  | 'ACK_FENCED'                  // 旧代际/非属主确认被拒(确认仍持久化留痕)
  | 'ATTEMPT_ABORTED'             // 接管时在途投递被中止(将由新属主重投)
  | 'DELIVERY_STARTED'            // 任务被领取,租约生效
  | 'LEASE_RENEWED'               // 租约续约
  | 'RENEW_REJECTED'              // 续约被拒绝(租约已过期/状态不符)
  | 'DEVICE_ACK_APPLIED'          // 设备确认已持久化并生效
  | 'DEVICE_ACK_IGNORED'          // 设备确认已持久化但被忽略(带原因)
  | 'DEVICE_ACK_DUPLICATE'        // 相同 ackId 的重复确认被去重
  | 'COMMAND_SUCCEEDED'           // 指令被标记成功(仅由已持久化的确认触发)
  | 'LEASE_EXPIRED'               // 租约超期(网关失联)
  | 'RETRY_SCHEDULED'             // 安排重试(沿用稳定执行身份)
  | 'COMMAND_FAILED';             // 重试耗尽,指令终结失败

/**
 * 因果事件。seq 由存储层分配,提供全局单调的因果顺序。
 * 产线维度事件(所有权/CLAIM_FENCED)commandId 为 null、lineId 必填;
 * 指令维度事件同时携带 lineId,使产线视角能还原完整因果。
 */
export interface DomainEvent {
  commandId: string | null;
  lineId: string | null;
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
