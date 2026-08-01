export type CommandState =
  | "PENDING"
  | "CLAIMED"
  | "DELIVERED"
  | "TIMED_OUT"
  | "FAILED";

export type CommandType = "CALIBRATE" | "SWITCH_RECIPE" | "RESET" | string;

export interface CommandPayload {
  type: CommandType;
  params?: Record<string, unknown>;
}

export type CausedBy =
  | "SUBMIT"
  | "CLAIM"
  | "RENEW"
  | "ACK"
  | "LEASE_EXPIRY"
  | "RETRY"
  | "TIMEOUT";

export interface DomainEvent {
  eventId: string;
  commandId: string;
  version: number;
  type: string;
  data: Record<string, unknown>;
  causedBy: CausedBy;
  causationId?: string;
  occurredAt: number;
  recordedAt: number;
}

export interface CommandSnapshot {
  commandId: string;
  idempotencyKey: string;
  deviceId: string;
  payload: CommandPayload;
  state: CommandState;
  version: number;
  attempt: number;
  generation: number;
  gatewayId?: string;
  leaseId?: string;
  leaseExpiresAt?: number;
  deliveredAt?: number;
  terminalReason?: string;
  createdAt: number;
  updatedAt: number;
}

export interface SubmitCommandInput {
  commandId: string;
  idempotencyKey: string;
  deviceId: string;
  payload: CommandPayload;
  submittedAt: number;
}

export interface ClaimInput {
  gatewayId: string;
  leaseId: string;
  leaseDurationMs: number;
  claimedAt: number;
  maxAttempts: number;
}

export interface RenewInput {
  gatewayId: string;
  leaseId: string;
  generation: number;
  leaseDurationMs: number;
  renewedAt: number;
}

export interface AckInput {
  gatewayId: string;
  leaseId: string;
  generation: number;
  ackCode: string;
  ackPayload?: Record<string, unknown>;
  receivedAt: number;
}

export interface ExpireInput {
  now: number;
  maxAttempts: number;
  maxAgeMs?: number;
}

export interface Decision<T = Record<string, unknown>> {
  accepted: boolean;
  events: DomainEvent[];
  reason?: string;
  result?: T;
}
