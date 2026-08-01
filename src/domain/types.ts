export type CommandStatus =
  | "PENDING"
  | "CLAIMED"
  | "SUCCEEDED"
  | "FAILED"
  | "CANCELLED"
  | "SUPERSEDED";

export interface CommandPayload {
  deviceId: string;
  action: string;
  params?: Record<string, unknown>;
  replacesCommandId?: string;
}

export interface Command {
  commandId: string;
  idempotencyKey: string;
  payload: CommandPayload;
  status: CommandStatus;
  currentLeaseId: string | null;
  attempt: number;
  maxAttempts: number;
  leaseExpiresAt: number | null;
  gatewayId: string | null;
  confirmationCode: string | null;
  deviceTimestamp: number | null;
  failureReason: string | null;
  cancelReason: string | null;
  supersededByCommandId: string | null;
  createdAt: number;
  updatedAt: number;
}

export type EventType =
  | "CommandSubmitted"
  | "CommandClaimed"
  | "LeaseRenewed"
  | "DeliveryReported"
  | "DeviceConfirmed"
  | "CommandSucceeded"
  | "LeaseExpired"
  | "CommandReclaimed"
  | "CommandFailed"
  | "CommandCancelled"
  | "CommandSuperseded"
  | "LeaseOperationRejected"
  | "StaleMessageRejected";

export interface CommandEvent {
  eventId: string;
  commandId: string;
  eventType: EventType;
  leaseId: string | null;
  attempt: number | null;
  gatewayId: string | null;
  payload: Record<string, unknown> | null;
  causedBy: string;
  timestamp: number;
}

export interface SubmitCommandInput {
  idempotencyKey: string;
  payload: CommandPayload;
  maxAttempts?: number;
}

export interface ClaimInput {
  gatewayId: string;
  leaseDurationMs: number;
  deviceId?: string;
}

export interface RenewInput {
  commandId: string;
  leaseId: string;
  gatewayId: string;
  leaseDurationMs: number;
}

export interface ReportDeliveryInput {
  commandId: string;
  leaseId: string;
  gatewayId: string;
  deviceMessage?: string;
}

export interface ConfirmInput {
  commandId: string;
  leaseId: string;
  gatewayId: string;
  confirmationCode: string;
  deviceTimestamp: number;
}

export interface WithdrawInput {
  commandId: string;
  reason: string;
  requestedBy?: string;
}

export interface SupersedeInput {
  oldCommandId: string;
  idempotencyKey: string;
  payload: CommandPayload;
  reason: string;
  requestedBy?: string;
  maxAttempts?: number;
}

export interface ExpireResult {
  commandId: string;
  oldLeaseId: string;
  attempt: number;
  failed: boolean;
  reason: string | null;
}
