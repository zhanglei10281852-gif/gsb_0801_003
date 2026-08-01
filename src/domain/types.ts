export type CommandStatus = "PENDING" | "CLAIMED" | "SUCCEEDED" | "FAILED";

export interface CommandPayload {
  deviceId: string;
  action: string;
  params?: Record<string, unknown>;
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

export interface ExpireResult {
  commandId: string;
  oldLeaseId: string;
  attempt: number;
  failed: boolean;
  reason: string | null;
}
