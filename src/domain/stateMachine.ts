import {
  Command,
  CommandEvent,
  CommandStatus,
  ConfirmInput,
  ClaimInput,
  EventType,
  ExpireResult,
  RenewInput,
  ReportDeliveryInput,
  SubmitCommandInput,
  SupersedeInput,
  WithdrawInput,
} from "./types.js";
import {
  CommandAlreadyCompletedError,
  InvalidLeaseError,
  InvalidStateTransitionError,
} from "./errors.js";

export interface Decision {
  command: Command;
  events: CommandEvent[];
}

export interface Clock {
  now(): number;
}

export interface IdGenerator {
  newId(): string;
}

function createEvent(
  idGen: IdGenerator,
  clock: Clock,
  commandId: string,
  eventType: EventType,
  causedBy: string,
  opts: {
    leaseId?: string | null;
    attempt?: number | null;
    gatewayId?: string | null;
    payload?: Record<string, unknown> | null;
  } = {},
): CommandEvent {
  return {
    eventId: idGen.newId(),
    commandId,
    eventType,
    leaseId: opts.leaseId ?? null,
    attempt: opts.attempt ?? null,
    gatewayId: opts.gatewayId ?? null,
    payload: opts.payload ?? null,
    causedBy,
    timestamp: clock.now(),
  };
}

export function createPendingCommand(
  input: SubmitCommandInput,
  existing: Command | null,
  idGen: IdGenerator,
  clock: Clock,
): Decision {
  if (existing) {
    return { command: existing, events: [] };
  }

  const now = clock.now();
  const commandId = idGen.newId();
  const command: Command = {
    commandId,
    idempotencyKey: input.idempotencyKey,
    payload: input.payload,
    status: "PENDING",
    currentLeaseId: null,
    attempt: 0,
    maxAttempts: input.maxAttempts ?? 3,
    leaseExpiresAt: null,
    gatewayId: null,
    confirmationCode: null,
    deviceTimestamp: null,
    failureReason: null,
    cancelReason: null,
    supersededByCommandId: null,
    createdAt: now,
    updatedAt: now,
  };

  const event = createEvent(
    idGen,
    clock,
    commandId,
    "CommandSubmitted",
    "submit",
    {
      payload: {
        idempotencyKey: input.idempotencyKey,
        deviceId: input.payload.deviceId,
        action: input.payload.action,
        maxAttempts: command.maxAttempts,
        replacesCommandId: input.payload.replacesCommandId ?? null,
      },
    },
  );

  return { command, events: [event] };
}

export function claimCommand(
  command: Command,
  input: ClaimInput,
  idGen: IdGenerator,
  clock: Clock,
): Decision {
  if (
    command.status === "SUCCEEDED" ||
    command.status === "FAILED" ||
    command.status === "CANCELLED" ||
    command.status === "SUPERSEDED"
  ) {
    throw new InvalidStateTransitionError(
      command.commandId,
      command.status,
      "claim",
    );
  }

  const now = clock.now();
  const leaseId = idGen.newId();
  const attempt = command.attempt + 1;
  const leaseExpiresAt = now + input.leaseDurationMs;

  let eventType: EventType;
  let causedBy: string;
  let eventPayload: Record<string, unknown>;

  if (command.status === "CLAIMED") {
    const stillValid =
      command.leaseExpiresAt !== null && command.leaseExpiresAt > now;
    if (stillValid) {
      throw new InvalidLeaseError(
        command.commandId,
        command.currentLeaseId,
        leaseId,
        "lease is still active; cannot claim while held",
      );
    }
    eventType = "CommandReclaimed";
    causedBy = "claim-after-expiry";
    eventPayload = {
      oldLeaseId: command.currentLeaseId,
      oldGatewayId: command.gatewayId,
      oldGeneration: command.attempt,
      newLeaseId: leaseId,
      newGeneration: attempt,
    };
  } else {
    eventType = "CommandClaimed";
    causedBy = "claim";
    eventPayload = {
      leaseId,
      generation: attempt,
      leaseDurationMs: input.leaseDurationMs,
    };
  }

  if (attempt > command.maxAttempts) {
    const failedCommand: Command = {
      ...command,
      status: "FAILED",
      currentLeaseId: null,
      leaseExpiresAt: null,
      gatewayId: null,
      failureReason: `max attempts (${command.maxAttempts}) exhausted`,
      cancelReason: null,
      supersededByCommandId: null,
      updatedAt: now,
    };
    const failedEvent = createEvent(
      idGen,
      clock,
      command.commandId,
      "CommandFailed",
      "claim-exceeds-max-attempts",
      {
        attempt,
        gatewayId: input.gatewayId,
        payload: { reason: failedCommand.failureReason },
      },
    );
    return { command: failedCommand, events: [failedEvent] };
  }

  const updated: Command = {
    ...command,
    status: "CLAIMED",
    currentLeaseId: leaseId,
    attempt,
    leaseExpiresAt,
    gatewayId: input.gatewayId,
    failureReason: null,
    updatedAt: now,
  };

  const event = createEvent(
    idGen,
    clock,
    command.commandId,
    eventType,
    causedBy,
    {
      leaseId,
      attempt,
      gatewayId: input.gatewayId,
      payload: eventPayload,
    },
  );

  return { command: updated, events: [event] };
}

export function renewLease(
  command: Command,
  input: RenewInput,
  idGen: IdGenerator,
  clock: Clock,
): Decision {
  if (command.status !== "CLAIMED") {
    throw new InvalidStateTransitionError(
      command.commandId,
      command.status,
      "renew",
    );
  }

  if (command.currentLeaseId !== input.leaseId) {
    throw new InvalidLeaseError(
      command.commandId,
      command.currentLeaseId,
      input.leaseId,
      "leaseId does not match current lease",
    );
  }

  const now = clock.now();
  if (command.leaseExpiresAt !== null && command.leaseExpiresAt <= now) {
    throw new InvalidLeaseError(
      command.commandId,
      command.currentLeaseId,
      input.leaseId,
      "lease already expired",
    );
  }

  const leaseExpiresAt = now + input.leaseDurationMs;
  const updated: Command = {
    ...command,
    leaseExpiresAt,
    updatedAt: now,
  };

  const event = createEvent(
    idGen,
    clock,
    command.commandId,
    "LeaseRenewed",
    "renew",
    {
      leaseId: input.leaseId,
      attempt: command.attempt,
      gatewayId: input.gatewayId,
      payload: { leaseDurationMs: input.leaseDurationMs, leaseExpiresAt },
    },
  );

  return { command: updated, events: [event] };
}

export function reportDelivery(
  command: Command,
  input: ReportDeliveryInput,
  idGen: IdGenerator,
  clock: Clock,
): Decision {
  if (command.status !== "CLAIMED") {
    throw new InvalidStateTransitionError(
      command.commandId,
      command.status,
      "reportDelivery",
    );
  }

  if (command.currentLeaseId !== input.leaseId) {
    throw new InvalidLeaseError(
      command.commandId,
      command.currentLeaseId,
      input.leaseId,
      "leaseId does not match current lease",
    );
  }

  const now = clock.now();
  const updated: Command = { ...command, updatedAt: now };

  const event = createEvent(
    idGen,
    clock,
    command.commandId,
    "DeliveryReported",
    "report-delivery",
    {
      leaseId: input.leaseId,
      attempt: command.attempt,
      gatewayId: input.gatewayId,
      payload: input.deviceMessage
        ? { deviceMessage: input.deviceMessage }
        : null,
    },
  );

  return { command: updated, events: [event] };
}

export function confirmCommand(
  command: Command,
  input: ConfirmInput,
  idGen: IdGenerator,
  clock: Clock,
): Decision {
  const now = clock.now();

  if (command.status === "SUCCEEDED") {
    const staleEvent = createEvent(
      idGen,
      clock,
      command.commandId,
      "StaleMessageRejected",
      "duplicate-confirm-after-success",
      {
        leaseId: input.leaseId,
        attempt: command.attempt,
        gatewayId: input.gatewayId,
        payload: {
          reason: "command already succeeded",
          confirmationCode: input.confirmationCode,
        },
      },
    );
    return { command, events: [staleEvent] };
  }

  if (command.status === "FAILED") {
    throw new InvalidStateTransitionError(
      command.commandId,
      command.status,
      "confirm",
    );
  }

  if (
    command.status === "PENDING" ||
    command.status === "CANCELLED" ||
    command.status === "SUPERSEDED"
  ) {
    const reason =
      command.status === "CANCELLED"
        ? "command was withdrawn; late confirmation rejected"
        : command.status === "SUPERSEDED"
          ? "command was superseded; late confirmation rejected"
          : "command is PENDING (previous lease expired); late confirmation rejected";
    const staleEvent = createEvent(
      idGen,
      clock,
      command.commandId,
      "StaleMessageRejected",
      "confirm-after-terminal",
      {
        leaseId: input.leaseId,
        gatewayId: input.gatewayId,
        payload: {
          reason,
          finalStatus: command.status,
        },
      },
    );
    return { command, events: [staleEvent] };
  }

  if (command.currentLeaseId !== input.leaseId) {
    const staleEvent = createEvent(
      idGen,
      clock,
      command.commandId,
      "StaleMessageRejected",
      "confirm-stale-lease",
      {
        leaseId: input.leaseId,
        attempt: command.attempt,
        gatewayId: input.gatewayId,
        payload: {
          reason: "leaseId does not match current lease (possibly reclaimed)",
          currentLeaseId: command.currentLeaseId,
        },
      },
    );
    return { command, events: [staleEvent] };
  }

  if (command.leaseExpiresAt !== null && command.leaseExpiresAt <= now) {
    const staleEvent = createEvent(
      idGen,
      clock,
      command.commandId,
      "StaleMessageRejected",
      "confirm-lease-expired",
      {
        leaseId: input.leaseId,
        attempt: command.attempt,
        gatewayId: input.gatewayId,
        payload: {
          reason:
            "lease already expired; late confirmation rejected to prevent zombie resurrection",
          leaseExpiresAt: command.leaseExpiresAt,
        },
      },
    );
    return { command, events: [staleEvent] };
  }

  const succeeded: Command = {
    ...command,
    status: "SUCCEEDED",
    confirmationCode: input.confirmationCode,
    deviceTimestamp: input.deviceTimestamp,
    leaseExpiresAt: null,
    updatedAt: now,
  };

  const confirmedEvent = createEvent(
    idGen,
    clock,
    command.commandId,
    "DeviceConfirmed",
    "device-confirmation",
    {
      leaseId: input.leaseId,
      attempt: command.attempt,
      gatewayId: input.gatewayId,
      payload: {
        confirmationCode: input.confirmationCode,
        deviceTimestamp: input.deviceTimestamp,
      },
    },
  );

  const succeededEvent = createEvent(
    idGen,
    clock,
    command.commandId,
    "CommandSucceeded",
    "confirm",
    {
      leaseId: input.leaseId,
      attempt: command.attempt,
      gatewayId: input.gatewayId,
      payload: { confirmationCode: input.confirmationCode },
    },
  );

  return { command: succeeded, events: [confirmedEvent, succeededEvent] };
}

export function expireLease(
  command: Command,
  idGen: IdGenerator,
  clock: Clock,
): { decision: Decision; result: ExpireResult } | null {
  if (command.status !== "CLAIMED") {
    return null;
  }

  const now = clock.now();
  if (command.leaseExpiresAt === null || command.leaseExpiresAt > now) {
    return null;
  }

  const oldLeaseId = command.currentLeaseId!;
  const oldGatewayId = command.gatewayId;
  const attemptsExhausted = command.attempt >= command.maxAttempts;

  let updatedStatus: CommandStatus;
  let failureReason: string | null = null;

  if (attemptsExhausted) {
    updatedStatus = "FAILED";
    failureReason = `lease expired after ${command.attempt} attempt(s); max attempts exhausted`;
  } else {
    updatedStatus = "PENDING";
  }

  const updated: Command = {
    ...command,
    status: updatedStatus,
    currentLeaseId: null,
    leaseExpiresAt: null,
    gatewayId: null,
    failureReason,
    updatedAt: now,
  };

  const expiredEvent = createEvent(
    idGen,
    clock,
    command.commandId,
    "LeaseExpired",
    "lease-timeout",
    {
      leaseId: oldLeaseId,
      attempt: command.attempt,
      gatewayId: oldGatewayId,
      payload: {
        leaseExpiresAt: command.leaseExpiresAt,
        expiredAt: now,
        attemptsExhausted,
      },
    },
  );

  const events: CommandEvent[] = [expiredEvent];

  if (attemptsExhausted) {
    const failedEvent = createEvent(
      idGen,
      clock,
      command.commandId,
      "CommandFailed",
      "lease-timeout-exhausted",
      {
        leaseId: oldLeaseId,
        attempt: command.attempt,
        gatewayId: oldGatewayId,
        payload: { reason: failureReason },
      },
    );
    events.push(failedEvent);
  }

  const result: ExpireResult = {
    commandId: command.commandId,
    oldLeaseId,
    attempt: command.attempt,
    failed: attemptsExhausted,
    reason: failureReason,
  };

  return { decision: { command: updated, events }, result };
}

export function isConfirmStale(
  command: Command,
  leaseId: string,
  now: number,
): boolean {
  if (command.status === "SUCCEEDED") return true;
  if (command.status !== "CLAIMED") return true;
  if (command.currentLeaseId !== leaseId) return true;
  if (command.leaseExpiresAt !== null && command.leaseExpiresAt <= now)
    return true;
  return false;
}

export function withdrawCommand(
  command: Command,
  input: WithdrawInput,
  idGen: IdGenerator,
  clock: Clock,
): Decision {
  if (command.status === "SUCCEEDED") {
    throw new CommandAlreadyCompletedError(
      command.commandId,
      "SUCCEEDED",
      "withdraw",
    );
  }
  if (
    command.status === "FAILED" ||
    command.status === "CANCELLED" ||
    command.status === "SUPERSEDED"
  ) {
    throw new CommandAlreadyCompletedError(
      command.commandId,
      command.status,
      "withdraw",
    );
  }

  const now = clock.now();
  const oldLeaseId = command.currentLeaseId;
  const oldGatewayId = command.gatewayId;

  const cancelled: Command = {
    ...command,
    status: "CANCELLED",
    currentLeaseId: null,
    leaseExpiresAt: null,
    gatewayId: null,
    cancelReason: input.reason,
    updatedAt: now,
  };

  const event = createEvent(
    idGen,
    clock,
    command.commandId,
    "CommandCancelled",
    "withdraw",
    {
      leaseId: oldLeaseId,
      attempt: command.attempt,
      gatewayId: oldGatewayId,
      payload: {
        reason: input.reason,
        requestedBy: input.requestedBy ?? null,
        previousStatus: command.status,
        invalidatedLeaseId: oldLeaseId,
        invalidatedGatewayId: oldGatewayId,
      },
    },
  );

  return { command: cancelled, events: [event] };
}

export interface SupersedeDecision {
  oldCommand: Command;
  newCommand: Command;
  events: CommandEvent[];
}

export function supersedeCommand(
  oldCommand: Command,
  input: SupersedeInput,
  idGen: IdGenerator,
  clock: Clock,
): SupersedeDecision {
  if (oldCommand.status === "SUCCEEDED") {
    throw new CommandAlreadyCompletedError(
      oldCommand.commandId,
      "SUCCEEDED",
      "supersede",
    );
  }
  if (
    oldCommand.status === "FAILED" ||
    oldCommand.status === "CANCELLED" ||
    oldCommand.status === "SUPERSEDED"
  ) {
    throw new CommandAlreadyCompletedError(
      oldCommand.commandId,
      oldCommand.status,
      "supersede",
    );
  }

  const now = clock.now();
  const newCommandId = idGen.newId();
  const oldLeaseId = oldCommand.currentLeaseId;
  const oldGatewayId = oldCommand.gatewayId;

  const superseded: Command = {
    ...oldCommand,
    status: "SUPERSEDED",
    currentLeaseId: null,
    leaseExpiresAt: null,
    gatewayId: null,
    supersededByCommandId: newCommandId,
    updatedAt: now,
  };

  const newPayload = {
    ...input.payload,
    replacesCommandId: oldCommand.commandId,
  };

  const newCommand: Command = {
    commandId: newCommandId,
    idempotencyKey: input.idempotencyKey,
    payload: newPayload,
    status: "PENDING",
    currentLeaseId: null,
    attempt: 0,
    maxAttempts: input.maxAttempts ?? oldCommand.maxAttempts,
    leaseExpiresAt: null,
    gatewayId: null,
    confirmationCode: null,
    deviceTimestamp: null,
    failureReason: null,
    cancelReason: null,
    supersededByCommandId: null,
    createdAt: now,
    updatedAt: now,
  };

  const supersededEvent = createEvent(
    idGen,
    clock,
    oldCommand.commandId,
    "CommandSuperseded",
    "supersede",
    {
      leaseId: oldLeaseId,
      attempt: oldCommand.attempt,
      gatewayId: oldGatewayId,
      payload: {
        reason: input.reason,
        requestedBy: input.requestedBy ?? null,
        newCommandId,
        previousStatus: oldCommand.status,
        invalidatedLeaseId: oldLeaseId,
        invalidatedGatewayId: oldGatewayId,
      },
    },
  );

  const submittedEvent = createEvent(
    idGen,
    clock,
    newCommandId,
    "CommandSubmitted",
    "supersede-replacement",
    {
      payload: {
        idempotencyKey: input.idempotencyKey,
        deviceId: input.payload.deviceId,
        action: input.payload.action,
        maxAttempts: newCommand.maxAttempts,
        replacesCommandId: oldCommand.commandId,
      },
    },
  );

  return {
    oldCommand: superseded,
    newCommand,
    events: [supersededEvent, submittedEvent],
  };
}

export interface RejectedOperationInput {
  operation: "renew" | "report-delivery" | "claim-contention";
  leaseId: string;
  gatewayId: string;
  reason: string;
}

export function recordRejectedLeaseOperation(
  command: Command,
  input: RejectedOperationInput,
  idGen: IdGenerator,
  clock: Clock,
): CommandEvent {
  return createEvent(
    idGen,
    clock,
    command.commandId,
    "LeaseOperationRejected",
    `rejected-${input.operation}`,
    {
      leaseId: input.leaseId,
      attempt: command.attempt,
      gatewayId: input.gatewayId,
      payload: {
        operation: input.operation,
        reason: input.reason,
        currentLeaseId: command.currentLeaseId,
        currentGatewayId: command.gatewayId,
        currentGeneration: command.attempt,
        rejectedLeaseId: input.leaseId,
        rejectedGatewayId: input.gatewayId,
      },
    },
  );
}
