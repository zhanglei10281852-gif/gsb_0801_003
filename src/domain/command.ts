import { randomUUID } from "node:crypto";
import type {
  AckInput,
  CancelInput,
  ClaimInput,
  CommandSnapshot,
  Decision,
  DomainEvent,
  ExpireInput,
  RenewInput,
  ReplaceInput,
  SubmitCommandInput,
} from "./types.js";

export interface EventFactory {
  nextId: () => string;
  now: () => number;
}

export const systemEventFactory: EventFactory = {
  nextId: () => randomUUID(),
  now: () => Date.now(),
};

export function createEventFactory(prefix = "evt"): EventFactory {
  let n = 0;
  return {
    nextId: () => `${prefix}-${++n}`,
    now: () => 0,
  };
}

export function initialState(): CommandSnapshot | undefined {
  return undefined;
}

export function apply(
  state: CommandSnapshot | undefined,
  event: DomainEvent,
): CommandSnapshot {
  switch (event.type) {
    case "CommandSubmitted":
      return {
        commandId: event.commandId,
        idempotencyKey: event.data.idempotencyKey as string,
        deviceId: event.data.deviceId as string,
        payload: event.data.payload as CommandSnapshot["payload"],
        state: "PENDING",
        version: event.version,
        attempt: 0,
        generation: 0,
        supersedesCommandId:
          (event.data.supersedesCommandId as string | undefined) ?? undefined,
        createdAt: event.occurredAt,
        updatedAt: event.recordedAt,
      };
    case "CommandClaimed":
      if (!state) throw new Error("invalid event stream");
      return {
        ...state,
        state: "CLAIMED",
        version: event.version,
        attempt: event.data.attempt as number,
        generation: event.data.generation as number,
        gatewayId: event.data.gatewayId as string,
        leaseId: event.data.leaseId as string,
        leaseExpiresAt: event.data.leaseExpiresAt as number,
        updatedAt: event.recordedAt,
      };
    case "LeaseRenewed":
      if (!state) throw new Error("invalid event stream");
      return {
        ...state,
        version: event.version,
        leaseExpiresAt: event.data.leaseExpiresAt as number,
        updatedAt: event.recordedAt,
      };
    case "LeaseExpired":
      if (!state) throw new Error("invalid event stream");
      return {
        ...state,
        state: "PENDING",
        version: event.version,
        attempt: event.data.attempt as number,
        gatewayId: undefined,
        leaseId: undefined,
        leaseExpiresAt: undefined,
        updatedAt: event.recordedAt,
      };
    case "DeviceAckRecorded":
      if (!state) throw new Error("invalid event stream");
      return {
        ...state,
        state: "DELIVERED",
        version: event.version,
        deliveredAt: event.occurredAt,
        terminalReason: event.data.ackCode as string,
        updatedAt: event.recordedAt,
      };
    case "DeviceNackRecorded":
      if (!state) throw new Error("invalid event stream");
      return {
        ...state,
        state: "FAILED",
        version: event.version,
        deliveredAt: event.occurredAt,
        terminalReason: event.data.ackCode as string,
        updatedAt: event.recordedAt,
      };
    case "CommandTimedOut":
      if (!state) throw new Error("invalid event stream");
      return {
        ...state,
        state: "TIMED_OUT",
        version: event.version,
        gatewayId: undefined,
        leaseId: undefined,
        leaseExpiresAt: undefined,
        terminalReason: event.data.reason as string,
        updatedAt: event.recordedAt,
      };
    case "CommandCancelled":
      if (!state) throw new Error("invalid event stream");
      return {
        ...state,
        state: "CANCELLED",
        version: event.version,
        gatewayId: undefined,
        leaseId: undefined,
        leaseExpiresAt: undefined,
        cancelledAt: event.occurredAt,
        cancelledBy: event.data.requestedBy as string,
        cancelReason: event.data.reason as string,
        terminalReason: event.data.reason as string,
        updatedAt: event.recordedAt,
      };
    case "CommandSuperseded":
      if (!state) throw new Error("invalid event stream");
      return {
        ...state,
        state: "CANCELLED",
        version: event.version,
        gatewayId: undefined,
        leaseId: undefined,
        leaseExpiresAt: undefined,
        cancelledAt: event.occurredAt,
        cancelledBy: event.data.requestedBy as string,
        cancelReason: event.data.reason as string,
        supersededByCommandId: event.data.replacementCommandId as string,
        terminalReason: `SUPERSEDED_BY_${event.data.replacementCommandId as string}`,
        updatedAt: event.recordedAt,
      };
    default:
      if (!state) throw new Error("invalid event stream");
      return { ...state, version: event.version, updatedAt: event.recordedAt };
  }
}

export function replay(events: DomainEvent[]): CommandSnapshot | undefined {
  return events.reduce<CommandSnapshot | undefined>(
    (s, e) => apply(s, e),
    undefined,
  );
}

function makeEvent(
  state: CommandSnapshot | undefined,
  type: string,
  data: Record<string, unknown>,
  causedBy: DomainEvent["causedBy"],
  occurredAt: number,
  factory: EventFactory,
  causationId?: string,
): DomainEvent {
  return {
    eventId: factory.nextId(),
    commandId: state?.commandId ?? (data.commandId as string),
    version: (state?.version ?? 0) + 1,
    type,
    data,
    causedBy,
    causationId,
    occurredAt,
    recordedAt: factory.now(),
  };
}

function terminal(state: CommandSnapshot | undefined): boolean {
  return (
    state?.state === "DELIVERED" ||
    state?.state === "FAILED" ||
    state?.state === "TIMED_OUT" ||
    state?.state === "CANCELLED"
  );
}

export function decideSubmit(
  state: CommandSnapshot | undefined,
  input: SubmitCommandInput,
  factory: EventFactory,
): Decision {
  if (state) {
    if (state.idempotencyKey === input.idempotencyKey) {
      return {
        accepted: true,
        events: [],
        result: {
          duplicate: true,
          commandId: state.commandId,
          state: state.state,
        },
      };
    }
    return { accepted: false, events: [], reason: "IDEMPOTENCY_KEY_CONFLICT" };
  }
  const event = makeEvent(
    undefined,
    "CommandSubmitted",
    {
      commandId: input.commandId,
      idempotencyKey: input.idempotencyKey,
      deviceId: input.deviceId,
      payload: input.payload,
    },
    "SUBMIT",
    input.submittedAt,
    factory,
    input.idempotencyKey,
  );
  return {
    accepted: true,
    events: [event],
    result: { duplicate: false, commandId: input.commandId },
  };
}

export function decideClaim(
  state: CommandSnapshot | undefined,
  input: ClaimInput,
  factory: EventFactory,
): Decision {
  if (!state)
    return { accepted: false, events: [], reason: "COMMAND_NOT_FOUND" };
  if (terminal(state))
    return { accepted: false, events: [], reason: `TERMINAL_${state.state}` };

  const events: DomainEvent[] = [];
  let current = state;

  if (
    current.state === "CLAIMED" &&
    current.leaseExpiresAt! > input.claimedAt
  ) {
    return { accepted: false, events: [], reason: "LEASE_ACTIVE" };
  }

  if (
    current.state === "CLAIMED" &&
    current.leaseExpiresAt! <= input.claimedAt
  ) {
    const expired = makeEvent(
      current,
      "LeaseExpired",
      {
        attempt: current.attempt,
        generation: current.generation,
        oldLeaseId: current.leaseId,
        oldGatewayId: current.gatewayId,
      },
      "LEASE_EXPIRY",
      current.leaseExpiresAt!,
      factory,
      current.leaseId,
    );
    events.push(expired);
    current = apply(current, expired);
  }

  if (current.state !== "PENDING") {
    return {
      accepted: false,
      events,
      reason: `INVALID_STATE_${current.state}`,
    };
  }

  if (input.maxAttempts > 0 && current.attempt >= input.maxAttempts) {
    const timedOut = makeEvent(
      current,
      "CommandTimedOut",
      { reason: "MAX_ATTEMPTS_REACHED", attempt: current.attempt },
      "TIMEOUT",
      input.claimedAt,
      factory,
    );
    events.push(timedOut);
    return { accepted: false, events, reason: "MAX_ATTEMPTS_REACHED" };
  }

  const attempt = current.attempt + 1;
  const generation = current.generation + 1;
  const claimed = makeEvent(
    current,
    "CommandClaimed",
    {
      attempt,
      generation,
      previousGeneration: state.generation,
      gatewayId: input.gatewayId,
      previousGatewayId: state.gatewayId ?? null,
      leaseId: input.leaseId,
      leaseExpiresAt: input.claimedAt + input.leaseDurationMs,
    },
    "CLAIM",
    input.claimedAt,
    factory,
    input.leaseId,
  );
  events.push(claimed);
  current = apply(current, claimed);
  return { accepted: true, events, result: { snapshot: current } };
}

export function decideRenew(
  state: CommandSnapshot | undefined,
  input: RenewInput,
  factory: EventFactory,
): Decision {
  if (!state)
    return { accepted: false, events: [], reason: "COMMAND_NOT_FOUND" };
  if (terminal(state))
    return { accepted: false, events: [], reason: `TERMINAL_${state.state}` };
  if (state.state !== "CLAIMED")
    return { accepted: false, events: [], reason: "NOT_CLAIMED" };
  if (state.gatewayId !== input.gatewayId || state.leaseId !== input.leaseId) {
    return { accepted: false, events: [], reason: "LEASE_MISMATCH" };
  }
  if (input.generation !== state.generation) {
    return { accepted: false, events: [], reason: "STALE_GENERATION" };
  }
  if (state.leaseExpiresAt! <= input.renewedAt) {
    return { accepted: false, events: [], reason: "LEASE_EXPIRED" };
  }
  const event = makeEvent(
    state,
    "LeaseRenewed",
    { leaseExpiresAt: input.renewedAt + input.leaseDurationMs },
    "RENEW",
    input.renewedAt,
    factory,
    input.leaseId,
  );
  return { accepted: true, events: [event] };
}

export function decideAck(
  state: CommandSnapshot | undefined,
  input: AckInput & { success: boolean },
  factory: EventFactory,
): Decision {
  if (!state)
    return { accepted: false, events: [], reason: "COMMAND_NOT_FOUND" };
  if (terminal(state))
    return { accepted: false, events: [], reason: `TERMINAL_${state.state}` };
  if (state.state !== "CLAIMED")
    return { accepted: false, events: [], reason: "NOT_CLAIMED" };
  if (state.gatewayId !== input.gatewayId || state.leaseId !== input.leaseId) {
    return { accepted: false, events: [], reason: "LEASE_MISMATCH" };
  }
  if (input.generation !== state.generation) {
    return { accepted: false, events: [], reason: "STALE_GENERATION" };
  }
  const type = input.success ? "DeviceAckRecorded" : "DeviceNackRecorded";
  const event = makeEvent(
    state,
    type,
    {
      ackCode: input.ackCode,
      ackPayload: input.ackPayload ?? null,
      generation: input.generation,
      gatewayId: input.gatewayId,
    },
    "ACK",
    input.receivedAt,
    factory,
    input.leaseId,
  );
  return { accepted: true, events: [event] };
}

export function decideExpire(
  state: CommandSnapshot | undefined,
  input: ExpireInput,
  factory: EventFactory,
): Decision {
  if (!state)
    return { accepted: false, events: [], reason: "COMMAND_NOT_FOUND" };
  if (terminal(state))
    return { accepted: false, events: [], reason: `TERMINAL_${state.state}` };

  const events: DomainEvent[] = [];
  let current = state;

  const ageExpired =
    input.maxAgeMs !== undefined &&
    input.now - current.createdAt >= input.maxAgeMs;

  if (
    current.state === "CLAIMED" &&
    (current.leaseExpiresAt! <= input.now || ageExpired)
  ) {
    const expired = makeEvent(
      current,
      "LeaseExpired",
      {
        attempt: current.attempt,
        generation: current.generation,
        oldLeaseId: current.leaseId,
        oldGatewayId: current.gatewayId,
      },
      "LEASE_EXPIRY",
      Math.min(current.leaseExpiresAt!, input.now),
      factory,
      current.leaseId,
    );
    events.push(expired);
    current = apply(current, expired);
  }

  if (current.state === "PENDING") {
    const attemptsExhausted =
      input.maxAttempts > 0 && current.attempt >= input.maxAttempts;
    if (attemptsExhausted || ageExpired) {
      const reason = ageExpired ? "MAX_AGE_REACHED" : "MAX_ATTEMPTS_REACHED";
      const timedOut = makeEvent(
        current,
        "CommandTimedOut",
        { reason, attempt: current.attempt },
        "TIMEOUT",
        input.now,
        factory,
      );
      events.push(timedOut);
      current = apply(current, timedOut);
    }
  }

  if (events.length === 0)
    return { accepted: false, events: [], reason: "NOT_DUE" };
  return { accepted: true, events, result: { state: current.state } };
}

export function decideCancel(
  state: CommandSnapshot | undefined,
  input: CancelInput,
  factory: EventFactory,
): Decision {
  if (!state)
    return { accepted: false, events: [], reason: "COMMAND_NOT_FOUND" };
  if (state.state === "DELIVERED")
    return { accepted: false, events: [], reason: "ALREADY_DELIVERED" };
  if (state.state === "FAILED")
    return { accepted: false, events: [], reason: "ALREADY_FAILED" };
  if (state.state === "TIMED_OUT")
    return { accepted: false, events: [], reason: "ALREADY_TIMED_OUT" };
  if (state.state === "CANCELLED")
    return { accepted: false, events: [], reason: "ALREADY_CANCELLED" };

  const event = makeEvent(
    state,
    "CommandCancelled",
    {
      reason: input.reason,
      requestedBy: input.requestedBy,
      oldGatewayId: state.gatewayId ?? null,
      oldLeaseId: state.leaseId ?? null,
      oldGeneration: state.generation,
    },
    "CANCEL",
    input.cancelledAt,
    factory,
    input.commandId,
  );
  return {
    accepted: true,
    events: [event],
    result: { state: "CANCELLED" as const },
  };
}

export function decideReplace(
  oldState: CommandSnapshot | undefined,
  input: ReplaceInput,
  factory: EventFactory,
): Decision {
  if (!oldState)
    return { accepted: false, events: [], reason: "COMMAND_NOT_FOUND" };
  if (oldState.state === "DELIVERED")
    return { accepted: false, events: [], reason: "OLD_ALREADY_DELIVERED" };
  if (oldState.state === "FAILED")
    return { accepted: false, events: [], reason: "OLD_ALREADY_FAILED" };
  if (oldState.state === "TIMED_OUT")
    return { accepted: false, events: [], reason: "OLD_ALREADY_TIMED_OUT" };
  if (oldState.state === "CANCELLED")
    return { accepted: false, events: [], reason: "OLD_ALREADY_CANCELLED" };

  const events: DomainEvent[] = [];

  const superseded = makeEvent(
    oldState,
    "CommandSuperseded",
    {
      replacementCommandId: input.newCommandId,
      replacementIdempotencyKey: input.newIdempotencyKey,
      reason: input.reason,
      requestedBy: input.requestedBy,
      oldGatewayId: oldState.gatewayId ?? null,
      oldLeaseId: oldState.leaseId ?? null,
      oldGeneration: oldState.generation,
    },
    "REPLACE",
    input.replacedAt,
    factory,
    input.oldCommandId,
  );
  events.push(superseded);

  const submitted = makeEvent(
    undefined,
    "CommandSubmitted",
    {
      commandId: input.newCommandId,
      idempotencyKey: input.newIdempotencyKey,
      deviceId: input.deviceId,
      payload: input.replacementPayload,
      supersedesCommandId: input.oldCommandId,
    },
    "REPLACE",
    input.replacedAt,
    factory,
    input.newIdempotencyKey,
  );
  events.push(submitted);

  return {
    accepted: true,
    events,
    result: {
      oldState: "CANCELLED" as const,
      newCommandId: input.newCommandId,
    },
  };
}
