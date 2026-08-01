import { describe, it, expect } from "vitest";
import {
  claimCommand,
  confirmCommand,
  createPendingCommand,
  expireLease,
  recordRejectedLeaseOperation,
  renewLease,
  reportDelivery,
  Clock,
  IdGenerator,
} from "../../src/domain/stateMachine.js";
import {
  InvalidLeaseError,
  InvalidStateTransitionError,
} from "../../src/domain/errors.js";
import { Command } from "../../src/domain/types.js";

class FakeClock implements Clock {
  private t: number;
  constructor(start = 1_000_000) {
    this.t = start;
  }
  now(): number {
    return this.t;
  }
  advance(ms: number): void {
    this.t += ms;
  }
}

class FakeIdGen implements IdGenerator {
  private counter = 0;
  newId(): string {
    this.counter += 1;
    return `id-${this.counter}`;
  }
}

function makeSubmit() {
  return {
    idempotencyKey: "biz-key-1",
    payload: { deviceId: "dev-A", action: "calibrate", params: { x: 1 } },
    maxAttempts: 2,
  };
}

function setup(clock = new FakeClock(), idGen = new FakeIdGen()) {
  const submit = makeSubmit();
  const { command, events } = createPendingCommand(submit, null, idGen, clock);
  return { clock, idGen, command, events, submit };
}

describe("state machine — submit idempotency", () => {
  it("creates a new PENDING command and emits CommandSubmitted", () => {
    const { command, events } = setup();
    expect(command.status).toBe("PENDING");
    expect(command.attempt).toBe(0);
    expect(command.currentLeaseId).toBeNull();
    expect(events).toHaveLength(1);
    expect(events[0].eventType).toBe("CommandSubmitted");
    expect(events[0].causedBy).toBe("submit");
  });

  it("returns the existing command unchanged when idempotency key already exists", () => {
    const clock = new FakeClock();
    const idGen = new FakeIdGen();
    const first = createPendingCommand(makeSubmit(), null, idGen, clock);
    clock.advance(5000);
    const second = createPendingCommand(
      makeSubmit(),
      first.command,
      idGen,
      clock,
    );
    expect(second.command.commandId).toBe(first.command.commandId);
    expect(second.events).toHaveLength(0);
  });
});

describe("state machine — claim and renew", () => {
  it("transitions PENDING to CLAIMED with a new lease and increments attempt", () => {
    const { clock, idGen, command } = setup();
    const decision = claimCommand(
      command,
      { gatewayId: "gw-1", leaseDurationMs: 1000 },
      idGen,
      clock,
    );
    expect(decision.command.status).toBe("CLAIMED");
    expect(decision.command.attempt).toBe(1);
    expect(decision.command.currentLeaseId).toBeTruthy();
    expect(decision.command.currentLeaseId).not.toBe(command.commandId);
    expect(decision.command.leaseExpiresAt).toBe(clock.now() + 1000);
    expect(decision.events[0].eventType).toBe("CommandClaimed");
  });

  it("renews an active lease and extends expiry", () => {
    const { clock, idGen, command } = setup();
    const claimed = claimCommand(
      command,
      { gatewayId: "gw-1", leaseDurationMs: 1000 },
      idGen,
      clock,
    );
    clock.advance(500);
    const renewed = renewLease(
      claimed.command,
      {
        commandId: claimed.command.commandId,
        leaseId: claimed.command.currentLeaseId!,
        gatewayId: "gw-1",
        leaseDurationMs: 1000,
      },
      idGen,
      clock,
    );
    expect(renewed.command.leaseExpiresAt).toBe(clock.now() + 1000);
    expect(renewed.events[0].eventType).toBe("LeaseRenewed");
  });

  it("rejects renew with wrong leaseId", () => {
    const { clock, idGen, command } = setup();
    const claimed = claimCommand(
      command,
      { gatewayId: "gw-1", leaseDurationMs: 1000 },
      idGen,
      clock,
    );
    expect(() =>
      renewLease(
        claimed.command,
        {
          commandId: claimed.command.commandId,
          leaseId: "wrong-lease",
          gatewayId: "gw-1",
          leaseDurationMs: 1000,
        },
        idGen,
        clock,
      ),
    ).toThrow(InvalidLeaseError);
  });

  it("rejects renew after lease expiry", () => {
    const { clock, idGen, command } = setup();
    const claimed = claimCommand(
      command,
      { gatewayId: "gw-1", leaseDurationMs: 1000 },
      idGen,
      clock,
    );
    clock.advance(1001);
    expect(() =>
      renewLease(
        claimed.command,
        {
          commandId: claimed.command.commandId,
          leaseId: claimed.command.currentLeaseId!,
          gatewayId: "gw-1",
          leaseDurationMs: 1000,
        },
        idGen,
        clock,
      ),
    ).toThrow(InvalidLeaseError);
  });
});

describe("state machine — device confirmation", () => {
  it("marks command SUCCEEDED only with valid active lease and emits DeviceConfirmed + CommandSucceeded", () => {
    const { clock, idGen, command } = setup();
    const claimed = claimCommand(
      command,
      { gatewayId: "gw-1", leaseDurationMs: 1000 },
      idGen,
      clock,
    );
    const result = confirmCommand(
      claimed.command,
      {
        commandId: claimed.command.commandId,
        leaseId: claimed.command.currentLeaseId!,
        gatewayId: "gw-1",
        confirmationCode: "ACK-42",
        deviceTimestamp: clock.now(),
      },
      idGen,
      clock,
    );
    expect(result.command.status).toBe("SUCCEEDED");
    expect(result.command.confirmationCode).toBe("ACK-42");
    expect(result.events.map((e) => e.eventType)).toEqual([
      "DeviceConfirmed",
      "CommandSucceeded",
    ]);
  });

  it("rejects late confirmation after lease expiry (no zombie resurrection)", () => {
    const { clock, idGen, command } = setup();
    const claimed = claimCommand(
      command,
      { gatewayId: "gw-1", leaseDurationMs: 1000 },
      idGen,
      clock,
    );
    clock.advance(1001);
    const result = confirmCommand(
      claimed.command,
      {
        commandId: claimed.command.commandId,
        leaseId: claimed.command.currentLeaseId!,
        gatewayId: "gw-1",
        confirmationCode: "LATE-ACK",
        deviceTimestamp: clock.now(),
      },
      idGen,
      clock,
    );
    expect(result.command.status).toBe("CLAIMED");
    expect(result.command.confirmationCode).toBeNull();
    expect(result.events[0].eventType).toBe("StaleMessageRejected");
    expect(result.events[0].causedBy).toBe("confirm-lease-expired");
  });

  it("rejects confirmation with stale leaseId after reclaim (old gateway late ack)", () => {
    const { clock, idGen, command } = setup();
    const first = claimCommand(
      command,
      { gatewayId: "gw-1", leaseDurationMs: 500 },
      idGen,
      clock,
    );
    const firstLeaseId = first.command.currentLeaseId!;
    clock.advance(501);
    const second = claimCommand(
      first.command,
      { gatewayId: "gw-2", leaseDurationMs: 500 },
      idGen,
      clock,
    );
    expect(second.command.currentLeaseId).not.toBe(firstLeaseId);
    expect(second.events[0].eventType).toBe("CommandReclaimed");

    const stale = confirmCommand(
      second.command,
      {
        commandId: second.command.commandId,
        leaseId: firstLeaseId,
        gatewayId: "gw-1",
        confirmationCode: "OLD-ACK",
        deviceTimestamp: clock.now(),
      },
      idGen,
      clock,
    );
    expect(stale.command.status).toBe("CLAIMED");
    expect(stale.command.confirmationCode).toBeNull();
    expect(stale.events[0].eventType).toBe("StaleMessageRejected");
    expect(stale.events[0].causedBy).toBe("confirm-stale-lease");
  });

  it("is idempotent for duplicate confirmation after success and records StaleMessageRejected", () => {
    const { clock, idGen, command } = setup();
    const claimed = claimCommand(
      command,
      { gatewayId: "gw-1", leaseDurationMs: 1000 },
      idGen,
      clock,
    );
    const succeeded = confirmCommand(
      claimed.command,
      {
        commandId: claimed.command.commandId,
        leaseId: claimed.command.currentLeaseId!,
        gatewayId: "gw-1",
        confirmationCode: "ACK-1",
        deviceTimestamp: clock.now(),
      },
      idGen,
      clock,
    );
    const dup = confirmCommand(
      succeeded.command,
      {
        commandId: succeeded.command.commandId,
        leaseId: "whatever",
        gatewayId: "gw-1",
        confirmationCode: "ACK-1",
        deviceTimestamp: clock.now(),
      },
      idGen,
      clock,
    );
    expect(dup.command.status).toBe("SUCCEEDED");
    expect(dup.events).toHaveLength(1);
    expect(dup.events[0].eventType).toBe("StaleMessageRejected");
    expect(dup.events[0].causedBy).toBe("duplicate-confirm-after-success");
  });

  it("rejects confirmation on a PENDING command (lease expired and reset) without resurrecting", () => {
    const { clock, idGen, command } = setup();
    const claimed = claimCommand(
      command,
      { gatewayId: "gw-1", leaseDurationMs: 500 },
      idGen,
      clock,
    );
    const leaseId = claimed.command.currentLeaseId!;
    clock.advance(501);
    const expired = expireLease(claimed.command, idGen, clock);
    expect(expired).not.toBeNull();
    expect(expired!.decision.command.status).toBe("PENDING");
    const stale = confirmCommand(
      expired!.decision.command,
      {
        commandId: expired!.decision.command.commandId,
        leaseId,
        gatewayId: "gw-1",
        confirmationCode: "LATE",
        deviceTimestamp: clock.now(),
      },
      idGen,
      clock,
    );
    expect(stale.command.status).toBe("PENDING");
    expect(stale.events[0].eventType).toBe("StaleMessageRejected");
    expect(stale.events[0].causedBy).toBe("confirm-after-lease-expired");
  });
});

describe("state machine — lease expiry and reclaim", () => {
  it("returns CLAIMED to PENDING on lease expiry when attempts remain", () => {
    const { clock, idGen, command } = setup();
    const claimed = claimCommand(
      command,
      { gatewayId: "gw-1", leaseDurationMs: 500 },
      idGen,
      clock,
    );
    clock.advance(501);
    const outcome = expireLease(claimed.command, idGen, clock);
    expect(outcome).not.toBeNull();
    expect(outcome!.decision.command.status).toBe("PENDING");
    expect(outcome!.decision.command.currentLeaseId).toBeNull();
    expect(outcome!.decision.command.attempt).toBe(1);
    expect(outcome!.result.failed).toBe(false);
    expect(outcome!.decision.events[0].eventType).toBe("LeaseExpired");
  });

  it("marks FAILED when the last attempt lease expires", () => {
    const { clock, idGen, command } = setup();
    const first = claimCommand(
      command,
      { gatewayId: "gw-1", leaseDurationMs: 500 },
      idGen,
      clock,
    );
    clock.advance(501);
    const expired1 = expireLease(first.command, idGen, clock)!;
    const second = claimCommand(
      expired1.decision.command,
      { gatewayId: "gw-2", leaseDurationMs: 500 },
      idGen,
      clock,
    );
    clock.advance(501);
    const expired2 = expireLease(second.command, idGen, clock);
    expect(expired2).not.toBeNull();
    expect(expired2!.decision.command.status).toBe("FAILED");
    expect(expired2!.result.failed).toBe(true);
    expect(expired2!.decision.events.map((e) => e.eventType)).toContain(
      "CommandFailed",
    );
  });

  it("does not expire a lease that is still active", () => {
    const { clock, idGen, command } = setup();
    const claimed = claimCommand(
      command,
      { gatewayId: "gw-1", leaseDurationMs: 1000 },
      idGen,
      clock,
    );
    clock.advance(100);
    expect(expireLease(claimed.command, idGen, clock)).toBeNull();
  });

  it("rejects a claim while another lease is still active", () => {
    const { clock, idGen, command } = setup();
    const claimed = claimCommand(
      command,
      { gatewayId: "gw-1", leaseDurationMs: 1000 },
      idGen,
      clock,
    );
    expect(() =>
      claimCommand(
        claimed.command,
        { gatewayId: "gw-2", leaseDurationMs: 1000 },
        idGen,
        clock,
      ),
    ).toThrow(InvalidLeaseError);
  });

  it("reclaims after expiry and increments attempt with stable commandId", () => {
    const { clock, idGen, command } = setup();
    const first = claimCommand(
      command,
      { gatewayId: "gw-1", leaseDurationMs: 500 },
      idGen,
      clock,
    );
    const originalId = first.command.commandId;
    clock.advance(501);
    const second = claimCommand(
      first.command,
      { gatewayId: "gw-2", leaseDurationMs: 500 },
      idGen,
      clock,
    );
    expect(second.command.commandId).toBe(originalId);
    expect(second.command.attempt).toBe(2);
    expect(second.command.status).toBe("CLAIMED");
    expect(second.events[0].eventType).toBe("CommandReclaimed");
  });

  it("fails when claim would exceed maxAttempts", () => {
    const { clock, idGen, command } = setup();
    const first = claimCommand(
      command,
      { gatewayId: "gw-1", leaseDurationMs: 500 },
      idGen,
      clock,
    );
    clock.advance(501);
    const expired1 = expireLease(first.command, idGen, clock)!;
    const second = claimCommand(
      expired1.decision.command,
      { gatewayId: "gw-2", leaseDurationMs: 500 },
      idGen,
      clock,
    );
    clock.advance(501);
    const expired2 = expireLease(second.command, idGen, clock)!;
    expect(expired2.decision.command.status).toBe("FAILED");
    expect(() =>
      claimCommand(
        expired2.decision.command,
        { gatewayId: "gw-3", leaseDurationMs: 500 },
        idGen,
        clock,
      ),
    ).toThrow(InvalidStateTransitionError);
  });
});

describe("state machine — delivery report", () => {
  it("records a DeliveryReported event without changing lease state", () => {
    const { clock, idGen, command } = setup();
    const claimed = claimCommand(
      command,
      { gatewayId: "gw-1", leaseDurationMs: 1000 },
      idGen,
      clock,
    );
    const reported = reportDelivery(
      claimed.command,
      {
        commandId: claimed.command.commandId,
        leaseId: claimed.command.currentLeaseId!,
        gatewayId: "gw-1",
        deviceMessage: "sent over Modbus",
      },
      idGen,
      clock,
    );
    expect(reported.command.status).toBe("CLAIMED");
    expect(reported.events[0].eventType).toBe("DeliveryReported");
  });
});

describe("state machine — causal chain integrity", () => {
  it("produces an ordered causal event chain through a full happy path", () => {
    const clock = new FakeClock();
    const idGen = new FakeIdGen();
    const submitted = createPendingCommand(makeSubmit(), null, idGen, clock);
    clock.advance(10);
    const claimed = claimCommand(
      submitted.command,
      { gatewayId: "gw-1", leaseDurationMs: 5000 },
      idGen,
      clock,
    );
    clock.advance(10);
    const reported = reportDelivery(
      claimed.command,
      {
        commandId: claimed.command.commandId,
        leaseId: claimed.command.currentLeaseId!,
        gatewayId: "gw-1",
      },
      idGen,
      clock,
    );
    clock.advance(10);
    const confirmed = confirmCommand(
      reported.command,
      {
        commandId: reported.command.commandId,
        leaseId: reported.command.currentLeaseId!,
        gatewayId: "gw-1",
        confirmationCode: "OK",
        deviceTimestamp: clock.now(),
      },
      idGen,
      clock,
    );
    const allEvents = [
      ...submitted.events,
      ...claimed.events,
      ...reported.events,
      ...confirmed.events,
    ];
    expect(allEvents.map((e) => e.eventType)).toEqual([
      "CommandSubmitted",
      "CommandClaimed",
      "DeliveryReported",
      "DeviceConfirmed",
      "CommandSucceeded",
    ]);
    for (const e of allEvents) {
      expect(e.commandId).toBe(submitted.command.commandId);
    }
  });
});

describe("state machine — ownership generation (HA fencing)", () => {
  it("CommandReclaimed event carries oldGeneration -> newGeneration transition", () => {
    const { clock, idGen, command } = setup();
    const first = claimCommand(
      command,
      { gatewayId: "gw-primary", leaseDurationMs: 500 },
      idGen,
      clock,
    );
    clock.advance(501);
    const reclaimed = claimCommand(
      first.command,
      { gatewayId: "gw-standby", leaseDurationMs: 500 },
      idGen,
      clock,
    );
    expect(reclaimed.command.attempt).toBe(2);
    const event = reclaimed.events[0];
    expect(event.eventType).toBe("CommandReclaimed");
    expect(event.payload).toMatchObject({
      oldGatewayId: "gw-primary",
      oldGeneration: 1,
      newGeneration: 2,
    });
    expect(event.leaseId).toBe(reclaimed.command.currentLeaseId);
    expect(event.gatewayId).toBe("gw-standby");
  });

  it("CommandClaimed event records generation for first ownership", () => {
    const { clock, idGen, command } = setup();
    const claimed = claimCommand(
      command,
      { gatewayId: "gw-1", leaseDurationMs: 1000 },
      idGen,
      clock,
    );
    expect(claimed.events[0].payload).toMatchObject({ generation: 1 });
  });

  it("recordRejectedLeaseOperation produces an auditable event without changing state", () => {
    const { clock, idGen, command } = setup();
    const claimed = claimCommand(
      command,
      { gatewayId: "gw-current", leaseDurationMs: 5000 },
      idGen,
      clock,
    );
    const oldLeaseId = "stale-old-lease";
    const event = recordRejectedLeaseOperation(
      claimed.command,
      {
        operation: "renew",
        leaseId: oldLeaseId,
        gatewayId: "gw-old",
        reason: "leaseId does not match current lease",
      },
      idGen,
      clock,
    );
    expect(event.eventType).toBe("LeaseOperationRejected");
    expect(event.causedBy).toBe("rejected-renew");
    expect(event.leaseId).toBe(oldLeaseId);
    expect(event.gatewayId).toBe("gw-old");
    expect(event.attempt).toBe(claimed.command.attempt);
    expect(event.payload).toMatchObject({
      operation: "renew",
      currentLeaseId: claimed.command.currentLeaseId,
      currentGatewayId: "gw-current",
      currentGeneration: 1,
      rejectedLeaseId: oldLeaseId,
      rejectedGatewayId: "gw-old",
    });
  });

  it("old generation renew/confirm after reclaim cannot advance state", () => {
    const { clock, idGen, command } = setup();
    const first = claimCommand(
      command,
      { gatewayId: "gw-old", leaseDurationMs: 500 },
      idGen,
      clock,
    );
    const oldLease = first.command.currentLeaseId!;
    clock.advance(501);
    const second = claimCommand(
      first.command,
      { gatewayId: "gw-new", leaseDurationMs: 5000 },
      idGen,
      clock,
    );
    expect(second.command.currentLeaseId).not.toBe(oldLease);
    expect(second.command.attempt).toBe(2);

    expect(() =>
      renewLease(
        second.command,
        {
          commandId: second.command.commandId,
          leaseId: oldLease,
          gatewayId: "gw-old",
          leaseDurationMs: 5000,
        },
        idGen,
        clock,
      ),
    ).toThrow(InvalidLeaseError);

    const staleConfirm = confirmCommand(
      second.command,
      {
        commandId: second.command.commandId,
        leaseId: oldLease,
        gatewayId: "gw-old",
        confirmationCode: "GHOST",
        deviceTimestamp: clock.now(),
      },
      idGen,
      clock,
    );
    expect(staleConfirm.command.status).toBe("CLAIMED");
    expect(staleConfirm.command.confirmationCode).toBeNull();
    expect(staleConfirm.events[0].eventType).toBe("StaleMessageRejected");
    expect(staleConfirm.events[0].causedBy).toBe("confirm-stale-lease");
  });
});
