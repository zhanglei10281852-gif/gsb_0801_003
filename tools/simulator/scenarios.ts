import { ApiClient } from "./api-client.js";
import { randomUUID } from "node:crypto";
import {
  GatewaySimulator,
  immediateAckDevice,
  immediateNackDevice,
  type DeviceBehavior,
} from "./gateway.js";

function uniqueKey(prefix: string) {
  return `${prefix}-${Date.now()}-${randomUUID()}`;
}

export interface ScenarioResult {
  name: string;
  passed: boolean;
  details: string[];
  data?: Record<string, unknown>;
}

function ok(
  name: string,
  details: string[],
  data?: Record<string, unknown>,
): ScenarioResult {
  return { name, passed: true, details, data };
}
function fail(
  name: string,
  details: string[],
  data?: Record<string, unknown>,
): ScenarioResult {
  return { name, passed: false, details, data };
}

export async function scenarioHappyPath(
  client: ApiClient,
): Promise<ScenarioResult> {
  const key = uniqueKey("happy");
  const submit = await client.submit({
    idempotencyKey: key,
    deviceId: "device-1",
    payload: { type: "CALIBRATE", params: { target: 42 } },
  });
  const cmdId = (submit.body as { commandId: string }).commandId;
  const gw = new GatewaySimulator("gw-happy", client, immediateAckDevice);
  const leased = await gw.pollOnce(undefined, cmdId);
  if (!leased || leased.commandId !== cmdId)
    return fail("happy", ["claim did not return command"]);
  const ack = await gw.ack(leased);
  if (ack.status !== 200)
    return fail("happy", ["ack not accepted", String(ack.status)]);
  const final = await client.getCommand(cmdId, true);
  const body = final.body as {
    state: string;
    events: unknown[];
    attempt: number;
  };
  if (body.state !== "DELIVERED")
    return fail("happy", ["state not delivered", body.state]);
  return ok("happy", ["submitted, claimed with attempt=1, acked, delivered"], {
    commandId: cmdId,
    events: body.events.length,
    attempt: body.attempt,
  });
}

export async function scenarioDuplicateSubmit(
  client: ApiClient,
): Promise<ScenarioResult> {
  const key = uniqueKey("dup");
  const first = await client.submit({
    idempotencyKey: key,
    deviceId: "device-1",
    payload: { type: "RESET" },
  });
  const second = await client.submit({
    idempotencyKey: key,
    deviceId: "device-1",
    payload: { type: "RESET" },
  });
  const b1 = first.body as { commandId: string; duplicate: boolean };
  const b2 = second.body as { commandId: string; duplicate: boolean };
  if (b1.commandId !== b2.commandId || !b2.duplicate) {
    return fail(
      "duplicate-submit",
      ["same idempotency key did not map to stable command id"],
      {
        firstStatus: first.status,
        secondStatus: second.status,
        first: first.body,
        second: second.body,
        key,
      },
    );
  }
  return ok(
    "duplicate-submit",
    ["same command id returned on duplicate submission"],
    {
      commandId: b1.commandId,
    },
  );
}

export async function scenarioLeaseExpiryAndRetry(
  client: ApiClient,
): Promise<ScenarioResult> {
  const key = uniqueKey("lease");
  const submit = await client.submit({
    idempotencyKey: key,
    deviceId: "device-1",
    payload: { type: "SWITCH_RECIPE", params: { recipe: "A" } },
  });
  const cmdId = (submit.body as { commandId: string }).commandId;
  const gw1 = new GatewaySimulator("gw-lost", client, immediateAckDevice);
  const leased1 = await gw1.pollOnce(300, cmdId);
  if (!leased1) return fail("lease-retry", ["first claim failed"]);
  await new Promise((r) => setTimeout(r, 400));
  const sweep = await client.adminSweep();
  const gw2 = new GatewaySimulator("gw-recovery", client, immediateAckDevice);
  const leased2 = await gw2.pollOnce(5000, cmdId);
  if (!leased2 || leased2.commandId !== cmdId) {
    return fail("lease-retry", ["reclaim after expiry failed"]);
  }
  if (leased2.attempt !== 2) {
    return fail("lease-retry", [`expected attempt 2, got ${leased2.attempt}`]);
  }
  const staleAck = await GatewaySimulator.duplicateLeaseAck(
    client,
    gw1.gatewayId,
    cmdId,
    leased1.leaseId,
    leased1.generation,
  );
  if (staleAck.status === 200) {
    return fail("lease-retry", ["stale lease ack should have been rejected"]);
  }
  const ack = await gw2.ack(leased2);
  const final = await client.getCommand(cmdId, true);
  const body = final.body as { state: string; events: { type: string }[] };
  const types = body.events.map((e) => e.type);
  const hasExpired = types.includes("LeaseExpired");
  if (ack.status !== 200 || body.state !== "DELIVERED" || !hasExpired) {
    return fail("lease-retry", ["retry state or event chain incorrect"], {
      types,
      state: body.state,
    });
  }
  return ok(
    "lease-retry",
    ["lease expired, same command retried with attempt=2, stale ack rejected"],
    {
      sweep: sweep.body,
      events: types,
    },
  );
}

export async function scenarioDuplicateAck(
  client: ApiClient,
): Promise<ScenarioResult> {
  const key = uniqueKey("dupack");
  const submit = await client.submit({
    idempotencyKey: key,
    deviceId: "device-2",
    payload: { type: "CALIBRATE" },
  });
  const cmdId = (submit.body as { commandId: string }).commandId;
  const gw = new GatewaySimulator("gw-dupack", client, immediateAckDevice);
  const leased = await gw.pollOnce(undefined, cmdId);
  if (!leased) return fail("duplicate-ack", ["claim failed"]);
  const first = await gw.ack(leased);
  const second = await gw.ack(leased);
  if (first.status !== 200 || second.status !== 409) {
    return fail("duplicate-ack", ["second ack must be rejected"], {
      first: first.status,
      second: second.status,
    });
  }
  const final = await client.getCommand(cmdId);
  if ((final.body as { state: string }).state !== "DELIVERED") {
    return fail("duplicate-ack", ["state not delivered"]);
  }
  return ok("duplicate-ack", ["first ack delivered, duplicate ack rejected"]);
}

export async function scenarioOutOfOrderAndWrongLease(
  client: ApiClient,
): Promise<ScenarioResult> {
  const key = uniqueKey("ooo");
  await client.submit({
    idempotencyKey: key,
    deviceId: "device-3",
    payload: { type: "RESET" },
  });
  const gw = new GatewaySimulator("gw-ooo", client, immediateAckDevice);
  const cmdId = (
    (await client.getByIdempotencyKey(key)).body as { commandId: string }
  ).commandId;
  const leased = await gw.pollOnce(1000, cmdId);
  if (!leased) return fail("wrong-lease", ["claim failed"]);
  const fakeLease = GatewaySimulator.newLeaseId();
  const wrong = await client.ack(gw.gatewayId, leased.commandId, fakeLease, leased.generation, {
    success: true,
    ackCode: "SPURIOUS",
  });
  if (wrong.status !== 409)
    return fail("wrong-lease", ["wrong lease should be rejected"]);
  const renewWrong = await client.renew(
    gw.gatewayId,
    leased.commandId,
    fakeLease,
    leased.generation,
  );
  if (renewWrong.status !== 409)
    return fail("wrong-lease", ["renew wrong lease should be rejected"]);
  const good = await gw.ack(leased);
  if (good.status !== 200) return fail("wrong-lease", ["valid ack failed"]);
  return ok("wrong-lease", [
    "wrong/out-of-order lease context rejected; valid lease accepted",
  ]);
}

export async function scenarioNackFailure(
  client: ApiClient,
): Promise<ScenarioResult> {
  const key = uniqueKey("nack");
  const submit = await client.submit({
    idempotencyKey: key,
    deviceId: "device-4",
    payload: { type: "CALIBRATE" },
  });
  const cmdId = (submit.body as { commandId: string }).commandId;
  const gw = new GatewaySimulator("gw-nack", client, immediateNackDevice);
  const leased = await gw.pollOnce(undefined, cmdId);
  if (!leased) return fail("nack", ["claim failed"]);
  const ack = await gw.ack(leased);
  if (ack.status !== 200) return fail("nack", ["nack not recorded"]);
  const final = await client.getCommand(cmdId, true);
  const body = final.body as { state: string; events: { type: string }[] };
  if (
    body.state !== "FAILED" ||
    !body.events.some((e) => e.type === "DeviceNackRecorded")
  ) {
    return fail("nack", ["nack did not produce terminal FAILED state"]);
  }
  return ok("nack", ["negative device ack recorded as terminal FAILED"]);
}

export async function scenarioDualGatewayFailover(
  client: ApiClient,
): Promise<ScenarioResult> {
  const key = uniqueKey("failover");
  const submit = await client.submit({
    idempotencyKey: key,
    deviceId: "device-redundant",
    payload: { type: "SWITCH_RECIPE", params: { recipe: "C" } },
  });
  const cmdId = (submit.body as { commandId: string }).commandId;

  const primary = new GatewaySimulator(
    "gw-primary",
    client,
    immediateAckDevice,
  );
  const standby = new GatewaySimulator(
    "gw-standby",
    client,
    immediateAckDevice,
  );

  const primaryLease = await primary.pollOnce(400, cmdId);
  if (!primaryLease) return fail("failover", ["primary claim failed"]);
  if (primaryLease.generation !== 1) {
    return fail("failover", ["first ownership generation must be 1"], {
      got: primaryLease.generation,
    });
  }

  const blocked = await standby.pollOnce(400, cmdId);
  if (blocked) {
    return fail(
      "failover",
      ["standby must not take over while primary lease is active"],
      {
        claimed: blocked,
      },
    );
  }

  await new Promise((r) => setTimeout(r, 450));

  const standbyLease = await standby.pollOnce(5000, cmdId);
  if (!standbyLease)
    return fail("failover", ["standby failed to take over after lease expiry"]);
  if (standbyLease.commandId !== cmdId || standbyLease.attempt !== 2) {
    return fail(
      "failover",
      ["takeover must reuse stable commandId and increment attempt"],
      {
        standbyLease,
      },
    );
  }
  if (standbyLease.generation !== 2) {
    return fail(
      "failover",
      ["takeover must advance ownership generation to 2"],
      {
        got: standbyLease.generation,
      },
    );
  }

  const staleRenew = await primary.renew(primaryLease, 5000);
  if (staleRenew.status === 200) {
    return fail("failover", [
      "stale primary renew after takeover must be fenced",
    ]);
  }
  const staleAck = await GatewaySimulator.staleGenerationAck(
    client,
    primary.gatewayId,
    cmdId,
    primaryLease.leaseId,
    primaryLease.generation,
  );
  if (staleAck.status === 200) {
    return fail("failover", [
      "stale primary ack after takeover must be fenced",
    ]);
  }

  const ack = await standby.ack(standbyLease);
  if (ack.status !== 200) return fail("failover", ["standby ack not accepted"]);

  const final = await client.getCommand(cmdId, true);
  const body = final.body as {
    state: string;
    attempt: number;
    generation: number;
    events: { type: string; data: Record<string, unknown>; causedBy: string }[];
  };
  const types = body.events.map((e) => e.type);
  const claimGen1 = body.events.find(
    (e) => e.type === "CommandClaimed" && e.data.generation === 1,
  );
  const claimGen2 = body.events.find(
    (e) => e.type === "CommandClaimed" && e.data.generation === 2,
  );
  const delivered = body.events.find((e) => e.type === "DeviceAckRecorded");
  if (
    body.state !== "DELIVERED" ||
    body.attempt !== 2 ||
    body.generation !== 2 ||
    !claimGen1 ||
    !claimGen2 ||
    (delivered?.data as { generation?: number })?.generation !== 2
  ) {
    return fail("failover", ["takeover audit trail or final state incorrect"], {
      state: body.state,
      attempt: body.attempt,
      generation: body.generation,
      deliveredGeneration: (delivered?.data as { generation?: number })
        ?.generation,
      types,
    });
  }
  return ok(
    "failover",
    [
      "primary owned generation 1",
      "standby blocked during active lease",
      "standby took over with generation 2 and attempt 2 under same commandId",
      "stale primary renew/ack fenced",
      "generation-2 ack durably delivered",
    ],
    { commandId: cmdId, events: types },
  );
}

export async function scenarioCancel(client: ApiClient): Promise<ScenarioResult> {
  const key = uniqueKey("cancel");
  const submit = await client.submit({
    idempotencyKey: key,
    deviceId: "device-cancel",
    payload: { type: "CALIBRATE" },
  });
  const cmdId = (submit.body as { commandId: string }).commandId;
  const cancelled = await client.cancel(cmdId, "OPERATOR_ABORT", "operator");
  if (cancelled.status !== 200)
    return fail("cancel", ["cancel not accepted"], { body: cancelled.body });
  const final = (await client.getCommand(cmdId, true)).body as {
    state: string;
    events: { type: string; causedBy: string }[];
  };
  if (final.state !== "CANCELLED")
    return fail("cancel", ["state not CANCELLED", final.state]);
  if (!final.events.some((e) => e.type === "CommandCancelled"))
    return fail("cancel", ["CommandCancelled event missing"]);
  return ok("cancel", ["pending command cancelled and event recorded"]);
}

export async function scenarioCancelDeliveredRejected(
  client: ApiClient,
): Promise<ScenarioResult> {
  const key = uniqueKey("cancel-delivered");
  const submit = await client.submit({
    idempotencyKey: key,
    deviceId: "device-cancel-done",
    payload: { type: "CALIBRATE" },
  });
  const cmdId = (submit.body as { commandId: string }).commandId;
  const gw = new GatewaySimulator("gw-cancel-done", client, immediateAckDevice);
  const leased = await gw.pollOnce(undefined, cmdId);
  if (!leased) return fail("cancel-delivered", ["claim failed"]);
  const ack = await gw.ack(leased);
  if (ack.status !== 200)
    return fail("cancel-delivered", ["ack failed", String(ack.status)]);
  const cancelAttempt = await client.cancel(cmdId, "TOO_LATE", "operator");
  if (cancelAttempt.status !== 409)
    return fail("cancel-delivered", ["cancelling a delivered command must be rejected"], {
      status: cancelAttempt.status,
    });
  const final = (await client.getCommand(cmdId)).body as { state: string };
  if (final.state !== "DELIVERED")
    return fail("cancel-delivered", ["delivered state must not be disguised as cancel"]);
  return ok("cancel-delivered", [
    "delivered action cannot be disguised as a successful cancel",
  ]);
}

export async function scenarioReplace(client: ApiClient): Promise<ScenarioResult> {
  const key = uniqueKey("replace-old");
  const submit = await client.submit({
    idempotencyKey: key,
    deviceId: "device-replace",
    payload: { type: "SWITCH_RECIPE", params: { recipe: "X" } },
  });
  const oldCmdId = (submit.body as { commandId: string }).commandId;
  const newKey = uniqueKey("replace-new");
  const replaced = await client.replace(
    oldCmdId,
    newKey,
    { type: "SAFE_STOP", params: { reason: "emergency" } },
    "UNSAFE_RECIPE",
  );
  if (replaced.status !== 200)
    return fail("replace", ["replace not accepted"], { body: replaced.body });
  const body = replaced.body as {
    newCommandId: string;
    old: { state: string; supersededByCommandId?: string };
    replacement: { state: string; supersedesCommandId?: string };
  };
  if (body.old.state !== "CANCELLED" || body.replacement.state !== "PENDING")
    return fail("replace", ["old should be CANCELLED, new should be PENDING"]);
  if (
    body.old.supersededByCommandId !== body.newCommandId ||
    body.replacement.supersedesCommandId !== oldCmdId
  ) {
    return fail("replace", ["bidirectional supersession links missing"]);
  }

  const oldHistory = (await client.getCommand(oldCmdId, true)).body as {
    events: { type: string; data: Record<string, unknown> }[];
  };
  const superseded = oldHistory.events.find((e) => e.type === "CommandSuperseded");
  if (!superseded || superseded.data.replacementCommandId !== body.newCommandId) {
    return fail("replace", ["old command causal chain missing supersession"]);
  }

  const gw = new GatewaySimulator("gw-replace", client, immediateAckDevice);
  const leased = await gw.pollOnce(5000, body.newCommandId);
  if (!leased) return fail("replace", ["replacement not claimable"]);
  const ack = await gw.ack(leased);
  if (ack.status !== 200)
    return fail("replace", ["replacement ack failed", String(ack.status)]);

  const duplicate = await client.replace(
    oldCmdId,
    newKey,
    { type: "SAFE_STOP" },
    "DUPLICATE",
  );
  if (duplicate.status !== 200)
    return fail("replace", ["idempotent replace retry should return same result"], {
      status: duplicate.status,
    });
  const dupBody = duplicate.body as { newCommandId: string };
  if (dupBody.newCommandId !== body.newCommandId)
    return fail("replace", ["replace retry changed new command id"]);

  return ok("replace", [
    "old command CANCELLED and linked to replacement",
    "replacement claimed, acked and delivered under a new stable identity",
    "replace retry is idempotent",
  ]);
}

export async function scenarioThreeRoundTakeoverAndReplace(
  client: ApiClient,
): Promise<ScenarioResult> {
  const key = uniqueKey("three-round");
  const submit = await client.submit({
    idempotencyKey: key,
    deviceId: "device-three-round",
    payload: { type: "SWITCH_RECIPE", params: { recipe: "Y" } },
  });
  const firstCmdId = (submit.body as { commandId: string }).commandId;

  const gw1 = new GatewaySimulator("gw-round-1", client, immediateAckDevice);
  const lease1 = await gw1.pollOnce(300, firstCmdId);
  if (!lease1 || lease1.generation !== 1)
    return fail("three-round", ["round-1 claim failed"]);

  await new Promise((r) => setTimeout(r, 400));
  const gw2 = new GatewaySimulator("gw-round-2", client, immediateAckDevice);
  const lease2 = await gw2.pollOnce(3000, firstCmdId);
  if (!lease2 || lease2.generation !== 2)
    return fail("three-round", ["round-2 takeover failed"]);

  const lateFrom1 = await GatewaySimulator.staleGenerationAck(
    client,
    gw1.gatewayId,
    firstCmdId,
    lease1.leaseId,
    lease1.generation,
  );
  if (lateFrom1.status === 200)
    return fail("three-round", ["round-1 stale ack must be fenced after takeover"]);

  const replaceKey = uniqueKey("three-round-safe");
  const replaced = await client.replace(
    firstCmdId,
    replaceKey,
    { type: "SAFE_STOP", params: { round: 3 } },
    "ROUND_3_SAFE_COMMAND",
  );
  if (replaced.status !== 200)
    return fail("three-round", ["round-3 replace not accepted"], {
      body: replaced.body,
    });
  const replaceBody = replaced.body as {
    newCommandId: string;
    old: { state: string };
  };

  const lateFrom2 = await GatewaySimulator.staleGenerationAck(
    client,
    gw2.gatewayId,
    firstCmdId,
    lease2.leaseId,
    lease2.generation,
  );
  if (lateFrom2.status === 200)
    return fail("three-round", [
      "round-2 stale ack must be fenced after round-3 replacement",
    ]);

  const gw3 = new GatewaySimulator("gw-round-3", client, immediateAckDevice);
  const lease3 = await gw3.pollOnce(5000, replaceBody.newCommandId);
  if (!lease3 || lease3.generation !== 1)
    return fail("three-round", ["round-3 replacement claim failed"]);
  const ack3 = await gw3.ack(lease3);
  if (ack3.status !== 200)
    return fail("three-round", ["round-3 ack failed", String(ack3.status)]);

  const oldFinal = (await client.getCommand(firstCmdId, true)).body as {
    state: string;
    supersededByCommandId?: string;
    events: { type: string; data: Record<string, unknown> }[];
  };
  const newFinal = (await client.getCommand(replaceBody.newCommandId)).body as {
    state: string;
    supersedesCommandId?: string;
  };
  const chain = oldFinal.events.map((e) => e.type);
  if (
    oldFinal.state !== "CANCELLED" ||
    oldFinal.supersededByCommandId !== replaceBody.newCommandId ||
    newFinal.state !== "DELIVERED" ||
    newFinal.supersedesCommandId !== firstCmdId ||
    !chain.includes("CommandClaimed") ||
    !chain.includes("CommandSuperseded")
  ) {
    return fail("three-round", ["three-round causal chain inconsistent"], {
      oldState: oldFinal.state,
      newState: newFinal.state,
      chain,
    });
  }
  return ok("three-round", [
    "round 1 owned generation 1; round 2 took over generation 2 and fenced round-1 ack",
    "round 3 replaced with a safe command and fenced round-2 late ack",
    "replacement delivered as new stable identity; old command CANCELLED with full causal chain",
  ]);
}

export const builtInScenarios: Record<
  string,
  (client: ApiClient) => Promise<ScenarioResult>
> = {
  happy: scenarioHappyPath,
  "duplicate-submit": scenarioDuplicateSubmit,
  "lease-retry": scenarioLeaseExpiryAndRetry,
  "duplicate-ack": scenarioDuplicateAck,
  "wrong-lease": scenarioOutOfOrderAndWrongLease,
  nack: scenarioNackFailure,
  failover: scenarioDualGatewayFailover,
  cancel: scenarioCancel,
  "cancel-delivered-rejected": scenarioCancelDeliveredRejected,
  replace: scenarioReplace,
  "three-round": scenarioThreeRoundTakeoverAndReplace,
};

export function customDevice(mode: "ack" | "nack" | "slow"): DeviceBehavior {
  if (mode === "nack") return immediateNackDevice;
  if (mode === "slow") {
    return {
      async process() {
        await new Promise((r) => setTimeout(r, 50));
        return { success: true, ackCode: "EXECUTED_SLOW" };
      },
    };
  }
  return immediateAckDevice;
}
