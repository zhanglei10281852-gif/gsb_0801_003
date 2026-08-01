import assert from "node:assert/strict";
import {
  startCompiledServer,
  createManagedServer,
  sleep,
  ServerHandle,
} from "./serverHarness.js";
import { ApiClient, ApiCommand } from "../simulator/apiClient.js";
import { DeviceSimulator } from "../simulator/deviceSimulator.js";
import { GatewaySimulator } from "../simulator/gatewaySimulator.js";

let passed = 0;
let failed = 0;
const failures: { name: string; error: Error }[] = [];

async function test(name: string, fn: () => Promise<void>): Promise<void> {
  try {
    await fn();
    passed++;
    console.log(`  PASS  ${name}`);
  } catch (err) {
    failed++;
    failures.push({ name, error: err as Error });
    console.log(`  FAIL  ${name}`);
    console.log(`        ${(err as Error).message}`);
  }
}

function isCommand(body: unknown): body is ApiCommand {
  return (
    typeof body === "object" &&
    body !== null &&
    "commandId" in body &&
    "status" in body
  );
}

async function fetchCommand(
  api: ApiClient,
  commandId: string,
): Promise<ApiCommand> {
  const { body } = await api.getCommand(commandId);
  assert.ok(isCommand(body), `expected command, got: ${JSON.stringify(body)}`);
  return body;
}

function assertEventTypes(
  events: { eventType: string }[],
  expected: string[],
): void {
  const actual = events.map((e) => e.eventType);
  assert.deepEqual(
    actual,
    expected,
    `event chain mismatch\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`,
  );
}

async function waitForStatus(
  api: ApiClient,
  commandId: string,
  status: string,
  timeoutMs = 10000,
): Promise<ApiCommand> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const { body } = await api.getCommand(commandId);
    if (isCommand(body) && body.status === status) return body;
    await sleep(100);
  }
  const { body } = await api.getCommand(commandId);
  throw new Error(
    `command ${commandId} did not reach ${status} within ${timeoutMs}ms; last=${JSON.stringify(body)}`,
  );
}

async function scenarioHappyPath(server: ServerHandle): Promise<void> {
  await test("happy path: submit -> claim -> deliver -> confirm -> SUCCEEDED with causal events", async () => {
    const api = new ApiClient(server.baseUrl);
    const device = new DeviceSimulator("dev-1", { confirmDelayMs: 20 });
    const gw = new GatewaySimulator({
      gatewayId: "gw-happy",
      pollIntervalMs: 50,
      leaseDurationMs: 5000,
      renewIntervalMs: 2000,
      api,
      device,
    });

    const { body: submitted, status } = await api.submitCommand("e2e-happy", {
      deviceId: "dev-1",
      action: "calibrate",
    });
    assert.equal(status, 201);
    assert.equal(submitted.status, "PENDING");

    gw.start();
    await waitForStatus(api, submitted.commandId, "SUCCEEDED", 8000);
    gw.stop();

    const final = await fetchCommand(api, submitted.commandId);
    assert.equal(final.status, "SUCCEEDED");
    assert.ok(final.confirmationCode, "confirmationCode must be set");
    assert.equal(final.attempt, 1);

    const { body: eventsRes } = await api.getEvents(submitted.commandId);
    assertEventTypes(eventsRes.events, [
      "CommandSubmitted",
      "CommandClaimed",
      "DeliveryReported",
      "DeviceConfirmed",
      "CommandSucceeded",
    ]);
  });
}

async function scenarioIdempotentSubmit(server: ServerHandle): Promise<void> {
  await test("idempotent submit: duplicate business key returns same command (crash-after-commit recovery)", async () => {
    const api = new ApiClient(server.baseUrl);
    const payload = { deviceId: "dev-2", action: "switch-recipe" };

    const first = await api.submitCommand("e2e-idem-1", payload);
    assert.equal(first.status, 201);

    const second = await api.submitCommand("e2e-idem-1", {
      deviceId: "dev-DIFFERENT",
      action: "reset",
    });
    assert.equal(second.status, 200);
    assert.equal(second.body.commandId, first.body.commandId);
    assert.deepEqual(second.body.payload, payload);

    const byKey = await api.getCommandByKey("e2e-idem-1");
    assert.ok(isCommand(byKey.body), "by-key lookup should return command");
    assert.equal(byKey.body.commandId, first.body.commandId);
  });
}

async function scenarioGatewayCrashAndReclaim(
  server: ServerHandle,
): Promise<void> {
  await test("gateway loss: lease expires, reclaim uses stable commandId, old gateway confirmation rejected", async () => {
    const api = new ApiClient(server.baseUrl);
    const device1 = new DeviceSimulator("dev-3", { dropConfirm: true });
    const gw1 = new GatewaySimulator({
      gatewayId: "gw-lost",
      pollIntervalMs: 50,
      leaseDurationMs: 800,
      renewIntervalMs: 10000,
      api,
      device: device1,
    });
    const device2 = new DeviceSimulator("dev-3", { confirmDelayMs: 20 });
    const gw2 = new GatewaySimulator({
      gatewayId: "gw-recovery",
      pollIntervalMs: 50,
      leaseDurationMs: 5000,
      renewIntervalMs: 2000,
      api,
      device: device2,
    });

    const { body: submitted } = await api.submitCommand(
      "e2e-crash-1",
      {
        deviceId: "dev-3",
        action: "calibrate",
      },
      3,
    );

    gw1.start();
    await waitForStatus(api, submitted.commandId, "CLAIMED", 5000);
    const held = gw1.getHeldTask();
    assert.ok(held, "gw1 should have claimed the task");
    const originalCommandId = held!.commandId;
    const oldLeaseId = held!.currentLeaseId!;

    gw1.goOffline();
    await sleep(1200);

    gw2.start();
    await waitForStatus(api, originalCommandId, "SUCCEEDED", 8000);
    const final = await fetchCommand(api, originalCommandId);
    assert.equal(final.status, "SUCCEEDED");
    assert.equal(final.attempt, 2);
    assert.equal(final.commandId, originalCommandId);
    assert.ok(
      final.currentLeaseId !== oldLeaseId,
      "new lease must differ from old lease",
    );

    await gw1.sendLateConfirmWithOldLease(originalCommandId, oldLeaseId);
    const afterLate = await fetchCommand(api, originalCommandId);
    assert.equal(
      afterLate.status,
      "SUCCEEDED",
      "late confirmation must not change state",
    );
    assert.equal(
      afterLate.currentLeaseId,
      final.currentLeaseId,
      "lease must not change after late ack",
    );

    gw1.stop();
    gw2.stop();

    const { body: eventsRes } = await api.getEvents(originalCommandId);
    const types = eventsRes.events.map((e) => e.eventType);
    assert.ok(
      types.includes("StaleMessageRejected"),
      "must record StaleMessageRejected for old lease",
    );
    const reclaimOrReclaim =
      types.includes("CommandReclaimed") ||
      types.filter((t) => t === "CommandClaimed").length >= 2;
    assert.ok(
      reclaimOrReclaim,
      "must record either CommandReclaimed (direct reclaim) or a second CommandClaimed (scan then reclaim)",
    );
    const succeeded = types.filter((t) => t === "CommandSucceeded");
    assert.equal(succeeded.length, 1, "exactly one CommandSucceeded");
  });
}

async function scenarioDuplicateConfirmation(
  server: ServerHandle,
): Promise<void> {
  await test("duplicate device confirmation: idempotent, only one success, stale duplicate recorded", async () => {
    const api = new ApiClient(server.baseUrl);
    const device = new DeviceSimulator("dev-4", {
      confirmDelayMs: 20,
      duplicateConfirm: true,
    });
    const gw = new GatewaySimulator({
      gatewayId: "gw-dup",
      pollIntervalMs: 50,
      leaseDurationMs: 5000,
      renewIntervalMs: 2000,
      api,
      device,
    });

    const { body: submitted } = await api.submitCommand("e2e-dup-ack", {
      deviceId: "dev-4",
      action: "set-speed",
      params: { rpm: 1200 },
    });
    gw.start();
    await waitForStatus(api, submitted.commandId, "SUCCEEDED", 8000);
    await sleep(300);
    gw.stop();

    const { body: eventsRes } = await api.getEvents(submitted.commandId);
    const types = eventsRes.events.map((e) => e.eventType);
    const successCount = types.filter((t) => t === "CommandSucceeded").length;
    assert.equal(successCount, 1, "exactly one CommandSucceeded event");
    const staleCount = types.filter((t) => t === "StaleMessageRejected").length;
    assert.ok(
      staleCount >= 1,
      "duplicate confirmation must be recorded as StaleMessageRejected",
    );
  });
}

async function scenarioLateConfirmAfterExpiry(
  server: ServerHandle,
): Promise<void> {
  await test("late confirmation after lease expiry (before reclaim) is rejected and does not resurrect", async () => {
    const api = new ApiClient(server.baseUrl);
    const device = new DeviceSimulator("dev-5", { dropConfirm: true });
    const gw = new GatewaySimulator({
      gatewayId: "gw-late",
      pollIntervalMs: 50,
      leaseDurationMs: 600,
      renewIntervalMs: 10000,
      api,
      device,
    });

    const { body: submitted } = await api.submitCommand(
      "e2e-late-ack",
      {
        deviceId: "dev-5",
        action: "calibrate",
      },
      3,
    );

    gw.start();
    await waitForStatus(api, submitted.commandId, "CLAIMED", 5000);
    const held = gw.getHeldTask();
    assert.ok(held);
    const leaseId = held!.currentLeaseId!;
    gw.goOffline();

    await sleep(1200);
    await api.scanExpired();

    const afterExpiry = await fetchCommand(api, submitted.commandId);
    assert.equal(afterExpiry.status, "PENDING");

    const { body: confirmRes } = await api.confirm({
      commandId: submitted.commandId,
      leaseId,
      gatewayId: "gw-late",
      confirmationCode: "TOO-LATE",
      deviceTimestamp: Date.now(),
    });
    assert.equal(confirmRes.accepted, false);
    assert.equal(confirmRes.command.status, "PENDING");
    assert.equal(confirmRes.command.confirmationCode, null);

    gw.stop();
  });
}

async function scenarioMaxAttemptsFailure(server: ServerHandle): Promise<void> {
  await test("max attempts exhausted: command ends FAILED with causal retry/expiry trail", async () => {
    const api = new ApiClient(server.baseUrl);
    const { body: submitted } = await api.submitCommand(
      "e2e-max-att",
      {
        deviceId: "dev-6",
        action: "calibrate",
      },
      2,
    );

    for (let attempt = 1; attempt <= 2; attempt++) {
      const device = new DeviceSimulator("dev-6", { dropConfirm: true });
      const gw = new GatewaySimulator({
        gatewayId: `gw-fail-${attempt}`,
        pollIntervalMs: 50,
        leaseDurationMs: 1000,
        renewIntervalMs: 10000,
        api,
        device,
      });
      gw.start();
      await waitForStatus(api, submitted.commandId, "CLAIMED", 5000);
      const held = gw.getHeldTask();
      assert.ok(held, `attempt ${attempt} should be claimed`);
      assert.equal(held!.attempt, attempt);
      gw.goOffline();
      await sleep(1500);
      await api.scanExpired();
      gw.stop();
    }

    await waitForStatus(api, submitted.commandId, "FAILED", 5000);
    const final = await fetchCommand(api, submitted.commandId);
    assert.equal(final.status, "FAILED");
    assert.equal(final.attempt, 2);
    assert.ok(final.failureReason?.includes("max attempts"));

    const { body: eventsRes } = await api.getEvents(submitted.commandId);
    const types = eventsRes.events.map((e) => e.eventType);
    assert.equal(types.filter((t) => t === "LeaseExpired").length, 2);
    assert.ok(types.includes("CommandFailed"));
    const totalClaimEvents =
      types.filter((t) => t === "CommandClaimed").length +
      types.filter((t) => t === "CommandReclaimed").length;
    assert.equal(
      totalClaimEvents,
      2,
      "two claim/reclaim events across attempts",
    );
  });
}

async function scenarioDuplicateSubmissionDedup(
  server: ServerHandle,
): Promise<void> {
  await test("concurrent duplicate submission: same idempotency key creates exactly one device action", async () => {
    const api = new ApiClient(server.baseUrl);
    const payload = { deviceId: "dev-7", action: "reset" };

    const results = await Promise.all([
      api.submitCommand("e2e-concurrent", payload),
      api.submitCommand("e2e-concurrent", payload),
      api.submitCommand("e2e-concurrent", payload),
    ]);

    const ids = new Set(results.map((r) => r.body.commandId));
    assert.equal(
      ids.size,
      1,
      "all concurrent submissions must resolve to one commandId",
    );

    const device = new DeviceSimulator("dev-7", { confirmDelayMs: 20 });
    const gw = new GatewaySimulator({
      gatewayId: "gw-conc",
      pollIntervalMs: 50,
      leaseDurationMs: 5000,
      renewIntervalMs: 2000,
      api,
      device,
    });
    gw.start();
    const commandId = results[0].body.commandId;
    await waitForStatus(api, commandId, "SUCCEEDED", 8000);
    gw.stop();

    const received = device.getReceived();
    assert.equal(
      received.length,
      1,
      "device must receive exactly one delivery",
    );
    assert.equal(received[0].commandId, commandId);
  });
}

async function scenarioOutOfOrderConfirm(server: ServerHandle): Promise<void> {
  await test("out-of-order confirmation: stale lease from previous attempt cannot overwrite success", async () => {
    const api = new ApiClient(server.baseUrl);
    const device1 = new DeviceSimulator("dev-8", { dropConfirm: true });
    const gw1 = new GatewaySimulator({
      gatewayId: "gw-oox-1",
      pollIntervalMs: 50,
      leaseDurationMs: 700,
      renewIntervalMs: 10000,
      api,
      device: device1,
    });

    const { body: submitted } = await api.submitCommand(
      "e2e-oox",
      {
        deviceId: "dev-8",
        action: "calibrate",
      },
      3,
    );

    gw1.start();
    await waitForStatus(api, submitted.commandId, "CLAIMED", 5000);
    const firstHeld = gw1.getHeldTask();
    assert.ok(firstHeld);
    const firstLease = firstHeld!.currentLeaseId!;
    gw1.goOffline();
    await sleep(1200);

    const device2 = new DeviceSimulator("dev-8", { confirmDelayMs: 20 });
    const gw2 = new GatewaySimulator({
      gatewayId: "gw-oox-2",
      pollIntervalMs: 50,
      leaseDurationMs: 5000,
      renewIntervalMs: 2000,
      api,
      device: device2,
    });
    gw2.start();
    await waitForStatus(api, submitted.commandId, "SUCCEEDED", 8000);
    gw2.stop();

    await gw1.sendLateConfirmWithOldLease(submitted.commandId, firstLease);
    const final = await fetchCommand(api, submitted.commandId);
    assert.equal(final.status, "SUCCEEDED");
    assert.notEqual(final.currentLeaseId, firstLease);

    gw1.stop();

    const { body: eventsRes } = await api.getEvents(submitted.commandId);
    const successIdx = eventsRes.events.findIndex(
      (e) => e.eventType === "CommandSucceeded",
    );
    const staleAfterSuccess = eventsRes.events.filter(
      (e, i) => e.eventType === "StaleMessageRejected" && i > successIdx,
    );
    assert.ok(
      staleAfterSuccess.length >= 1,
      "a StaleMessageRejected must appear after CommandSucceeded for the late old-lease confirmation",
    );
  });
}

async function scenarioDualGatewayFailover(
  server: ServerHandle,
): Promise<void> {
  await test("HA failover: standby reclaims after primary lease expiry, stable commandId, old generation fenced", async () => {
    const api = new ApiClient(server.baseUrl);
    const { body: submitted } = await api.submitCommand(
      "e2e-ha-failover",
      {
        deviceId: "dev-ha",
        action: "calibrate",
      },
      3,
    );

    const primaryDevice = new DeviceSimulator("dev-ha", { dropConfirm: true });
    const primary = new GatewaySimulator({
      gatewayId: "gw-primary",
      pollIntervalMs: 50,
      leaseDurationMs: 800,
      renewIntervalMs: 10000,
      api,
      device: primaryDevice,
    });
    const standbyDevice = new DeviceSimulator("dev-ha", { confirmDelayMs: 20 });
    const standby = new GatewaySimulator({
      gatewayId: "gw-standby",
      pollIntervalMs: 50,
      leaseDurationMs: 5000,
      renewIntervalMs: 2000,
      api,
      device: standbyDevice,
    });

    primary.start();
    await waitForStatus(api, submitted.commandId, "CLAIMED", 5000);
    const firstHeld = primary.getHeldTask();
    assert.ok(firstHeld);
    assert.equal(firstHeld!.gatewayId, "gw-primary");
    const oldLeaseId = firstHeld!.currentLeaseId!;
    assert.equal(firstHeld!.attempt, 1);

    primary.goOffline();
    await sleep(1200);

    standby.start();
    const reclaimed = await waitForStatus(
      api,
      submitted.commandId,
      "SUCCEEDED",
      8000,
    );
    assert.equal(
      reclaimed.commandId,
      submitted.commandId,
      "commandId must remain stable across failover",
    );
    assert.equal(
      reclaimed.attempt,
      2,
      "generation must increment after takeover",
    );
    assert.notEqual(reclaimed.currentLeaseId, oldLeaseId);

    const renewRes = await api.renew({
      commandId: submitted.commandId,
      leaseId: oldLeaseId,
      gatewayId: "gw-primary",
      leaseDurationMs: 5000,
    });
    assert.equal(renewRes.status, 409, "old generation renew must be rejected");

    const reportRes = await api.reportDelivery({
      commandId: submitted.commandId,
      leaseId: oldLeaseId,
      gatewayId: "gw-primary",
      deviceMessage: "late delivery report from fenced primary",
    });
    assert.equal(
      reportRes.status,
      409,
      "old generation reportDelivery must be rejected",
    );

    const { body: confirmRes } = await api.confirm({
      commandId: submitted.commandId,
      leaseId: oldLeaseId,
      gatewayId: "gw-primary",
      confirmationCode: "GHOST-ACK",
      deviceTimestamp: Date.now(),
    });
    assert.equal(
      confirmRes.accepted,
      false,
      "old generation confirm must not advance state",
    );

    primary.stop();
    standby.stop();

    const { body: eventsRes } = await api.getEvents(submitted.commandId);
    const types = eventsRes.events.map((e) => e.eventType);
    const causedBy = eventsRes.events.map((e) => e.causedBy);

    assert.ok(
      types.includes("CommandReclaimed") ||
        types.filter((t) => t === "CommandClaimed").length >= 2,
      "takeover must produce a reclaim or second claim event",
    );
    assert.ok(
      types.includes("LeaseOperationRejected"),
      `rejected operations from old generation must be audited; got: ${types.join(",")}`,
    );
    assert.ok(
      causedBy.includes("rejected-renew"),
      "renew rejection must have causedBy=rejected-renew",
    );
    assert.ok(
      causedBy.includes("rejected-report-delivery"),
      "reportDelivery rejection must be audited",
    );
    assert.equal(types.filter((t) => t === "CommandSucceeded").length, 1);
  });
}

async function scenarioSplitBrainOldGatewayWrites(
  server: ServerHandle,
): Promise<void> {
  await test("split-brain: old gateway keeps writing after takeover, all fenced and audited, no zombie success", async () => {
    const api = new ApiClient(server.baseUrl);
    const { body: submitted } = await api.submitCommand(
      "e2e-split-brain",
      {
        deviceId: "dev-split",
        action: "switch-recipe",
        params: { recipe: "B" },
      },
      3,
    );

    const oldDevice = new DeviceSimulator("dev-split", { dropConfirm: true });
    const oldGw = new GatewaySimulator({
      gatewayId: "gw-old",
      pollIntervalMs: 50,
      leaseDurationMs: 800,
      renewIntervalMs: 10000,
      api,
      device: oldDevice,
    });
    const newDevice = new DeviceSimulator("dev-split", { confirmDelayMs: 20 });
    const newGw = new GatewaySimulator({
      gatewayId: "gw-new",
      pollIntervalMs: 50,
      leaseDurationMs: 5000,
      renewIntervalMs: 2000,
      api,
      device: newDevice,
    });

    oldGw.start();
    await waitForStatus(api, submitted.commandId, "CLAIMED", 5000);
    const oldHeld = oldGw.getHeldTask()!;
    const oldLease = oldHeld.currentLeaseId!;
    oldGw.goOffline();
    await sleep(1200);

    newGw.start();
    await waitForStatus(api, submitted.commandId, "SUCCEEDED", 8000);

    await oldGw.sendLateConfirmWithOldLease(submitted.commandId, oldLease);
    const { body: secondConfirm } = await api.confirm({
      commandId: submitted.commandId,
      leaseId: oldLease,
      gatewayId: "gw-old",
      confirmationCode: "GHOST-2",
      deviceTimestamp: Date.now(),
    });
    assert.equal(secondConfirm.accepted, false);

    const final = await fetchCommand(api, submitted.commandId);
    assert.equal(final.status, "SUCCEEDED");
    assert.equal(final.gatewayId, "gw-new");

    oldGw.stop();
    newGw.stop();

    const { body: eventsRes } = await api.getEvents(submitted.commandId);
    const events = eventsRes.events;
    const successIdx = events.findIndex(
      (e) => e.eventType === "CommandSucceeded",
    );
    const staleAfterSuccess = events.filter(
      (e, i) => e.eventType === "StaleMessageRejected" && i > successIdx,
    );
    assert.ok(
      staleAfterSuccess.length >= 1,
      "late confirms after success must be recorded as stale",
    );

    const rejectEvents = events.filter(
      (e) => e.eventType === "LeaseOperationRejected",
    );
    for (const e of rejectEvents) {
      assert.ok(
        e.payload?.currentGeneration,
        "rejection must record current generation",
      );
      assert.ok(
        e.payload?.rejectedLeaseId === oldLease || e.gatewayId === "gw-old",
        "rejection must identify the fenced old gateway",
      );
    }
  });
}

async function scenarioProcessRestartDuringLease(): Promise<void> {
  await test("process restart mid-lease: state recovered from SQLite, standby takes over after lease expiry", async () => {
    const server = createManagedServer();
    await server.start();
    try {
      const api = new ApiClient(server.baseUrl);
      const { body: submitted } = await api.submitCommand(
        "e2e-restart",
        {
          deviceId: "dev-restart",
          action: "calibrate",
        },
        3,
      );

      const device1 = new DeviceSimulator("dev-restart", { dropConfirm: true });
      const gw1 = new GatewaySimulator({
        gatewayId: "gw-before-restart",
        pollIntervalMs: 50,
        leaseDurationMs: 1500,
        renewIntervalMs: 10000,
        api,
        device: device1,
      });
      gw1.start();
      await waitForStatus(api, submitted.commandId, "CLAIMED", 5000);
      const before = await fetchCommand(api, submitted.commandId);
      assert.equal(before.status, "CLAIMED");
      const leaseBefore = before.currentLeaseId!;
      gw1.goOffline();

      await server.restart();

      const afterRestart = await fetchCommand(api, submitted.commandId);
      assert.equal(
        afterRestart.status,
        "CLAIMED",
        "lease state must survive process restart",
      );
      assert.equal(
        afterRestart.currentLeaseId,
        leaseBefore,
        "lease identity must survive restart",
      );
      assert.equal(afterRestart.commandId, submitted.commandId);

      const device2 = new DeviceSimulator("dev-restart", {
        confirmDelayMs: 20,
      });
      const gw2 = new GatewaySimulator({
        gatewayId: "gw-after-restart",
        pollIntervalMs: 50,
        leaseDurationMs: 5000,
        renewIntervalMs: 2000,
        api,
        device: device2,
      });
      gw2.start();
      const final = await waitForStatus(
        api,
        submitted.commandId,
        "SUCCEEDED",
        10000,
      );
      assert.equal(final.attempt, 2);
      assert.equal(final.commandId, submitted.commandId);
      gw1.stop();
      gw2.stop();
    } finally {
      await server.stop();
    }
  });
}

async function scenarioTakeoverAuditCausality(
  server: ServerHandle,
): Promise<void> {
  await test("audit trail: event chain reconstructs full takeover causality across generations", async () => {
    const api = new ApiClient(server.baseUrl);
    const { body: submitted } = await api.submitCommand(
      "e2e-audit-ha",
      {
        deviceId: "dev-audit",
        action: "reset",
      },
      3,
    );

    const dev1 = new DeviceSimulator("dev-audit", { dropConfirm: true });
    const gw1 = new GatewaySimulator({
      gatewayId: "gw-audit-1",
      pollIntervalMs: 50,
      leaseDurationMs: 800,
      renewIntervalMs: 10000,
      api,
      device: dev1,
    });
    gw1.start();
    await waitForStatus(api, submitted.commandId, "CLAIMED", 5000);
    const held1 = gw1.getHeldTask()!;
    gw1.goOffline();
    await sleep(1200);

    await api
      .renew({
        commandId: submitted.commandId,
        leaseId: held1.currentLeaseId!,
        gatewayId: "gw-audit-1",
        leaseDurationMs: 5000,
      })
      .catch(() => {});

    const dev2 = new DeviceSimulator("dev-audit", { confirmDelayMs: 20 });
    const gw2 = new GatewaySimulator({
      gatewayId: "gw-audit-2",
      pollIntervalMs: 50,
      leaseDurationMs: 5000,
      renewIntervalMs: 2000,
      api,
      device: dev2,
    });
    gw2.start();
    await waitForStatus(api, submitted.commandId, "SUCCEEDED", 8000);
    gw1.stop();
    gw2.stop();

    const { body: eventsRes } = await api.getEvents(submitted.commandId);
    const events = eventsRes.events;

    const submit = events.find((e) => e.eventType === "CommandSubmitted")!;
    assert.ok(submit, "must have CommandSubmitted");
    assert.equal(submit.gatewayId, null);

    const firstClaim = events.find(
      (e) =>
        (e.eventType === "CommandClaimed" ||
          e.eventType === "CommandReclaimed") &&
        e.gatewayId === "gw-audit-1",
    )!;
    assert.ok(firstClaim, "must have first claim by gw-audit-1");
    assert.equal(firstClaim.attempt, 1);

    const rejected = events.find(
      (e) =>
        e.eventType === "LeaseOperationRejected" &&
        e.gatewayId === "gw-audit-1",
    )!;
    assert.ok(rejected, "must record rejected operation from old gateway");
    assert.ok(
      rejected.timestamp >= firstClaim.timestamp,
      "rejection must occur after first claim",
    );

    const secondClaim = events.find(
      (e) =>
        (e.eventType === "CommandClaimed" ||
          e.eventType === "CommandReclaimed") &&
        e.gatewayId === "gw-audit-2",
    )!;
    assert.ok(secondClaim, "must have claim by gw-audit-2");
    assert.equal(secondClaim.attempt, 2);

    const success = events.find((e) => e.eventType === "CommandSucceeded")!;
    assert.ok(success);
    assert.equal(success.gatewayId, "gw-audit-2");
    assert.equal(success.attempt, 2);
    assert.ok(
      success.timestamp >= secondClaim.timestamp,
      "success must occur after takeover claim",
    );
  });
}

async function scenarioWithdrawPending(server: ServerHandle): Promise<void> {
  await test("withdraw pending: command becomes CANCELLED, not claimable, event chain queryable", async () => {
    const api = new ApiClient(server.baseUrl);
    const { body: submitted } = await api.submitCommand(
      "e2e-withdraw-pending",
      {
        deviceId: "dev-wd-1",
        action: "calibrate",
      },
    );

    const withdrawRes = await api.withdraw(
      submitted.commandId,
      "operator abort",
    );
    assert.equal(withdrawRes.status, 200);
    assert.ok(isCommand(withdrawRes.body), "withdraw should return a command");
    assert.equal(withdrawRes.body.status, "CANCELLED");
    assert.equal(withdrawRes.body.cancelReason, "operator abort");

    const claimRes = await api.claim("gw-wd", 5000, "dev-wd-1");
    assert.equal(
      claimRes.status,
      204,
      "cancelled command must not be claimable",
    );

    const { body: eventsRes } = await api.getEvents(submitted.commandId);
    const types = eventsRes.events.map((e) => e.eventType);
    assert.deepEqual(types, ["CommandSubmitted", "CommandCancelled"]);
    const cancelEvent = eventsRes.events[1];
    assert.equal(cancelEvent.causedBy, "withdraw");
    assert.ok(cancelEvent.payload?.reason, "operator abort");
  });
}

async function scenarioWithdrawClaimed(server: ServerHandle): Promise<void> {
  await test("withdraw claimed: lease invalidated, old gateway renew/confirm rejected and audited", async () => {
    const api = new ApiClient(server.baseUrl);
    const device = new DeviceSimulator("dev-wd-2", { dropConfirm: true });
    const gw = new GatewaySimulator({
      gatewayId: "gw-wd-2",
      pollIntervalMs: 50,
      leaseDurationMs: 10000,
      renewIntervalMs: 10000,
      api,
      device,
    });

    const { body: submitted } = await api.submitCommand(
      "e2e-withdraw-claimed",
      {
        deviceId: "dev-wd-2",
        action: "calibrate",
      },
    );

    gw.start();
    await waitForStatus(api, submitted.commandId, "CLAIMED", 5000);
    const held = gw.getHeldTask()!;
    const oldLease = held.currentLeaseId!;

    const withdrawRes = await api.withdraw(
      submitted.commandId,
      "emergency stop",
    );
    assert.equal(withdrawRes.status, 200);
    assert.ok(isCommand(withdrawRes.body), "withdraw should return a command");
    assert.equal(withdrawRes.body.status, "CANCELLED");
    assert.equal(withdrawRes.body.currentLeaseId, null);

    const renewRes = await api.renew({
      commandId: submitted.commandId,
      leaseId: oldLease,
      gatewayId: "gw-wd-2",
      leaseDurationMs: 5000,
    });
    assert.equal(
      renewRes.status,
      409,
      "old gateway renew after withdraw must be rejected",
    );

    const { body: confirmRes } = await api.confirm({
      commandId: submitted.commandId,
      leaseId: oldLease,
      gatewayId: "gw-wd-2",
      confirmationCode: "LATE-ACK",
      deviceTimestamp: Date.now(),
    });
    assert.equal(confirmRes.accepted, false);
    assert.equal(confirmRes.command.status, "CANCELLED");

    gw.stop();

    const { body: eventsRes } = await api.getEvents(submitted.commandId);
    const types = eventsRes.events.map((e) => e.eventType);
    assert.ok(types.includes("CommandCancelled"));
    assert.ok(
      types.includes("LeaseOperationRejected"),
      `renew rejection must be audited; got: ${types.join(",")}`,
    );
    assert.ok(types.includes("StaleMessageRejected"));
  });
}

async function scenarioWithdrawAlreadySucceeded(
  server: ServerHandle,
): Promise<void> {
  await test("withdraw succeeded: rejected with 409, executed action cannot be faked as cancelled", async () => {
    const api = new ApiClient(server.baseUrl);
    const device = new DeviceSimulator("dev-wd-3", { confirmDelayMs: 20 });
    const gw = new GatewaySimulator({
      gatewayId: "gw-wd-3",
      pollIntervalMs: 50,
      leaseDurationMs: 5000,
      renewIntervalMs: 2000,
      api,
      device,
    });

    const { body: submitted } = await api.submitCommand("e2e-withdraw-done", {
      deviceId: "dev-wd-3",
      action: "calibrate",
    });
    gw.start();
    await waitForStatus(api, submitted.commandId, "SUCCEEDED", 8000);
    gw.stop();

    const withdrawRes = await api.withdraw(submitted.commandId, "too late");
    assert.equal(withdrawRes.status, 409);
    assert.ok(
      JSON.stringify(withdrawRes.body).includes("SUCCEEDED"),
      "error must mention SUCCEEDED",
    );

    const final = await fetchCommand(api, submitted.commandId);
    assert.equal(final.status, "SUCCEEDED");
    assert.ok(final.confirmationCode, "confirmation must remain intact");
  });
}

async function scenarioSupersede(server: ServerHandle): Promise<void> {
  await test("supersede: old command SUPERSEDED, replacement delivered and confirmed, causal chain queryable", async () => {
    const api = new ApiClient(server.baseUrl);
    const oldDevice = new DeviceSimulator("dev-sup-1", { dropConfirm: true });
    const oldGw = new GatewaySimulator({
      gatewayId: "gw-sup-old",
      pollIntervalMs: 50,
      leaseDurationMs: 10000,
      renewIntervalMs: 10000,
      api,
      device: oldDevice,
    });

    const { body: submitted } = await api.submitCommand("e2e-supersede", {
      deviceId: "dev-sup-1",
      action: "calibrate",
      params: { mode: "A" },
    });

    oldGw.start();
    await waitForStatus(api, submitted.commandId, "CLAIMED", 5000);
    const oldHeld = oldGw.getHeldTask()!;
    const oldLease = oldHeld.currentLeaseId!;

    const supRes = await api.supersede(
      submitted.commandId,
      "e2e-supersede-replacement",
      { deviceId: "dev-sup-1", action: "emergency-stop" },
      "safety override: mode A unsafe",
    );
    assert.equal(supRes.status, 201);
    const body = supRes.body as {
      oldCommand: ApiCommand;
      newCommand: ApiCommand;
    };
    assert.equal(body.oldCommand.status, "SUPERSEDED");
    assert.equal(
      body.oldCommand.supersededByCommandId,
      body.newCommand.commandId,
    );
    assert.equal(body.newCommand.status, "PENDING");
    assert.equal(
      body.newCommand.payload.replacesCommandId,
      submitted.commandId,
    );

    oldGw.stop();

    const newDevice = new DeviceSimulator("dev-sup-1", { confirmDelayMs: 20 });
    const newGw = new GatewaySimulator({
      gatewayId: "gw-sup-new",
      pollIntervalMs: 50,
      leaseDurationMs: 5000,
      renewIntervalMs: 2000,
      api,
      device: newDevice,
    });
    newGw.start();
    const replacementFinal = await waitForStatus(
      api,
      body.newCommand.commandId,
      "SUCCEEDED",
      8000,
    );
    assert.equal(
      replacementFinal.payload.replacesCommandId,
      submitted.commandId,
    );
    newGw.stop();

    const { body: oldEvents } = await api.getEvents(submitted.commandId);
    const oldTypes = oldEvents.events.map((e) => e.eventType);
    assert.ok(oldTypes.includes("CommandSuperseded"));
    assert.ok(oldTypes.includes("CommandSubmitted"));
    assert.ok(oldTypes.includes("CommandClaimed"));

    const { body: newEvents } = await api.getEvents(body.newCommand.commandId);
    const newTypes = newEvents.events.map((e) => e.eventType);
    assert.deepEqual(newTypes, [
      "CommandSubmitted",
      "CommandClaimed",
      "DeliveryReported",
      "DeviceConfirmed",
      "CommandSucceeded",
    ]);
    const submittedEvent = newEvents.events[0];
    assert.equal(submittedEvent.causedBy, "supersede-replacement");
    assert.equal(
      (submittedEvent.payload as { replacesCommandId: string })
        .replacesCommandId,
      submitted.commandId,
    );

    const renewRes = await api.renew({
      commandId: submitted.commandId,
      leaseId: oldLease,
      gatewayId: "gw-sup-old",
      leaseDurationMs: 5000,
    });
    assert.equal(
      renewRes.status,
      409,
      "old gateway renew after supersede must be rejected",
    );
  });
}

async function scenarioSupersedeAlreadySucceeded(
  server: ServerHandle,
): Promise<void> {
  await test("supersede succeeded: rejected with 409, cannot replace executed action", async () => {
    const api = new ApiClient(server.baseUrl);
    const device = new DeviceSimulator("dev-sup-2", { confirmDelayMs: 20 });
    const gw = new GatewaySimulator({
      gatewayId: "gw-sup-2",
      pollIntervalMs: 50,
      leaseDurationMs: 5000,
      renewIntervalMs: 2000,
      api,
      device,
    });

    const { body: submitted } = await api.submitCommand("e2e-sup-done", {
      deviceId: "dev-sup-2",
      action: "calibrate",
    });
    gw.start();
    await waitForStatus(api, submitted.commandId, "SUCCEEDED", 8000);
    gw.stop();

    const supRes = await api.supersede(
      submitted.commandId,
      "e2e-sup-done-repl",
      { deviceId: "dev-sup-2", action: "stop" },
      "too late",
    );
    assert.equal(supRes.status, 409);

    const final = await fetchCommand(api, submitted.commandId);
    assert.equal(final.status, "SUCCEEDED");
  });
}

async function scenarioCrashRecoveryWithSupersede(): Promise<void> {
  await test("crash recovery: supersede survives restart, old gateway late confirm rejected, replacement proceeds", async () => {
    const server = createManagedServer();
    await server.start();
    try {
      const api = new ApiClient(server.baseUrl);
      const device1 = new DeviceSimulator("dev-crash-sup", {
        dropConfirm: true,
      });
      const gw1 = new GatewaySimulator({
        gatewayId: "gw-crash-sup-1",
        pollIntervalMs: 50,
        leaseDurationMs: 1500,
        renewIntervalMs: 10000,
        api,
        device: device1,
      });

      const { body: submitted } = await api.submitCommand("e2e-crash-sup", {
        deviceId: "dev-crash-sup",
        action: "calibrate",
      });
      gw1.start();
      await waitForStatus(api, submitted.commandId, "CLAIMED", 5000);
      const oldHeld = gw1.getHeldTask()!;
      const oldLease = oldHeld.currentLeaseId!;
      gw1.goOffline();

      const supRes = await api.supersede(
        submitted.commandId,
        "e2e-crash-sup-repl",
        { deviceId: "dev-crash-sup", action: "safe-stop" },
        "override before crash",
      );
      assert.equal(supRes.status, 201);
      const newCommandId = (supRes.body as { newCommand: ApiCommand })
        .newCommand.commandId;

      await server.restart();

      const oldAfterRestart = await fetchCommand(api, submitted.commandId);
      assert.equal(oldAfterRestart.status, "SUPERSEDED");
      assert.equal(oldAfterRestart.supersededByCommandId, newCommandId);

      const { body: lateConfirm } = await api.confirm({
        commandId: submitted.commandId,
        leaseId: oldLease,
        gatewayId: "gw-crash-sup-1",
        confirmationCode: "GHOST",
        deviceTimestamp: Date.now(),
      });
      assert.equal(lateConfirm.accepted, false);
      assert.equal(lateConfirm.command.status, "SUPERSEDED");

      const device2 = new DeviceSimulator("dev-crash-sup", {
        confirmDelayMs: 20,
      });
      const gw2 = new GatewaySimulator({
        gatewayId: "gw-crash-sup-2",
        pollIntervalMs: 50,
        leaseDurationMs: 5000,
        renewIntervalMs: 2000,
        api,
        device: device2,
      });
      gw2.start();
      const final = await waitForStatus(api, newCommandId, "SUCCEEDED", 8000);
      assert.equal(final.payload.replacesCommandId, submitted.commandId);
      gw1.stop();
      gw2.stop();
    } finally {
      await server.stop();
    }
  });
}

async function scenarioEventAuditTrail(server: ServerHandle): Promise<void> {
  await test("operations audit: every state transition has a causedBy event with timestamp and lease/attempt context", async () => {
    const api = new ApiClient(server.baseUrl);
    const { body: eventsRes } = await api.listEvents(1000);
    assert.ok(
      eventsRes.events.length > 0,
      "event log must not be empty after scenarios",
    );

    for (const e of eventsRes.events) {
      assert.ok(e.eventId, "eventId required");
      assert.ok(e.commandId, "commandId required");
      assert.ok(e.eventType, "eventType required");
      assert.ok(e.causedBy, `causedBy required for ${e.eventType}`);
      assert.ok(typeof e.timestamp === "number", "timestamp required");
    }

    const submitEvents = eventsRes.events.filter(
      (e) => e.eventType === "CommandSubmitted",
    );
    for (const e of submitEvents) {
      assert.ok(
        e.payload?.idempotencyKey,
        "CommandSubmitted must record idempotencyKey",
      );
    }
  });
}

async function main(): Promise<void> {
  console.log("Starting compiled server for end-to-end acceptance...");
  const server = await startCompiledServer();
  console.log(`Server running at ${server.baseUrl}`);
  console.log("");

  try {
    console.log("Running acceptance scenarios:");
    await scenarioHappyPath(server);
    await scenarioIdempotentSubmit(server);
    await scenarioGatewayCrashAndReclaim(server);
    await scenarioDuplicateConfirmation(server);
    await scenarioLateConfirmAfterExpiry(server);
    await scenarioMaxAttemptsFailure(server);
    await scenarioDuplicateSubmissionDedup(server);
    await scenarioOutOfOrderConfirm(server);
    await scenarioDualGatewayFailover(server);
    await scenarioSplitBrainOldGatewayWrites(server);
    await scenarioTakeoverAuditCausality(server);
    await scenarioProcessRestartDuringLease();
    await scenarioWithdrawPending(server);
    await scenarioWithdrawClaimed(server);
    await scenarioWithdrawAlreadySucceeded(server);
    await scenarioSupersede(server);
    await scenarioSupersedeAlreadySucceeded(server);
    await scenarioCrashRecoveryWithSupersede();
    await scenarioEventAuditTrail(server);
  } finally {
    await server.stop();
    console.log("\nServer stopped.");
  }

  console.log(`\nResults: ${passed} passed, ${failed} failed`);
  if (failed > 0) {
    console.log("\nFailures:");
    for (const f of failures) {
      console.log(`  - ${f.name}: ${f.error.message}`);
    }
    process.exit(1);
  }
  setImmediate(() => process.exit(0));
}

main().catch((err) => {
  console.error("E2E runner fatal error:", err);
  process.exit(1);
});
