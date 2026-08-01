import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { ApiClient } from "../../tools/simulator/api-client.js";
import { GatewaySimulator } from "../../tools/simulator/gateway.js";
import { builtInScenarios } from "../../tools/simulator/scenarios.js";

const compiledEntry = resolve("dist/src/server/http-server.js");

function wait(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function stopServer(child: ChildProcess | undefined, timeoutMs = 5000) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  child.kill("SIGTERM");
  await new Promise<void>((res) => {
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      res();
    }, timeoutMs);
    child.once("exit", () => {
      clearTimeout(timer);
      res();
    });
  });
  await wait(100);
}

async function waitForHealth(client: ApiClient, timeoutMs = 10_000) {
  const start = Date.now();
  for (;;) {
    try {
      const res = await client.health();
      if (res.status === 200) return;
    } catch {
      if (Date.now() - start > timeoutMs)
        throw new Error("service did not become healthy");
    }
    await wait(100);
  }
}

async function startServer(
  port: number,
  dbPath: string,
  faultInjection = false,
): Promise<ChildProcess> {
  if (!existsSync(compiledEntry)) {
    throw new Error(
      `Compiled service not found at ${compiledEntry}. Run npm run build first.`,
    );
  }
  const child = spawn(process.execPath, [compiledEntry], {
    env: {
      ...process.env,
      PORT: String(port),
      HOST: "127.0.0.1",
      EDGE_DB_PATH: dbPath,
      LEASE_DURATION_MS: "800",
      MAX_ATTEMPTS: "3",
      SWEEP_INTERVAL_MS: "100000",
      FAULT_INJECTION: faultInjection ? "true" : "false",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stdout?.on("data", (d) => process.stdout.write(`[server] ${d}`));
  child.stderr?.on("data", (d) => process.stderr.write(`[server:err] ${d}`));
  return child;
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

async function main() {
  const tempDir = mkdtempSync(join(tmpdir(), "edge-e2e-"));
  const dbPath = join(tempDir, "e2e.db");
  const port = 18080;
  const baseUrl = `http://127.0.0.1:${port}`;
  const client = new ApiClient(baseUrl);
  let server: ChildProcess | undefined;

  try {
    server = await startServer(port, dbPath);
    await waitForHealth(client);
    console.log("e2e: service started");

    console.log("e2e: running built-in simulator scenarios over HTTP");
    for (const [name, scenario] of Object.entries(builtInScenarios)) {
      const result = await scenario(client);
      if (!result.passed) {
        throw new Error(
          `scenario ${name} failed: ${result.details.join("; ")} data=${JSON.stringify(result.data)}`,
        );
      }
      console.log(`  PASS ${name}: ${result.details.join("; ")}`);
    }

    console.log(
      "e2e: crash after durable commit then restart with same SQLite database",
    );
    const crashPort = port + 1;
    const crashDb = join(tempDir, "crash.db");
    const crashClient = new ApiClient(`http://127.0.0.1:${crashPort}`);
    const crashServer = await startServer(crashPort, crashDb, true);
    await waitForHealth(crashClient);

    const idempotencyKey = `crash-restart-${Date.now()}`;
    await crashClient.armCrashAfterCommit();
    let connectionReset = false;
    try {
      await crashClient.submit({
        idempotencyKey,
        deviceId: "crash-device",
        payload: { type: "SWITCH_RECIPE", params: { recipe: "B" } },
      });
    } catch {
      connectionReset = true;
    }
    assert(
      connectionReset,
      "submit connection should be reset by crash after durable commit",
    );
    await stopServer(crashServer);

    const restarted = await startServer(crashPort, crashDb, false);
    await waitForHealth(crashClient);
    const recovery = await crashClient.getByIdempotencyKey(idempotencyKey);
    assert(recovery.status === 200, "durable command should survive crash");
    const first = recovery.body as { commandId: string; state: string };

    const duplicate = await crashClient.submit({
      idempotencyKey,
      deviceId: "crash-device",
      payload: { type: "SWITCH_RECIPE", params: { recipe: "B" } },
    });
    const second = duplicate.body as {
      commandId: string;
      state: string;
      duplicate?: boolean;
    };
    assert(
      second.commandId === first.commandId,
      "restart must preserve stable command identity",
    );
    assert(
      second.duplicate === true,
      "post-crash resubmit must be recognized as duplicate",
    );

    const gw = new GatewaySimulator("gw-crash", crashClient);
    const leased = await gw.pollOnce(5000, first.commandId);
    assert(
      leased?.commandId === first.commandId,
      "recovered command should be claimable by same identity",
    );
    const ack = await gw.ack(leased);
    assert(
      ack.status === 200,
      "recovered command should accept valid device ack",
    );
    const delivered = await crashClient.getCommand(first.commandId);
    assert(
      (delivered.body as { state: string }).state === "DELIVERED",
      "state must become DELIVERED only after durable ack",
    );
    await stopServer(restarted);

    console.log("e2e: timeout and retry audit trail");
    const timeoutKey = `timeout-${Date.now()}`;
    await client.submit({
      idempotencyKey: timeoutKey,
      deviceId: "timeout-device",
      payload: { type: "CALIBRATE" },
    });
    const lostCmdId = (
      (await client.getByIdempotencyKey(timeoutKey)).body as {
        commandId: string;
      }
    ).commandId;
    const lostGw = new GatewaySimulator("gw-lost", client);
    const lost = await lostGw.pollOnce(300, lostCmdId);
    assert(lost, "gateway should claim command");
    await wait(450);
    await client.adminSweep();
    const recoveredGw = new GatewaySimulator("gw-recovered", client);
    const retried = await recoveredGw.pollOnce(5000, lostCmdId);
    assert(
      retried?.commandId === lost.commandId,
      "retry must use same command identity",
    );
    assert(
      retried.attempt === 2,
      "retry attempt must increase while commandId remains stable",
    );
    assert(retried.generation === 2, "retry ownership generation must advance");
    const stale = await GatewaySimulator.duplicateLeaseAck(
      client,
      lostGw.gatewayId,
      lost.commandId,
      lost.leaseId,
      lost.generation,
    );
    assert(stale.status === 409, "stale lease acknowledgment must be rejected");
    await recoveredGw.ack(retried);
    const audit = await client.getCommand(lost.commandId, true);
    const body = audit.body as {
      state: string;
      generation: number;
      events: {
        type: string;
        causedBy: string;
        data: Record<string, unknown>;
      }[];
    };
    const eventTypes = body.events.map((e) => e.type);
    assert(
      eventTypes.includes("LeaseExpired"),
      "audit trail must include lease expiry",
    );
    assert(
      eventTypes.includes("DeviceAckRecorded"),
      "audit trail must include durable ack",
    );
    assert(body.state === "DELIVERED", "final state should be DELIVERED");
    assert(body.generation === 2, "final snapshot must retain new generation");
    const ackEvent = body.events.find((e) => e.type === "DeviceAckRecorded")!;
    assert(
      ackEvent.data.generation === 2,
      "durable ack must record generation 2",
    );

    console.log(
      "e2e: process restart after dual-gateway takeover preserves fencing generation",
    );
    const restartPort = port + 2;
    const restartDb = join(tempDir, "failover.db");
    const restartClient = new ApiClient(`http://127.0.0.1:${restartPort}`);
    const fgServer = await startServer(restartPort, restartDb, false);
    await waitForHealth(restartClient);
    const fgKey = `failover-restart-${Date.now()}`;
    await restartClient.submit({
      idempotencyKey: fgKey,
      deviceId: "fg-device",
      payload: { type: "CALIBRATE" },
    });
    const fgCmdId = (
      (await restartClient.getByIdempotencyKey(fgKey)).body as {
        commandId: string;
      }
    ).commandId;
    const gwA = new GatewaySimulator("gw-a", restartClient);
    const leaseA = await gwA.pollOnce(300, fgCmdId);
    assert(
      leaseA?.generation === 1,
      "first generation after restart must be 1",
    );
    await stopServer(fgServer);

    const fgServer2 = await startServer(restartPort, restartDb, false);
    await waitForHealth(restartClient);
    const afterRestart = (await restartClient.getCommand(
      fgCmdId,
    )) as unknown as {
      body: { generation: number; state: string };
    };
    assert(
      afterRestart.body.generation === 1,
      "generation must survive process restart",
    );
    assert(
      afterRestart.body.state === "CLAIMED",
      "claimed lease must survive process restart",
    );
    await wait(400);
    const gwB = new GatewaySimulator("gw-b", restartClient);
    const leaseB = await gwB.pollOnce(5000, fgCmdId);
    assert(
      leaseB?.generation === 2,
      "standby must take over with generation 2 after restart",
    );
    const lateFromA = await GatewaySimulator.staleGenerationAck(
      restartClient,
      gwA.gatewayId,
      fgCmdId,
      leaseA!.leaseId,
      leaseA!.generation,
    );
    assert(
      lateFromA.status === 409,
      "old generation must be fenced after restart+takeover",
    );
    const finalAck = await gwB.ack(leaseB);
    assert(
      finalAck.status === 200,
      "new generation ack must deliver after restart",
    );
    const finalState = (await restartClient.getCommand(fgCmdId)) as unknown as {
      body: { state: string; generation: number };
    };
    assert(finalState.body.state === "DELIVERED", "state must be DELIVERED");
    assert(finalState.body.generation === 2, "final generation must be 2");
    await stopServer(fgServer2);

    console.log("e2e: crash recovery for emergency cancel and replace");
    const rcPort = port + 3;
    const rcDb = join(tempDir, "replace-crash.db");
    const crashSrv = await startServer(rcPort, rcDb, true);
    const crashCli = new ApiClient(`http://127.0.0.1:${rcPort}`);
    await waitForHealth(crashCli);

    const oldKey = `replace-crash-old-${Date.now()}`;
    await crashCli.submit({
      idempotencyKey: oldKey,
      deviceId: "crash-device",
      payload: { type: "SWITCH_RECIPE", params: { recipe: "D" } },
    });
    const oldCmdId = (
      (await crashCli.getByIdempotencyKey(oldKey)).body as { commandId: string }
    ).commandId;
    const gwOld = new GatewaySimulator("gw-crash-old", crashCli);
    const oldLease = await gwOld.pollOnce(300, oldCmdId);
    assert(oldLease?.generation === 1, "old claim should be generation 1");

    const newKey = `replace-crash-new-${Date.now()}`;
    await crashCli.armCrashAfterCommit();
    let replaceCrashed = false;
    try {
      await crashCli.replace(oldCmdId, newKey, { type: "SAFE_STOP" });
    } catch {
      replaceCrashed = true;
    }
    assert(
      replaceCrashed,
      "replace connection should be reset after durable commit",
    );
    await stopServer(crashSrv);

    const crashSrv2 = await startServer(rcPort, rcDb, false);
    await waitForHealth(crashCli);
    const oldAfter = (await crashCli.getCommand(oldCmdId, true)) as unknown as {
      body: {
        state: string;
        supersededByCommandId?: string;
        events: { type: string }[];
      };
    };
    assert(
      oldAfter.body.state === "CANCELLED",
      "old command must remain CANCELLED after restart",
    );
    const newId = oldAfter.body.supersededByCommandId!;
    assert(
      oldAfter.body.events.some((e) => e.type === "CommandSuperseded"),
      "supersession event must survive crash",
    );
    const newAfter = (await crashCli.getCommand(newId)) as unknown as {
      body: { state: string; supersedesCommandId?: string };
    };
    assert(
      newAfter.body.supersedesCommandId === oldCmdId,
      "replacement must link back to old command after restart",
    );

    const rcStale = await GatewaySimulator.staleGenerationAck(
      crashCli,
      gwOld.gatewayId,
      oldCmdId,
      oldLease.leaseId,
      oldLease.generation,
    );
    assert(
      rcStale.status === 409,
      "old gateway generation must remain fenced after restart+replace",
    );

    const idempotent = await crashCli.replace(oldCmdId, newKey, {
      type: "SAFE_STOP",
    });
    assert(idempotent.status === 200, "idempotent replace retry must succeed");
    assert(
      (idempotent.body as { newCommandId: string }).newCommandId === newId,
      "replace retry must not create a second replacement",
    );

    const gwNew = new GatewaySimulator("gw-crash-new", crashCli);
    const newLease = await gwNew.pollOnce(5000, newId);
    assert(
      newLease?.generation === 1,
      "replacement should be claimed at generation 1",
    );
    const newAck = await gwNew.ack(newLease);
    assert(newAck.status === 200, "replacement ack must succeed after restart");
    const newFinal = (await crashCli.getCommand(newId)) as unknown as {
      body: { state: string };
    };
    assert(
      newFinal.body.state === "DELIVERED",
      "replacement must be DELIVERED",
    );
    await stopServer(crashSrv2);

    console.log("e2e ALL CHECKS PASSED");
    await stopServer(server);
  } finally {
    await stopServer(server);
    try {
      rmSync(tempDir, {
        recursive: true,
        force: true,
        maxRetries: 5,
        retryDelay: 100,
      });
    } catch (cleanupErr) {
      console.warn("cleanup failed:", cleanupErr);
    }
  }
}

main().catch((err) => {
  console.error("e2e FAILED:", err);
  process.exit(1);
});
