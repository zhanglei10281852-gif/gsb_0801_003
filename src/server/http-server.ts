import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import type { AddressInfo } from "node:net";
import { pathToFileURL } from "node:url";
import { SqliteEventStore } from "../adapters/sqlite-store.js";
import { CommandService } from "../application/command-service.js";
import { loadConfig, type AppConfig } from "../config.js";

function getHeader(req: IncomingMessage, name: string): string | undefined {
  const value = req.headers[name.toLowerCase()];
  return Array.isArray(value) ? value[0] : value;
}

function sendJson(res: ServerResponse, status: number, body: unknown) {
  const json = JSON.stringify(body);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(json),
  });
  res.end(json);
}

async function readJson(req: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of req)
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  if (chunks.length === 0) return {};
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

function commandView(
  cmd: ReturnType<CommandService["getCommand"]>,
  events?: unknown[],
  duplicate?: boolean,
) {
  if (!cmd) return undefined;
  return {
    commandId: cmd.commandId,
    duplicate,
    idempotencyKey: cmd.idempotencyKey,
    deviceId: cmd.deviceId,
    payload: cmd.payload,
    state: cmd.state,
    version: cmd.version,
    attempt: cmd.attempt,
    generation: cmd.generation,
    lease: cmd.leaseId
      ? {
          gatewayId: cmd.gatewayId,
          leaseId: cmd.leaseId,
          generation: cmd.generation,
          expiresAt: cmd.leaseExpiresAt,
        }
      : undefined,
    deliveredAt: cmd.deliveredAt,
    cancelledAt: cmd.cancelledAt,
    cancelledBy: cmd.cancelledBy,
    cancelReason: cmd.cancelReason,
    supersededByCommandId: cmd.supersededByCommandId,
    supersedesCommandId: cmd.supersedesCommandId,
    terminalReason: cmd.terminalReason,
    createdAt: cmd.createdAt,
    updatedAt: cmd.updatedAt,
    events,
  };
}

export function createApp(config: AppConfig) {
  const store = new SqliteEventStore({ filePath: config.dbPath });
  const service = new CommandService(store, {
    leaseDurationMs: config.leaseDurationMs,
    maxAttempts: config.maxAttempts,
    maxAgeMs: config.maxAgeMs,
    claimBatchSize: config.claimBatchSize,
  });

  let crashAfterNextCommit = false;
  const sweeper = setInterval(() => {
    try {
      service.expireOutdated();
    } catch (err) {
      console.error("sweep failed", err);
    }
  }, config.sweepIntervalMs);
  sweeper.unref();

  function maybeCrash(point: string) {
    if (!config.faultInjectionEnabled) return;
    if (point === "afterCommit" && crashAfterNextCommit) {
      crashAfterNextCommit = false;
      console.error(
        "FAULT: crashing after durable commit and before HTTP response",
      );
      process.exit(87);
    }
  }

  const server = createServer(async (req, res) => {
    try {
      const url = new URL(
        req.url ?? "/",
        `http://${req.headers.host ?? "localhost"}`,
      );
      const method = req.method ?? "GET";
      const path = url.pathname;

      if (method === "GET" && path === "/health") {
        sendJson(res, 200, { ok: true, time: Date.now() });
        return;
      }

      if (method === "POST" && path === "/v1/upstream/commands") {
        const body = (await readJson(req)) as Record<string, unknown>;
        const idempotencyKey =
          getHeader(req, "idempotency-key") ?? (body.idempotencyKey as string);
        if (!idempotencyKey) {
          sendJson(res, 400, { error: "IDEMPOTENCY_KEY_REQUIRED" });
          return;
        }
        if (
          !body.deviceId ||
          !body.payload ||
          typeof body.payload !== "object"
        ) {
          sendJson(res, 400, {
            error: "INVALID_REQUEST",
            required: ["deviceId", "payload"],
          });
          return;
        }
        const result = service.submit({
          idempotencyKey,
          deviceId: body.deviceId as string,
          payload: body.payload as {
            type: string;
            params?: Record<string, unknown>;
          },
        });
        maybeCrash("afterCommit");
        const cmd = service.getCommand(result.commandId);
        sendJson(
          res,
          result.duplicate ? 200 : 202,
          commandView(cmd, undefined, result.duplicate),
        );
        return;
      }

      if (method === "GET" && path === "/v1/upstream/commands") {
        const key = url.searchParams.get("idempotencyKey");
        if (key) {
          const cmd = service.getByIdempotencyKey(key);
          if (!cmd) {
            sendJson(res, 404, { error: "NOT_FOUND" });
            return;
          }
          sendJson(res, 200, commandView(cmd));
          return;
        }
        sendJson(res, 200, {
          commands: service.listCommands().map((c) => commandView(c)),
        });
        return;
      }

      const commandMatch = path.match(
        /^\/v1\/(?:upstream|gateway)\/commands\/([^/]+)(?:\/(events))?$/,
      );
      if (commandMatch && method === "GET") {
        const commandId = decodeURIComponent(commandMatch[1]);
        const wantEvents = Boolean(commandMatch[2]);
        const cmd = service.getCommand(commandId);
        if (!cmd) {
          sendJson(res, 404, { error: "NOT_FOUND" });
          return;
        }
        const events = wantEvents ? service.getEvents(commandId) : undefined;
        sendJson(res, 200, commandView(cmd, events));
        return;
      }

      if (method === "POST" && path === "/v1/gateway/claim") {
        const body = (await readJson(req)) as Record<string, unknown>;
        const gatewayId =
          getHeader(req, "x-gateway-id") ?? (body.gatewayId as string);
        if (!gatewayId) {
          sendJson(res, 400, { error: "GATEWAY_ID_REQUIRED" });
          return;
        }
        const claimed = service.claimOne({
          gatewayId: gatewayId as string,
          leaseDurationMs: body.leaseDurationMs as number | undefined,
        });
        if (!claimed) {
          sendJson(res, 204, null);
          return;
        }
        maybeCrash("afterCommit");
        sendJson(res, 200, {
          command: commandView(claimed.command),
          lease: {
            leaseId: claimed.command.leaseId,
            generation: claimed.command.generation,
            expiresAt: claimed.command.leaseExpiresAt,
          },
        });
        return;
      }

      const renewMatch = path.match(
        /^\/v1\/gateway\/commands\/([^/]+)\/renew$/,
      );
      if (renewMatch && method === "POST") {
        const commandId = decodeURIComponent(renewMatch[1]);
        const body = (await readJson(req)) as Record<string, unknown>;
        const gatewayId =
          getHeader(req, "x-gateway-id") ?? (body.gatewayId as string);
        const leaseId =
          getHeader(req, "x-lease-id") ?? (body.leaseId as string);
        const generationRaw =
          getHeader(req, "x-generation") ??
          (body.generation as string | number | undefined);
        if (!gatewayId || !leaseId || generationRaw === undefined) {
          sendJson(res, 400, { error: "LEASE_CONTEXT_REQUIRED" });
          return;
        }
        const generation = Number(generationRaw);
        if (!Number.isInteger(generation) || generation < 1) {
          sendJson(res, 400, { error: "INVALID_GENERATION" });
          return;
        }
        const result = service.renew({
          commandId,
          gatewayId: gatewayId as string,
          leaseId: leaseId as string,
          generation,
          leaseDurationMs: body.leaseDurationMs as number | undefined,
        });
        if (!result.accepted) {
          sendJson(res, 409, { accepted: false, reason: result.reason });
          return;
        }
        maybeCrash("afterCommit");
        sendJson(res, 200, commandView(result.snapshot));
        return;
      }

      const ackMatch = path.match(/^\/v1\/gateway\/commands\/([^/]+)\/ack$/);
      if (ackMatch && method === "POST") {
        const commandId = decodeURIComponent(ackMatch[1]);
        const body = (await readJson(req)) as Record<string, unknown>;
        const gatewayId =
          getHeader(req, "x-gateway-id") ?? (body.gatewayId as string);
        const leaseId =
          getHeader(req, "x-lease-id") ?? (body.leaseId as string);
        const generationRaw =
          getHeader(req, "x-generation") ??
          (body.generation as string | number | undefined);
        if (!gatewayId || !leaseId || generationRaw === undefined) {
          sendJson(res, 400, { error: "LEASE_CONTEXT_REQUIRED" });
          return;
        }
        const generation = Number(generationRaw);
        if (!Number.isInteger(generation) || generation < 1) {
          sendJson(res, 400, { error: "INVALID_GENERATION" });
          return;
        }
        const successRaw = body.success;
        const success =
          typeof successRaw === "boolean"
            ? successRaw
            : (body.ackCode as string | undefined) !== "NACK";
        const result = service.ack({
          commandId,
          gatewayId: gatewayId as string,
          leaseId: leaseId as string,
          generation,
          ackCode: (body.ackCode as string) ?? (success ? "ACK" : "NACK"),
          success,
          ackPayload: body.ackPayload as Record<string, unknown> | undefined,
        });
        if (!result.accepted) {
          sendJson(res, 409, { accepted: false, reason: result.reason });
          return;
        }
        maybeCrash("afterCommit");
        sendJson(res, 200, commandView(result.snapshot));
        return;
      }

      const cancelMatch = path.match(
        /^\/v1\/upstream\/commands\/([^/]+)\/cancel$/,
      );
      if (cancelMatch && method === "POST") {
        const commandId = decodeURIComponent(cancelMatch[1]);
        const body = (await readJson(req)) as Record<string, unknown>;
        const requestedBy =
          (getHeader(req, "x-requested-by") as string) ??
          (body.requestedBy as string) ??
          "upstream";
        const result = service.cancel({
          commandId,
          reason: (body.reason as string) ?? "EMERGENCY_CANCEL",
          requestedBy,
        });
        if (!result.accepted) {
          sendJson(res, 409, { accepted: false, reason: result.reason });
          return;
        }
        maybeCrash("afterCommit");
        sendJson(res, 200, commandView(result.snapshot));
        return;
      }

      const replaceMatch = path.match(
        /^\/v1\/upstream\/commands\/([^/]+)\/replace$/,
      );
      if (replaceMatch && method === "POST") {
        const oldCommandId = decodeURIComponent(replaceMatch[1]);
        const body = (await readJson(req)) as Record<string, unknown>;
        const newIdempotencyKey =
          (getHeader(req, "idempotency-key") as string) ??
          (body.idempotencyKey as string) ??
          (body.newIdempotencyKey as string);
        if (
          !newIdempotencyKey ||
          !body.payload ||
          typeof body.payload !== "object"
        ) {
          sendJson(res, 400, {
            error: "INVALID_REQUEST",
            required: ["idempotency-key", "payload"],
          });
          return;
        }
        const requestedBy =
          (getHeader(req, "x-requested-by") as string) ??
          (body.requestedBy as string) ??
          "upstream";
        const result = service.replace({
          oldCommandId,
          newIdempotencyKey: newIdempotencyKey as string,
          reason: (body.reason as string) ?? "REPLACED_BY_SAFE_COMMAND",
          requestedBy,
          replacementPayload: body.payload as {
            type: string;
            params?: Record<string, unknown>;
          },
        });
        if (!result.accepted) {
          sendJson(res, 409, { accepted: false, reason: result.reason });
          return;
        }
        maybeCrash("afterCommit");
        sendJson(res, 200, {
          old: commandView(result.oldSnapshot),
          replacement: commandView(result.newSnapshot),
          newCommandId: result.newCommandId,
        });
        return;
      }

      if (method === "POST" && path === "/v1/admin/sweep") {
        const events = service.expireOutdated();
        sendJson(res, 200, { expired: events.length, events });
        return;
      }

      if (method === "GET" && path === "/v1/admin/events") {
        const limit = Number(url.searchParams.get("limit") ?? "100");
        sendJson(res, 200, { events: service.listEvents({ limit }) });
        return;
      }

      if (method === "POST" && path === "/v1/admin/fault/crash-after-commit") {
        crashAfterNextCommit = true;
        sendJson(res, 200, { armed: true });
        return;
      }

      sendJson(res, 404, { error: "NOT_FOUND", method, path });
    } catch (err) {
      console.error("request failed", err);
      sendJson(res, 500, {
        error: "INTERNAL_ERROR",
        message: (err as Error).message,
      });
    }
  });

  return {
    server,
    store,
    service,
    close: async () => {
      clearInterval(sweeper);
      await new Promise<void>((resolveClose) =>
        server.close(() => resolveClose()),
      );
      store.close();
    },
  };
}

async function main() {
  const config = loadConfig();
  const app = createApp(config);
  app.server.listen(config.port, config.host, () => {
    const addr = app.server.address() as AddressInfo;
    console.log(
      `edge command service listening on http://${addr.address}:${addr.port}`,
    );
  });

  const shutdown = () => {
    app.close().finally(() => process.exit(0));
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
