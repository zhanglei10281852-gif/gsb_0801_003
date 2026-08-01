/**
 * HTTP adapter: a thin translation layer over DispatchService using only the
 * Node core `http` module (no framework, no external deps). It maps requests to
 * service calls and service results to status codes. All domain decisions live
 * below this layer; the adapter never inspects command state to make choices.
 *
 * Routes:
 *   Upstream (scheduling system):
 *     POST /commands                 submit (idempotent by idempotencyKey; optional lineId, supersedesId)
 *     POST /commands/:id/cancel      emergency recall of a not-yet-completed action
 *     GET  /commands/:id             query by internal id
 *     GET  /commands?key=...         query by idempotency key
 *     GET  /commands/:id/events      causal history of one command
 *     GET  /commands/:id/chain       recall/replace lineage as one causal chain
 *   Gateway (edge, redundant):
 *     POST /gateway/ownership        { lineId, gateway } -> claim/heartbeat/takeover
 *     POST /gateway/lease            { leaseholder, lineId?, deviceId?, generation? } -> task or 204
 *     POST /gateway/renew            { commandId, leaseId, generation? }
 *     POST /gateway/confirm          { commandId, leaseId, outcome, deviceReceipt?, generation? }
 *   Ops:
 *     GET  /ops/commands             list (optional ?status=)
 *     GET  /ops/events               recent events across all subjects
 *     GET  /ops/ownership            current line ownership records
 *     GET  /ops/lines/:lineId/events causal history of one line's ownership
 *     POST /ops/sweep                force an expiry sweep (also runs periodically)
 *     GET  /healthz
 */

import { IncomingMessage, ServerResponse, createServer, Server } from 'node:http';
import { ConfirmOutcome } from '../domain/types';
import { DispatchService, NotFoundError } from '../app/dispatchService';

interface Json {
  [k: string]: unknown;
}

function send(res: ServerResponse, status: number, body: Json | null): void {
  const payload = body === null ? '' : JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(payload),
  });
  res.end(payload);
}

async function readJson(req: IncomingMessage): Promise<Json> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    size += (chunk as Buffer).length;
    if (size > 1_000_000) throw new BadRequest('request body too large');
    chunks.push(chunk as Buffer);
  }
  if (chunks.length === 0) return {};
  const text = Buffer.concat(chunks).toString('utf8');
  try {
    const parsed = JSON.parse(text);
    if (typeof parsed !== 'object' || parsed === null) {
      throw new BadRequest('body must be a JSON object');
    }
    return parsed as Json;
  } catch (e) {
    if (e instanceof BadRequest) throw e;
    throw new BadRequest('invalid JSON body');
  }
}

class BadRequest extends Error {}

function str(v: unknown, field: string): string {
  if (typeof v !== 'string' || v.length === 0) {
    throw new BadRequest(`missing or invalid string field: ${field}`);
  }
  return v;
}

/** Optional non-negative integer field (e.g. ownership generation). */
function optInt(v: unknown): number | null {
  return typeof v === 'number' && Number.isFinite(v) ? Math.floor(v) : null;
}

/** Optional string field. */
function optStr(v: unknown): string | undefined {
  return typeof v === 'string' && v.length > 0 ? v : undefined;
}

export function createHttpServer(service: DispatchService): Server {
  return createServer((req, res) => {
    handle(service, req, res).catch((err) => {
      if (err instanceof BadRequest) {
        send(res, 400, { error: 'bad_request', message: err.message });
      } else if (err instanceof NotFoundError) {
        send(res, 404, { error: 'not_found', id: err.id });
      } else {
        // Unexpected: log to stderr and return 500 without leaking internals.
        process.stderr.write(`[http] unhandled error: ${String(err)}\n`);
        send(res, 500, { error: 'internal_error' });
      }
    });
  });
}

async function handle(
  service: DispatchService,
  req: IncomingMessage,
  res: ServerResponse,
): Promise<void> {
  const url = new URL(req.url ?? '/', 'http://localhost');
  const path = url.pathname;
  const method = req.method ?? 'GET';

  // --- health ---
  if (method === 'GET' && path === '/healthz') {
    return send(res, 200, { status: 'ok' });
  }

  // --- upstream: submit ---
  if (method === 'POST' && path === '/commands') {
    const body = await readJson(req);
    const result = service.submit({
      idempotencyKey: str(body.idempotencyKey, 'idempotencyKey'),
      deviceId: str(body.deviceId, 'deviceId'),
      kind: str(body.kind, 'kind'),
      params:
        typeof body.params === 'object' && body.params !== null
          ? (body.params as Record<string, unknown>)
          : {},
      maxAttempts:
        typeof body.maxAttempts === 'number' ? body.maxAttempts : undefined,
      lineId: optStr(body.lineId),
      supersedesId: optStr(body.supersedesId),
      supersedeReason: optStr(body.supersedeReason),
      requestedBy: optStr(body.requestedBy),
    });
    // 200 for dedup replay, 201 for a freshly created command.
    return send(res, result.deduped ? 200 : 201, {
      command: result.command,
      deduped: result.deduped,
    });
  }

  // --- upstream: query by idempotency key ---
  if (method === 'GET' && path === '/commands' && url.searchParams.has('key')) {
    const key = url.searchParams.get('key')!;
    const cmd = service.getByIdempotencyKey(key);
    if (!cmd) return send(res, 404, { error: 'not_found', key });
    return send(res, 200, { command: cmd });
  }

  // --- ops: list ---
  if (method === 'GET' && path === '/ops/commands') {
    const status = url.searchParams.get('status') ?? undefined;
    const list = service.list(status as never, 200);
    return send(res, 200, { commands: list });
  }

  // --- ops: recent events ---
  if (method === 'GET' && path === '/ops/events') {
    const limit = Number(url.searchParams.get('limit') ?? '100');
    return send(res, 200, {
      events: service.recentEvents(Number.isFinite(limit) ? limit : 100),
    });
  }

  // --- ops: current line ownership ---
  if (method === 'GET' && path === '/ops/ownership') {
    return send(res, 200, { ownership: service.listOwnership() });
  }

  // --- ops: one line's ownership causal history ---
  const lineEvMatch = /^\/ops\/lines\/([^/]+)\/events$/.exec(path);
  if (method === 'GET' && lineEvMatch) {
    const lineId = decodeURIComponent(lineEvMatch[1]!);
    return send(res, 200, { lineId, events: service.history(lineId) });
  }

  // --- ops: force sweep ---
  if (method === 'POST' && path === '/ops/sweep') {
    return send(res, 200, service.sweepExpired());
  }

  // --- gateway: claim/heartbeat/takeover line ownership ---
  if (method === 'POST' && path === '/gateway/ownership') {
    const body = await readJson(req);
    const lineId = str(body.lineId, 'lineId');
    const gateway = str(body.gateway, 'gateway');
    const result = service.claimOwnership(lineId, gateway);
    // A rejected claim (healthy owner present) is a 409; otherwise 200 with the
    // authoritative generation the gateway must stamp on subsequent operations.
    const status = result.outcome === 'rejected' ? 409 : 200;
    return send(res, status, {
      outcome: result.outcome,
      reason: result.reason,
      lineId: result.ownership.lineId,
      owner: result.ownership.owner,
      generation: result.ownership.generation,
      expiresAt: result.ownership.expiresAt,
    });
  }

  // --- gateway: lease next ---
  if (method === 'POST' && path === '/gateway/lease') {
    const body = await readJson(req);
    const leaseholder = str(body.leaseholder, 'leaseholder');
    const result = service.leaseNext(leaseholder, {
      lineId: optStr(body.lineId),
      deviceId: optStr(body.deviceId),
      callerGeneration: optInt(body.generation),
    });
    if (!result) return send(res, 204, null);
    return send(res, 200, {
      commandId: result.command.id,
      executionId: result.executionId,
      leaseId: result.leaseId,
      lineId: result.command.lineId,
      ownerGeneration: result.ownerGeneration,
      preempted: result.preempted,
      payload: result.command.payload,
      leaseExpiresAt: result.command.leaseExpiresAt,
      attempt: result.command.attempts,
    });
  }

  // --- gateway: renew ---
  if (method === 'POST' && path === '/gateway/renew') {
    const body = await readJson(req);
    const result = service.renew(
      str(body.commandId, 'commandId'),
      str(body.leaseId, 'leaseId'),
      optInt(body.generation),
    );
    return send(res, result.renewed ? 200 : 409, {
      renewed: result.renewed,
      reason: result.reason,
      leaseExpiresAt: result.command.leaseExpiresAt,
      status: result.command.status,
    });
  }

  // --- gateway: confirm ---
  if (method === 'POST' && path === '/gateway/confirm') {
    const body = await readJson(req);
    const outcome = str(body.outcome, 'outcome');
    if (outcome !== 'success' && outcome !== 'failure') {
      throw new BadRequest('outcome must be "success" or "failure"');
    }
    const result = service.confirm(
      str(body.commandId, 'commandId'),
      str(body.leaseId, 'leaseId'),
      outcome as ConfirmOutcome,
      {
        deviceReceipt: optStr(body.deviceReceipt),
        callerGeneration: optInt(body.generation),
      },
    );
    return send(res, result.accepted ? 200 : 409, {
      accepted: result.accepted,
      reason: result.reason,
      status: result.command.status,
      terminalReason: result.command.terminalReason,
    });
  }

  // --- upstream: emergency recall of a not-yet-completed action ---
  const cancelMatch = /^\/commands\/([^/]+)\/cancel$/.exec(path);
  if (method === 'POST' && cancelMatch) {
    const id = decodeURIComponent(cancelMatch[1]!);
    const body = await readJson(req);
    const result = service.cancel(id, {
      reason: optStr(body.reason),
      requestedBy: optStr(body.requestedBy),
    });
    // 200 when the recall took effect; 409 when it was refused (e.g. the action
    // was already device-confirmed and cannot be faked into a cancel).
    return send(res, result.cancelled ? 200 : 409, {
      cancelled: result.cancelled,
      reason: result.reason,
      status: result.command.status,
      terminalReason: result.command.terminalReason,
    });
  }

  // --- id-scoped routes: /commands/:id, /commands/:id/events, /commands/:id/chain ---
  const cmdMatch = /^\/commands\/([^/]+)(\/events|\/chain)?$/.exec(path);
  if (method === 'GET' && cmdMatch) {
    const id = decodeURIComponent(cmdMatch[1]!);
    const suffix = cmdMatch[2];
    if (suffix === '/events') {
      const cmd = service.getById(id);
      if (!cmd) return send(res, 404, { error: 'not_found', id });
      return send(res, 200, { commandId: id, events: service.history(id) });
    }
    if (suffix === '/chain') {
      // Recall/replace lineage as one causal chain.
      const lin = service.lineage(id);
      if (!lin) return send(res, 404, { error: 'not_found', id });
      return send(res, 200, {
        commandId: id,
        chain: lin.chain.map((c) => ({
          id: c.id,
          status: c.status,
          supersedesId: c.supersedesId,
          terminalReason: c.terminalReason,
          kind: c.payload.kind,
        })),
        events: lin.events,
      });
    }
    const cmd = service.getById(id);
    if (!cmd) return send(res, 404, { error: 'not_found', id });
    return send(res, 200, { command: cmd });
  }

  send(res, 404, { error: 'route_not_found', path });
}
