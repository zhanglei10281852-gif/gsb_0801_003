/**
 * HTTP 适配器(node:http,无框架依赖)。
 * 只做:解析请求 → 调服务层 → 序列化响应。不含任何领域规则。
 */
import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { DispatchService, HttpError } from '../service/dispatchService';
import { ServerConfig } from '../config';

function readBody(req: http.IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    req.on('data', (c: Buffer) => {
      size += c.length;
      if (size > 1_000_000) {
        reject(new HttpError(413, 'too_large', '请求体过大'));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => {
      if (chunks.length === 0) return resolve({});
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch {
        reject(new HttpError(400, 'bad_json', '请求体不是合法 JSON'));
      }
    });
    req.on('error', reject);
  });
}

function send(res: http.ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  res.end(payload);
}

export function createServer(service: DispatchService, config: ServerConfig): http.Server {
  return http.createServer(async (req, res) => {
    const requestId = (req.headers['x-request-id'] as string) ?? randomUUID();
    try {
      const url = new URL(req.url ?? '/', 'http://localhost');
      const path = url.pathname;
      const method = req.method ?? 'GET';
      const body = method === 'POST' ? ((await readBody(req)) as Record<string, unknown>) : {};

      // 上游:提交指令(业务幂等键)
      if (method === 'POST' && path === '/v1/commands') {
        const reply = service.submit(
          {
            idempotencyKey: String(body.idempotencyKey ?? body.idempotency_key ?? ''),
            action: String(body.action ?? ''),
            params: body.params ?? null,
          },
          requestId,
        );
        // 故障注入钩子:模拟"持久化后、响应前崩溃"
        if (config.crashAfterSubmitCommit) {
          process.exit(42);
        }
        return send(res, reply.deduped ? 200 : 201, reply);
      }

      // 上游/运维:查询指令进度
      if (method === 'GET' && path.startsWith('/v1/commands/')) {
        const id = decodeURIComponent(path.slice('/v1/commands/'.length));
        if (id.endsWith('/events')) {
          const cmdId = id.slice(0, -'/events'.length);
          const cmd = service.getCommand(cmdId);
          if (!cmd) throw new HttpError(404, 'not_found', `指令 ${cmdId} 不存在`);
          return send(res, 200, { commandId: cmd.id, events: service.listEvents(cmd.id) });
        }
        const cmd = service.getCommand(id);
        if (!cmd) throw new HttpError(404, 'not_found', `指令 ${id} 不存在`);
        return send(res, 200, cmd);
      }
      if (method === 'GET' && path === '/v1/commands') {
        const key = url.searchParams.get('key');
        if (!key) throw new HttpError(400, 'bad_request', '缺少 ?key=');
        const cmd = service.getCommand(key);
        if (!cmd) throw new HttpError(404, 'not_found', `幂等键 ${key} 不存在`);
        return send(res, 200, cmd);
      }

      // 网关:领取待发任务
      if (method === 'POST' && path === '/v1/gateway/claims') {
        const tasks = service.claim(
          String(body.gatewayId ?? body.gateway_id ?? ''),
          Number(body.limit ?? 1),
          requestId,
        );
        return send(res, 200, { tasks });
      }

      // 网关:续约租约
      if (method === 'POST' && path === '/v1/gateway/renewals') {
        const reply = service.renew(
          String(body.leaseId ?? body.lease_id ?? ''),
          String(body.gatewayId ?? body.gateway_id ?? ''),
          requestId,
        );
        return send(res, reply.ok ? 200 : 409, reply);
      }

      // 网关:回传设备确认
      if (method === 'POST' && path === '/v1/gateway/acks') {
        const reply = service.ack(
          String(body.leaseId ?? body.lease_id ?? ''),
          String(body.gatewayId ?? body.gateway_id ?? ''),
          { ackId: String(body.ackId ?? body.ack_id ?? ''), result: body.result ?? null },
          requestId,
        );
        return send(res, 200, reply);
      }

      // 运维:全量/按指令追溯事件
      if (method === 'GET' && path === '/v1/events') {
        return send(res, 200, { events: service.listEvents(url.searchParams.get('command_id') ?? undefined) });
      }

      if (method === 'GET' && path === '/healthz') {
        return send(res, 200, { ok: true });
      }

      throw new HttpError(404, 'not_found', `${method} ${path}`);
    } catch (err) {
      if (err instanceof HttpError) {
        return send(res, err.status, { error: { code: err.code, message: err.message } });
      }
      send(res, 500, { error: { code: 'internal', message: String(err) } });
    }
  });
}
