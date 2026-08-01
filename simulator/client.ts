/**
 * 模拟器 HTTP 客户端:封装对调度服务的全部网关/上游调用,带超时控制,
 * 便于复现断网(请求超时/不发请求)等故障。
 */
export interface ClientOptions {
  baseUrl: string;
  timeoutMs?: number;
}

export class ApiError extends Error {
  constructor(
    public status: number,
    public body: unknown,
  ) {
    super(`HTTP ${status}: ${JSON.stringify(body)}`);
  }
}

async function request(
  opts: ClientOptions,
  method: string,
  path: string,
  body?: unknown,
): Promise<{ status: number; body: any }> {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), opts.timeoutMs ?? 5000);
  try {
    const res = await fetch(`${opts.baseUrl}${path}`, {
      method,
      headers: { 'content-type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: ac.signal,
    });
    const text = await res.text();
    const parsed = text ? JSON.parse(text) : null;
    return { status: res.status, body: parsed };
  } finally {
    clearTimeout(timer);
  }
}

export class DispatchClient {
  constructor(private opts: ClientOptions) {}

  async submit(idempotencyKey: string, action: string, params: unknown) {
    return request(this.opts, 'POST', '/v1/commands', { idempotencyKey, action, params });
  }

  async getCommand(idOrKey: string) {
    return request(this.opts, 'GET', `/v1/commands/${encodeURIComponent(idOrKey)}`);
  }

  async getEvents(commandId: string) {
    return request(this.opts, 'GET', `/v1/commands/${encodeURIComponent(commandId)}/events`);
  }

  async claim(gatewayId: string, limit = 1) {
    return request(this.opts, 'POST', '/v1/gateway/claims', { gatewayId, limit });
  }

  async renew(gatewayId: string, leaseId: string) {
    return request(this.opts, 'POST', '/v1/gateway/renewals', { gatewayId, leaseId });
  }

  async ack(gatewayId: string, leaseId: string, ackId: string, result: unknown) {
    return request(this.opts, 'POST', '/v1/gateway/acks', { gatewayId, leaseId, ackId, result });
  }

  async health() {
    return request(this.opts, 'GET', '/healthz');
  }
}
