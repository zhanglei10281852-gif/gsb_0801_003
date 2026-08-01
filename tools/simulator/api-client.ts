export interface ApiResponse<T> {
  status: number;
  body: T;
  headers: Headers;
}

export class ApiClient {
  constructor(private readonly baseUrl: string) {}

  private async request<T>(
    method: string,
    path: string,
    body?: unknown,
    headers: Record<string, string> = {}
  ): Promise<ApiResponse<T>> {
    const init: RequestInit = { method, headers: { ...headers } };
    if (body !== undefined) {
      (init.headers as Record<string, string>)['content-type'] = 'application/json';
      init.body = JSON.stringify(body);
    }
    const res = await fetch(new URL(path, this.baseUrl), init);
    const text = await res.text();
    const parsed = text ? (JSON.parse(text) as T) : (undefined as T);
    return { status: res.status, body: parsed, headers: res.headers };
  }

  submit(payload: {
    idempotencyKey: string;
    deviceId: string;
    payload: { type: string; params?: Record<string, unknown> };
  }) {
    return this.request('POST', '/v1/upstream/commands', payload, {
      'idempotency-key': payload.idempotencyKey,
    });
  }

  getCommand(commandId: string, withEvents = false) {
    return this.request('GET', `/v1/upstream/commands/${commandId}${withEvents ? '/events' : ''}`);
  }

  getByIdempotencyKey(key: string) {
    return this.request('GET', `/v1/upstream/commands?idempotencyKey=${encodeURIComponent(key)}`);
  }

  claim(gatewayId: string, leaseDurationMs?: number) {
    return this.request(
      'POST',
      '/v1/gateway/claim',
      leaseDurationMs ? { leaseDurationMs } : {},
      { 'x-gateway-id': gatewayId }
    );
  }

  renew(
    gatewayId: string,
    commandId: string,
    leaseId: string,
    leaseDurationMs?: number
  ) {
    return this.request(
      'POST',
      `/v1/gateway/commands/${commandId}/renew`,
      leaseDurationMs ? { leaseDurationMs } : {},
      { 'x-gateway-id': gatewayId, 'x-lease-id': leaseId }
    );
  }

  ack(
    gatewayId: string,
    commandId: string,
    leaseId: string,
    body: { ackCode?: string; success?: boolean; ackPayload?: Record<string, unknown> }
  ) {
    return this.request('POST', `/v1/gateway/commands/${commandId}/ack`, body, {
      'x-gateway-id': gatewayId,
      'x-lease-id': leaseId,
    });
  }

  adminSweep() {
    return this.request<{ expired: number }>('POST', '/v1/admin/sweep', {});
  }

  adminEvents(limit = 200) {
    return this.request<{ events: unknown[] }>('GET', `/v1/admin/events?limit=${limit}`);
  }

  armCrashAfterCommit() {
    return this.request('POST', '/v1/admin/fault/crash-after-commit', {});
  }

  health() {
    return this.request<{ ok: boolean }>('GET', '/health');
  }
}
