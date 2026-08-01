export interface ApiCommand {
  commandId: string;
  idempotencyKey: string;
  payload: { deviceId: string; action: string; params?: Record<string, unknown> };
  status: string;
  currentLeaseId: string | null;
  attempt: number;
  maxAttempts: number;
  leaseExpiresAt: number | null;
  gatewayId: string | null;
  confirmationCode: string | null;
  deviceTimestamp: number | null;
  failureReason: string | null;
  createdAt: number;
  updatedAt: number;
}

export interface ApiEvent {
  eventId: string;
  commandId: string;
  eventType: string;
  leaseId: string | null;
  attempt: number | null;
  gatewayId: string | null;
  payload: Record<string, unknown> | null;
  causedBy: string;
  timestamp: number;
}

export class ApiClient {
  constructor(private readonly baseUrl: string) {}

  private async request<T>(
    method: string,
    path: string,
    body?: unknown,
    headers: Record<string, string> = {}
  ): Promise<{ status: number; body: T }> {
    const res = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers: {
        'Content-Type': 'application/json',
        ...headers,
      },
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
    let parsed: unknown = null;
    const text = await res.text();
    if (text) {
      try {
        parsed = JSON.parse(text);
      } catch {
        parsed = text;
      }
    }
    return { status: res.status, body: parsed as T };
  }

  async health(): Promise<boolean> {
    const { status } = await this.request('GET', '/api/v1/health');
    return status === 200;
  }

  async submitCommand(
    idempotencyKey: string,
    payload: ApiCommand['payload'],
    maxAttempts?: number
  ): Promise<{ status: number; body: ApiCommand }> {
    return this.request('POST', '/api/v1/commands', { payload, maxAttempts }, {
      'Idempotency-Key': idempotencyKey,
    });
  }

  async getCommand(commandId: string): Promise<{ status: number; body: ApiCommand | { error: string } }> {
    return this.request('GET', `/api/v1/commands/${commandId}`);
  }

  async getCommandByKey(key: string): Promise<{ status: number; body: ApiCommand | { error: string } }> {
    return this.request('GET', `/api/v1/commands/by-key/${encodeURIComponent(key)}`);
  }

  async getEvents(commandId: string): Promise<{ status: number; body: { events: ApiEvent[] } }> {
    return this.request('GET', `/api/v1/commands/${commandId}/events`);
  }

  async claim(
    gatewayId: string,
    leaseDurationMs: number,
    deviceId?: string
  ): Promise<{ status: number; body: ApiCommand | null }> {
    const { status, body } = await this.request<ApiCommand | null>(
      'POST',
      '/api/v1/gateway/claim',
      { gatewayId, leaseDurationMs, deviceId }
    );
    return { status, body: status === 204 ? null : body };
  }

  async renew(input: {
    commandId: string;
    leaseId: string;
    gatewayId: string;
    leaseDurationMs: number;
  }): Promise<{ status: number; body: ApiCommand | { error: string } }> {
    return this.request('POST', '/api/v1/gateway/renew', input);
  }

  async reportDelivery(input: {
    commandId: string;
    leaseId: string;
    gatewayId: string;
    deviceMessage?: string;
  }): Promise<{ status: number; body: ApiCommand }> {
    return this.request('POST', '/api/v1/gateway/report-delivery', input);
  }

  async confirm(input: {
    commandId: string;
    leaseId: string;
    gatewayId: string;
    confirmationCode: string;
    deviceTimestamp: number;
  }): Promise<{ status: number; body: { accepted: boolean; command: ApiCommand } }> {
    return this.request('POST', '/api/v1/gateway/confirm', input);
  }

  async scanExpired(): Promise<{ status: number; body: { expired: unknown[] } }> {
    return this.request('POST', '/api/v1/admin/scan-expired');
  }

  async listEvents(limit = 500): Promise<{ status: number; body: { events: ApiEvent[] } }> {
    return this.request('GET', `/api/v1/events?limit=${limit}`);
  }
}
