import { randomUUID } from 'node:crypto';
import { ApiClient } from './api-client.js';

export interface LeasedCommand {
  commandId: string;
  leaseId: string;
  payload: unknown;
  attempt: number;
}

export interface DeviceBehavior {
  process: (cmd: LeasedCommand) =>
    | { success: boolean; ackCode?: string; ackPayload?: Record<string, unknown> }
    | Promise<{ success: boolean; ackCode?: string; ackPayload?: Record<string, unknown> }>;
}

export const immediateAckDevice: DeviceBehavior = {
  async process() {
    return { success: true, ackCode: 'EXECUTED' };
  },
};

export const immediateNackDevice: DeviceBehavior = {
  async process() {
    return { success: false, ackCode: 'DEVICE_BUSY' };
  },
};

export class GatewaySimulator {
  public readonly seenCommandIds = new Set<string>();
  public delivered: LeasedCommand[] = [];
  public acknowledgements: unknown[] = [];

  constructor(
    public readonly gatewayId: string,
    private readonly client: ApiClient,
    public device: DeviceBehavior = immediateAckDevice,
    public offline = false
  ) {}

  async pollOnce(
    leaseDurationMs?: number,
    expectedCommandId?: string
  ): Promise<LeasedCommand | undefined> {
    if (this.offline) return undefined;
    for (let i = 0; i < 25; i++) {
      const res = await this.client.claim(this.gatewayId, leaseDurationMs);
      if (res.status === 204 || !res.body) return undefined;
      const body = res.body as {
        command: { commandId: string; payload: unknown; attempt: number };
        lease: { leaseId: string };
      };
      const leased: LeasedCommand = {
        commandId: body.command.commandId,
        leaseId: body.lease.leaseId,
        payload: body.command.payload,
        attempt: body.command.attempt,
      };
      if (expectedCommandId && leased.commandId !== expectedCommandId) {
        this.seenCommandIds.add(leased.commandId);
        this.delivered.push(leased);
        await new Promise((r) => setTimeout(r, 20));
        continue;
      }
      this.seenCommandIds.add(leased.commandId);
      this.delivered.push(leased);
      return leased;
    }
    return undefined;
  }

  async renew(cmd: LeasedCommand, leaseDurationMs?: number) {
    return this.client.renew(this.gatewayId, cmd.commandId, cmd.leaseId, leaseDurationMs);
  }

  async ack(cmd: LeasedCommand, override?: { success?: boolean; ackCode?: string }) {
    const result = await this.device.process(cmd);
    const ack = await this.client.ack(this.gatewayId, cmd.commandId, cmd.leaseId, {
      success: override?.success ?? result.success,
      ackCode: override?.ackCode ?? result.ackCode ?? (result.success ? 'EXECUTED' : 'NACK'),
      ackPayload: result.ackPayload,
    });
    this.acknowledgements.push({
      commandId: cmd.commandId,
      attempt: cmd.attempt,
      status: ack.status,
      body: ack.body,
    });
    return ack;
  }

  static duplicateLeaseAck(
    client: ApiClient,
    gatewayId: string,
    commandId: string,
    leaseId: string
  ) {
    return client.ack(gatewayId, commandId, leaseId, { success: true, ackCode: 'DUPLICATE_ACK' });
  }

  static async waitForHealth(client: ApiClient, timeoutMs = 10_000) {
    const start = Date.now();
    for (;;) {
      try {
        const res = await client.health();
        if (res.status === 200) return;
      } catch {
        if (Date.now() - start > timeoutMs) throw new Error('service did not become healthy');
      }
      await new Promise((r) => setTimeout(r, 100));
    }
  }

  static newLeaseId() {
    return randomUUID();
  }
}
