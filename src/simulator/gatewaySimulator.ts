import { ApiClient, ApiCommand } from './apiClient.js';
import { DeviceSimulator, DeviceConfirmation } from './deviceSimulator.js';

export interface GatewayOptions {
  gatewayId: string;
  pollIntervalMs: number;
  leaseDurationMs: number;
  renewIntervalMs: number;
  api: ApiClient;
  device: DeviceSimulator;
}

type GatewayState = 'online' | 'offline';

interface HeldTask {
  command: ApiCommand;
  renewTimer: NodeJS.Timeout;
}

interface LastTask {
  commandId: string;
  leaseId: string;
}

export class GatewaySimulator {
  private state: GatewayState = 'online';
  private heldTask: HeldTask | null = null;
  private lastTask: LastTask | null = null;
  private pollTimer: NodeJS.Timeout | null = null;
  private log: string[] = [];
  private confirmBuffer: DeviceConfirmation[] = [];
  private duplicateConfirm = false;
  private dropConfirm = false;
  private delayConfirmMs = 0;

  public deliveredCommandIds: string[] = [];

  constructor(private readonly opts: GatewayOptions) {
    this.opts.device.onConfirm((c) => this.handleDeviceConfirm(c));
  }

  start(): void {
    this.schedulePoll(0);
  }

  stop(): void {
    if (this.pollTimer) clearTimeout(this.pollTimer);
    if (this.heldTask) clearInterval(this.heldTask.renewTimer);
    this.pollTimer = null;
    this.heldTask = null;
  }

  goOffline(): void {
    this.state = 'offline';
    if (this.heldTask) {
      clearInterval(this.heldTask.renewTimer);
    }
    this.log.push(`[${this.opts.gatewayId}] went OFFLINE (holding task, lease will expire)`);
  }

  goOnline(): void {
    this.state = 'online';
    this.log.push(`[${this.opts.gatewayId}] came ONLINE`);
    void this.flushBuffer();
    this.schedulePoll(0);
  }

  isOnline(): boolean {
    return this.state === 'online';
  }

  setDuplicateConfirm(v: boolean): void {
    this.duplicateConfirm = v;
  }

  setDropConfirm(v: boolean): void {
    this.dropConfirm = v;
  }

  setDelayConfirm(ms: number): void {
    this.delayConfirmMs = ms;
  }

  getHeldTask(): ApiCommand | null {
    return this.heldTask?.command ?? null;
  }

  getLogs(): string[] {
    return [...this.log];
  }

  private schedulePoll(delay: number): void {
    if (this.pollTimer) clearTimeout(this.pollTimer);
    this.pollTimer = setTimeout(() => {
      void this.poll();
    }, delay);
  }

  private async poll(): Promise<void> {
    if (this.state !== 'online') {
      this.schedulePoll(this.opts.pollIntervalMs);
      return;
    }
    if (this.heldTask) {
      this.schedulePoll(this.opts.pollIntervalMs);
      return;
    }
    try {
      const { status, body } = await this.opts.api.claim(
        this.opts.gatewayId,
        this.opts.leaseDurationMs,
        this.opts.device.deviceId
      );
      if (status === 200 && body) {
        this.acceptTask(body);
      }
    } catch (err) {
      this.log.push(`[${this.opts.gatewayId}] claim error: ${(err as Error).message}`);
    }
    this.schedulePoll(this.opts.pollIntervalMs);
  }

  private acceptTask(command: ApiCommand): void {
    this.log.push(
      `[${this.opts.gatewayId}] claimed ${command.commandId} (attempt ${command.attempt}, lease ${command.currentLeaseId})`
    );
    const renewTimer = setInterval(() => {
      void this.renew();
    }, this.opts.renewIntervalMs);
    this.heldTask = { command, renewTimer };
    this.lastTask = {
      commandId: command.commandId,
      leaseId: command.currentLeaseId!,
    };
    void this.dispatchToDevice(command);
  }

  private async dispatchToDevice(command: ApiCommand): Promise<void> {
    if (this.state !== 'online') {
      this.confirmBuffer.push({
        commandId: command.commandId,
        confirmationCode: `ACK-${command.commandId.slice(0, 8)}`,
        deviceTimestamp: Date.now(),
      });
      return;
    }
    this.deliveredCommandIds.push(command.commandId);
    try {
      await this.opts.api.reportDelivery({
        commandId: command.commandId,
        leaseId: command.currentLeaseId!,
        gatewayId: this.opts.gatewayId,
        deviceMessage: `dispatched to ${command.payload.deviceId}`,
      });
    } catch (err) {
      this.log.push(`[${this.opts.gatewayId}] report-delivery error: ${(err as Error).message}`);
    }
    try {
      await this.opts.device.receive({
        commandId: command.commandId,
        action: command.payload.action,
        deviceId: command.payload.deviceId,
      });
    } catch (err) {
      this.log.push(`[${this.opts.gatewayId}] device error: ${(err as Error).message}`);
    }
  }

  private async renew(): Promise<void> {
    if (!this.heldTask || this.state !== 'online') return;
    try {
      const { status, body } = await this.opts.api.renew({
        commandId: this.heldTask.command.commandId,
        leaseId: this.heldTask.command.currentLeaseId!,
        gatewayId: this.opts.gatewayId,
        leaseDurationMs: this.opts.leaseDurationMs,
      });
      if (status === 200 && body && 'commandId' in body) {
        this.heldTask.command = body as ApiCommand;
        this.lastTask = {
          commandId: body.commandId,
          leaseId: body.currentLeaseId!,
        };
      } else if (status === 409) {
        this.log.push(
          `[${this.opts.gatewayId}] renew rejected for ${this.heldTask.command.commandId}: ${JSON.stringify(body)}`
        );
        clearInterval(this.heldTask.renewTimer);
        this.heldTask = null;
      }
    } catch (err) {
      this.log.push(`[${this.opts.gatewayId}] renew error: ${(err as Error).message}`);
    }
  }

  private async handleDeviceConfirm(confirmation: DeviceConfirmation): Promise<void> {
    if (this.delayConfirmMs > 0) {
      await this.sleep(this.delayConfirmMs);
    }
    if (this.state !== 'online' || this.dropConfirm) {
      this.confirmBuffer.push(confirmation);
      this.log.push(
        `[${this.opts.gatewayId}] buffered confirmation for ${confirmation.commandId} (offline or dropped)`
      );
      return;
    }
    await this.sendConfirm(confirmation);
    if (this.duplicateConfirm) {
      await this.sleep(30);
      await this.sendConfirm(confirmation);
    }
  }

  private async sendConfirm(confirmation: DeviceConfirmation): Promise<void> {
    const leaseRef = this.heldTask ?? this.lastTask;
    if (!leaseRef) {
      this.log.push(
        `[${this.opts.gatewayId}] cannot confirm ${confirmation.commandId}: no active or last task`
      );
      return;
    }
    const commandId =
      'command' in leaseRef ? leaseRef.command.commandId : leaseRef.commandId;
    const leaseId =
      'command' in leaseRef ? leaseRef.command.currentLeaseId! : leaseRef.leaseId;
    try {
      const { status, body } = await this.opts.api.confirm({
        commandId,
        leaseId,
        gatewayId: this.opts.gatewayId,
        confirmationCode: confirmation.confirmationCode,
        deviceTimestamp: confirmation.deviceTimestamp,
      });
      this.log.push(
        `[${this.opts.gatewayId}] confirm for ${confirmation.commandId}: status=${status} accepted=${'accepted' in body ? body.accepted : 'n/a'}`
      );
      if (status === 200 && body.command?.status === 'SUCCEEDED') {
        if (this.heldTask) {
          clearInterval(this.heldTask.renewTimer);
          this.heldTask = null;
        }
      }
    } catch (err) {
      this.log.push(`[${this.opts.gatewayId}] confirm error: ${(err as Error).message}`);
      this.confirmBuffer.push(confirmation);
    }
  }

  async replayBufferedConfirm(commandId: string): Promise<void> {
    const idx = this.confirmBuffer.findIndex((c) => c.commandId === commandId);
    if (idx >= 0) {
      const [confirmation] = this.confirmBuffer.splice(idx, 1);
      await this.sendConfirm(confirmation);
    }
  }

  async sendLateConfirmWithOldLease(commandId: string, oldLeaseId: string): Promise<void> {
    const confirmation = {
      commandId,
      confirmationCode: `LATE-${commandId.slice(0, 8)}`,
      deviceTimestamp: Date.now(),
    };
    try {
      const { status, body } = await this.opts.api.confirm({
        commandId,
        leaseId: oldLeaseId,
        gatewayId: this.opts.gatewayId,
        confirmationCode: confirmation.confirmationCode,
        deviceTimestamp: confirmation.deviceTimestamp,
      });
      this.log.push(
        `[${this.opts.gatewayId}] LATE confirm with old lease ${oldLeaseId}: status=${status} accepted=${'accepted' in body ? body.accepted : 'n/a'}`
      );
    } catch (err) {
      this.log.push(`[${this.opts.gatewayId}] late confirm error: ${(err as Error).message}`);
    }
  }

  private async flushBuffer(): Promise<void> {
    while (this.confirmBuffer.length > 0) {
      const c = this.confirmBuffer.shift()!;
      await this.sendConfirm(c);
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
