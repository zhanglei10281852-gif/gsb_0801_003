export interface DeviceCommand {
  commandId: string;
  action: string;
  deviceId: string;
}

export interface DeviceConfirmation {
  commandId: string;
  confirmationCode: string;
  deviceTimestamp: number;
}

export type DeviceBehavior = {
  confirmDelayMs?: number;
  duplicateConfirm?: boolean;
  dropConfirm?: boolean;
};

export type DeviceConfirmHandler = (confirmation: DeviceConfirmation) => void;

export class DeviceSimulator {
  private handler: DeviceConfirmHandler | null = null;
  private received: DeviceCommand[] = [];
  private behavior: DeviceBehavior;

  constructor(
    public readonly deviceId: string,
    behavior: DeviceBehavior = {}
  ) {
    this.behavior = behavior;
  }

  setBehavior(behavior: DeviceBehavior): void {
    this.behavior = behavior;
  }

  onConfirm(handler: DeviceConfirmHandler): void {
    this.handler = handler;
  }

  async receive(command: DeviceCommand): Promise<void> {
    this.received.push(command);
    if (this.behavior.dropConfirm) {
      return;
    }
    const delay = this.behavior.confirmDelayMs ?? 50;
    await this.sleep(delay);
    const confirmation: DeviceConfirmation = {
      commandId: command.commandId,
      confirmationCode: `ACK-${command.commandId.slice(0, 8)}`,
      deviceTimestamp: Date.now(),
    };
    this.handler?.(confirmation);
    if (this.behavior.duplicateConfirm) {
      await this.sleep(20);
      this.handler?.({ ...confirmation });
    }
  }

  getReceived(): DeviceCommand[] {
    return [...this.received];
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
