/**
 * Simulated device. It executes an incoming action at most once per
 * executionId: the stable execution identity is the dedup key, mirroring how a
 * real PLC/controller would ignore a re-delivered command it already ran. This
 * is how the whole system upholds "one business request => at most one device
 * action" even under at-least-once delivery.
 */

export interface DeviceAction {
  executionId: string;
  kind: string;
  params: Record<string, unknown>;
}

export interface DeviceRunResult {
  executionId: string;
  outcome: 'success' | 'failure';
  duplicate: boolean; // true if this executionId was already executed
  receipt: string;
}

export class SimDevice {
  /** executionId -> receipt of the first execution. */
  private executed = new Map<string, string>();
  private counter = 0;

  constructor(
    public readonly deviceId: string,
    /** Deterministic outcome selector; defaults to always-success. */
    private readonly outcomeFor: (a: DeviceAction) => 'success' | 'failure' = () =>
      'success',
  ) {}

  /** Number of distinct device actions actually performed. */
  get distinctActions(): number {
    return this.executed.size;
  }

  execute(action: DeviceAction): DeviceRunResult {
    const prior = this.executed.get(action.executionId);
    if (prior !== undefined) {
      // Idempotent: report the original receipt, do NOT act again.
      return {
        executionId: action.executionId,
        outcome: this.outcomeFor(action),
        duplicate: true,
        receipt: prior,
      };
    }
    const receipt = `rcpt_${this.deviceId}_${++this.counter}`;
    this.executed.set(action.executionId, receipt);
    return {
      executionId: action.executionId,
      outcome: this.outcomeFor(action),
      duplicate: false,
      receipt,
    };
  }
}
