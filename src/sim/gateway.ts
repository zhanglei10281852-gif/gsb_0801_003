/**
 * Simulated edge gateway. It claims line ownership, leases work from the
 * dispatch service, drives the SimDevice, and relays confirmations. It supports
 * scripted fault injection so disconnection, duplicate submits, out-of-order and
 * duplicate confirmations, and redundant-gateway takeover can be reproduced
 * deterministically.
 *
 * Ownership: a gateway calls `acquireOwnership(lineId)` to claim/heartbeat a
 * line. The returned generation (代际) is stamped on every subsequent
 * lease/renew/confirm. A gateway that has been taken over keeps using its OLD
 * generation, letting scenarios prove that stale-generation operations are
 * fenced by the service.
 */

import { httpJson } from './httpClient';
import { DeviceRunResult, SimDevice } from './device';

export interface LeaseResponse {
  commandId: string;
  executionId: string;
  leaseId: string;
  lineId: string;
  ownerGeneration: number | null;
  preempted: boolean;
  payload: { deviceId: string; kind: string; params: Record<string, unknown> };
  leaseExpiresAt: number;
  attempt: number;
}

export interface OwnershipResponse {
  outcome: 'acquired' | 'renewed' | 'takeover' | 'rejected';
  reason?: string;
  lineId: string;
  owner: string;
  generation: number;
  expiresAt: number;
}

export interface GatewayFaults {
  /** Drop the confirmation after the device executed (simulates going offline
   *  right after acting but before reporting). The device still recorded the
   *  action, so a later re-lease + re-confirm must not double-act. */
  dropConfirm?: boolean;
  /** Send the confirmation twice (duplicate/at-least-once relay). */
  duplicateConfirm?: boolean;
  /** After leasing, do not renew and do not confirm (gateway vanished). */
  vanishAfterLease?: boolean;
}

export interface HandleResult {
  action: 'idle' | 'handled' | 'vanished' | 'dropped';
  commandId?: string;
  executionId?: string;
  leaseId?: string;
  device?: DeviceRunResult;
  confirmStatus?: number;
  confirmAccepted?: boolean;
  confirmReason?: string;
}

export class SimGateway {
  /** Ownership generation this gateway believes it holds (null = none yet). */
  private generation: number | null = null;

  constructor(
    public readonly baseUrl: string,
    public readonly leaseholder: string,
    public readonly device: SimDevice,
    /** Production line this gateway serves; defaults to the device's line. */
    public readonly lineId: string = 'default',
  ) {}

  /** The generation this gateway currently believes it owns. */
  get currentGeneration(): number | null {
    return this.generation;
  }

  /** Force this gateway to keep using a specific (possibly stale) generation,
   *  simulating a partitioned gateway that has not learned it was taken over. */
  forceGeneration(gen: number | null): void {
    this.generation = gen;
  }

  /** Claim/heartbeat/take over ownership of the line. On success records the
   *  authoritative generation locally for later stamping. */
  async acquireOwnership(): Promise<OwnershipResponse> {
    const res = await httpJson<OwnershipResponse>(
      this.baseUrl,
      'POST',
      '/gateway/ownership',
      { lineId: this.lineId, gateway: this.leaseholder },
    );
    if (res.status === 200) {
      this.generation = res.body.generation;
    }
    return res.body;
  }

  /** Lease one task (if any) and drive it according to the injected faults.
   *  Leasing is scoped to this gateway's line + device and stamped with the
   *  gateway's ownership generation. */
  async pumpOnce(faults: GatewayFaults = {}): Promise<HandleResult> {
    const leaseRes = await httpJson<LeaseResponse>(
      this.baseUrl,
      'POST',
      '/gateway/lease',
      {
        leaseholder: this.leaseholder,
        lineId: this.lineId,
        deviceId: this.device.deviceId,
        generation: this.generation,
      },
    );
    if (leaseRes.status === 204) return { action: 'idle' };
    if (leaseRes.status !== 200) {
      throw new Error(`lease failed: ${leaseRes.status}`);
    }
    const task = leaseRes.body;

    // Gateway leased the task, then disappears: never renews, never confirms.
    if (faults.vanishAfterLease) {
      return {
        action: 'vanished',
        commandId: task.commandId,
        executionId: task.executionId,
        leaseId: task.leaseId,
      };
    }

    // Drive the physical device (idempotent by executionId).
    const device = this.device.execute({
      executionId: task.executionId,
      kind: task.payload.kind,
      params: task.payload.params,
    });

    // Simulate losing the network right after acting: device did its work but
    // the confirmation never reaches the service.
    if (faults.dropConfirm) {
      return {
        action: 'dropped',
        commandId: task.commandId,
        executionId: task.executionId,
        leaseId: task.leaseId,
        device,
      };
    }

    const confirm = () =>
      httpJson<{ accepted: boolean; reason?: string }>(
        this.baseUrl,
        'POST',
        '/gateway/confirm',
        {
          commandId: task.commandId,
          leaseId: task.leaseId,
          outcome: device.outcome,
          deviceReceipt: device.receipt,
          generation: this.generation,
        },
      );

    const first = await confirm();
    if (faults.duplicateConfirm) {
      await confirm(); // at-least-once relay; must be idempotent server-side
    }

    return {
      action: 'handled',
      commandId: task.commandId,
      executionId: task.executionId,
      leaseId: task.leaseId,
      device,
      confirmStatus: first.status,
      confirmAccepted: first.body?.accepted ?? false,
      confirmReason: first.body?.reason,
    };
  }

  /** Explicitly renew a held lease (used by scripted scenarios). */
  async renew(commandId: string, leaseId: string): Promise<number> {
    const res = await httpJson(this.baseUrl, 'POST', '/gateway/renew', {
      commandId,
      leaseId,
      generation: this.generation,
    });
    return res.status;
  }

  /** Explicitly relay a confirmation with an arbitrary (possibly stale) lease id
   *  and/or generation — used to reproduce out-of-order / late / split-brain
   *  confirmations. */
  async confirmRaw(
    commandId: string,
    leaseId: string,
    outcome: 'success' | 'failure',
    opts: { receipt?: string; generation?: number | null } = {},
  ): Promise<{ status: number; accepted: boolean; reason?: string }> {
    const res = await httpJson<{ accepted: boolean; reason?: string }>(
      this.baseUrl,
      'POST',
      '/gateway/confirm',
      {
        commandId,
        leaseId,
        outcome,
        deviceReceipt: opts.receipt,
        generation:
          opts.generation !== undefined ? opts.generation : this.generation,
      },
    );
    return {
      status: res.status,
      accepted: res.body?.accepted ?? false,
      reason: res.body?.reason,
    };
  }
}
