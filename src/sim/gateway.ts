/**
 * Simulated edge gateway. It leases work from the dispatch service, drives the
 * SimDevice, and relays confirmations. It supports scripted fault injection so
 * disconnection, duplicate submits, out-of-order and duplicate confirmations
 * can be reproduced deterministically.
 */

import { httpJson } from './httpClient';
import { DeviceRunResult, SimDevice } from './device';

export interface LeaseResponse {
  commandId: string;
  executionId: string;
  leaseId: string;
  payload: { deviceId: string; kind: string; params: Record<string, unknown> };
  leaseExpiresAt: number;
  attempt: number;
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
  device?: DeviceRunResult;
  confirmStatus?: number;
  confirmAccepted?: boolean;
}

export class SimGateway {
  constructor(
    public readonly baseUrl: string,
    public readonly leaseholder: string,
    public readonly device: SimDevice,
  ) {}

  /** Lease one task (if any) and drive it according to the injected faults.
   *  Leasing is scoped to this gateway's device so scenarios don't steal each
   *  other's work when they share a database. */
  async pumpOnce(faults: GatewayFaults = {}): Promise<HandleResult> {
    const leaseRes = await httpJson<LeaseResponse>(
      this.baseUrl,
      'POST',
      '/gateway/lease',
      { leaseholder: this.leaseholder, deviceId: this.device.deviceId },
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
        device,
      };
    }

    const confirm = () =>
      httpJson<{ accepted: boolean }>(this.baseUrl, 'POST', '/gateway/confirm', {
        commandId: task.commandId,
        leaseId: task.leaseId,
        outcome: device.outcome,
        deviceReceipt: device.receipt,
      });

    const first = await confirm();
    if (faults.duplicateConfirm) {
      await confirm(); // at-least-once relay; must be idempotent server-side
    }

    return {
      action: 'handled',
      commandId: task.commandId,
      executionId: task.executionId,
      device,
      confirmStatus: first.status,
      confirmAccepted: (first.body as { accepted: boolean }).accepted,
    };
  }

  /** Explicitly renew a held lease (used by scripted scenarios). */
  async renew(commandId: string, leaseId: string): Promise<number> {
    const res = await httpJson(this.baseUrl, 'POST', '/gateway/renew', {
      commandId,
      leaseId,
    });
    return res.status;
  }

  /** Explicitly relay a confirmation with an arbitrary (possibly stale) lease
   *  id — used to reproduce out-of-order / late confirmations. */
  async confirmRaw(
    commandId: string,
    leaseId: string,
    outcome: 'success' | 'failure',
    receipt?: string,
  ): Promise<{ status: number; accepted: boolean; reason?: string }> {
    const res = await httpJson<{ accepted: boolean; reason?: string }>(
      this.baseUrl,
      'POST',
      '/gateway/confirm',
      { commandId, leaseId, outcome, deviceReceipt: receipt },
    );
    return {
      status: res.status,
      accepted: res.body?.accepted ?? false,
      reason: res.body?.reason,
    };
  }
}
