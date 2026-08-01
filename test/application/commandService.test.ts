import { describe, it, expect, beforeEach } from 'vitest';
import { CommandService } from '../../src/application/commandService.js';
import {
  InMemoryCommandRepository,
  InMemoryEventStore,
  InMemoryUnitOfWork,
} from '../../src/infrastructure/memory/inMemoryAdapter.js';
import { Clock, IdGenerator } from '../../src/domain/stateMachine.js';

class FakeClock implements Clock {
  private t = 1_000_000;
  now(): number {
    return this.t;
  }
  advance(ms: number): void {
    this.t += ms;
  }
}

class SeqIdGen implements IdGenerator {
  private n = 0;
  newId(): string {
    this.n += 1;
    return `evt-${this.n}`;
  }
}

function makeService() {
  const commands = new InMemoryCommandRepository();
  const events = new InMemoryEventStore();
  const uow = new InMemoryUnitOfWork(commands, events);
  const clock = new FakeClock();
  const idGen = new SeqIdGen();
  const service = new CommandService(uow, commands, events, clock, idGen);
  return { service, commands, events, clock, idGen };
}

const payload = { deviceId: 'dev-1', action: 'calibrate' };

describe('CommandService — submit idempotency across retries', () => {
  it('returns the same commandId for duplicate idempotency key (crash-after-commit scenario)', async () => {
    const { service } = makeService();
    const first = await service.submit({ idempotencyKey: 'key-1', payload });
    const second = await service.submit({ idempotencyKey: 'key-1', payload });
    expect(first.created).toBe(true);
    expect(second.created).toBe(false);
    expect(second.command.commandId).toBe(first.command.commandId);
    expect(second.command.status).toBe('PENDING');
  });

  it('does not allow two different payloads to share an idempotency key', async () => {
    const { service } = makeService();
    const first = await service.submit({ idempotencyKey: 'key-1', payload });
    const second = await service.submit({
      idempotencyKey: 'key-1',
      payload: { deviceId: 'dev-OTHER', action: 'reset' },
    });
    expect(second.command.commandId).toBe(first.command.commandId);
    expect(second.command.payload).toEqual(payload);
  });
});

describe('CommandService — claim / renew / confirm lifecycle', () => {
  let setup: ReturnType<typeof makeService>;
  beforeEach(() => {
    setup = makeService();
  });

  it('happy path: submit -> claim -> report -> confirm -> SUCCEEDED with full event trail', async () => {
    const { service, clock } = setup;
    const submitted = await service.submit({ idempotencyKey: 'k', payload, maxAttempts: 3 });
    const claimed = await service.claim({ gatewayId: 'gw-1', leaseDurationMs: 5000 });
    expect(claimed.commandId).toBe(submitted.command.commandId);
    expect(claimed.status).toBe('CLAIMED');
    expect(claimed.attempt).toBe(1);

    clock.advance(100);
    await service.reportDelivery({
      commandId: claimed.commandId,
      leaseId: claimed.currentLeaseId!,
      gatewayId: 'gw-1',
    });

    clock.advance(100);
    const { command: confirmed, accepted } = await service.confirm({
      commandId: claimed.commandId,
      leaseId: claimed.currentLeaseId!,
      gatewayId: 'gw-1',
      confirmationCode: 'ACK-1',
      deviceTimestamp: clock.now(),
    });
    expect(accepted).toBe(true);
    expect(confirmed.status).toBe('SUCCEEDED');
    expect(confirmed.confirmationCode).toBe('ACK-1');

    const events = await service.getEvents(claimed.commandId);
    expect(events.map((e) => e.eventType)).toEqual([
      'CommandSubmitted',
      'CommandClaimed',
      'DeliveryReported',
      'DeviceConfirmed',
      'CommandSucceeded',
    ]);
  });

  it('renews lease and stays CLAIMED', async () => {
    const { service, clock } = setup;
    await service.submit({ idempotencyKey: 'k', payload });
    const claimed = await service.claim({ gatewayId: 'gw-1', leaseDurationMs: 5000 });
    clock.advance(4000);
    const renewed = await service.renew({
      commandId: claimed.commandId,
      leaseId: claimed.currentLeaseId!,
      gatewayId: 'gw-1',
      leaseDurationMs: 5000,
    });
    expect(renewed.status).toBe('CLAIMED');
    expect(renewed.leaseExpiresAt).toBe(clock.now() + 5000);
  });

  it('rejects renew with stale lease after expiry', async () => {
    const { service, clock } = setup;
    await service.submit({ idempotencyKey: 'k', payload });
    const claimed = await service.claim({ gatewayId: 'gw-1', leaseDurationMs: 1000 });
    clock.advance(1001);
    await expect(
      service.renew({
        commandId: claimed.commandId,
        leaseId: claimed.currentLeaseId!,
        gatewayId: 'gw-1',
        leaseDurationMs: 1000,
      })
    ).rejects.toThrow(/lease already expired/);
  });
});

describe('CommandService — gateway loss and reclaim', () => {
  let setup: ReturnType<typeof makeService>;
  beforeEach(() => {
    setup = makeService();
  });

  it('reclaims an expired lease with stable commandId and rejects old lease confirmation', async () => {
    const { service, clock } = setup;
    const submitted = await service.submit({ idempotencyKey: 'k', payload, maxAttempts: 3 });
    const first = await service.claim({ gatewayId: 'gw-1', leaseDurationMs: 1000 });
    const firstLease = first.currentLeaseId!;

    clock.advance(1001);
    const expired = await service.scanExpiredLeases();
    expect(expired).toHaveLength(1);
    expect(expired[0].commandId).toBe(submitted.command.commandId);
    expect(expired[0].failed).toBe(false);

    const second = await service.claim({ gatewayId: 'gw-2', leaseDurationMs: 1000 });
    expect(second.commandId).toBe(submitted.command.commandId);
    expect(second.attempt).toBe(2);
    expect(second.currentLeaseId).not.toBe(firstLease);

    const { command: staleResult } = await service.confirm({
      commandId: submitted.command.commandId,
      leaseId: firstLease,
      gatewayId: 'gw-1',
      confirmationCode: 'GHOST-ACK',
      deviceTimestamp: clock.now(),
    });
    expect(staleResult.status).toBe('CLAIMED');
    expect(staleResult.confirmationCode).toBeNull();

    const events = await service.getEvents(submitted.command.commandId);
    const types = events.map((e) => e.eventType);
    expect(types).toContain('LeaseExpired');
    expect(types.filter((t) => t === 'CommandClaimed').length).toBe(2);
    expect(types).toContain('StaleMessageRejected');
    expect(types).not.toContain('CommandSucceeded');
  });

  it('emits CommandReclaimed when a claim directly reaps an expired lease without a prior scan', async () => {
    const { service, clock } = setup;
    await service.submit({ idempotencyKey: 'k', payload, maxAttempts: 3 });
    const first = await service.claim({ gatewayId: 'gw-1', leaseDurationMs: 500 });
    clock.advance(501);

    const second = await service.claim({ gatewayId: 'gw-2', leaseDurationMs: 500 });
    expect(second.attempt).toBe(2);
    expect(second.commandId).toBe(first.commandId);

    const events = await service.getEvents(first.commandId);
    const types = events.map((e) => e.eventType);
    expect(types).toContain('CommandReclaimed');
    expect(types).not.toContain('LeaseExpired');
  });

  it('fails the command after max attempts exhausted', async () => {
    const { service, clock } = setup;
    await service.submit({ idempotencyKey: 'k', payload, maxAttempts: 2 });

    const first = await service.claim({ gatewayId: 'gw-1', leaseDurationMs: 500 });
    clock.advance(501);
    await service.scanExpiredLeases();

    const second = await service.claim({ gatewayId: 'gw-2', leaseDurationMs: 500 });
    expect(second.attempt).toBe(2);
    clock.advance(501);
    const expired = await service.scanExpiredLeases();
    expect(expired[0].failed).toBe(true);

    const final = await service.getCommand(first.commandId);
    expect(final!.status).toBe('FAILED');
    expect(final!.failureReason).toMatch(/max attempts/);
  });
});

describe('CommandService — late confirmation does not resurrect', () => {
  let setup: ReturnType<typeof makeService>;
  beforeEach(() => {
    setup = makeService();
  });

  it('late confirm after expiry and before reclaim is recorded but does not move state to SUCCEEDED', async () => {
    const { service, clock } = setup;
    await service.submit({ idempotencyKey: 'k', payload });
    const claimed = await service.claim({ gatewayId: 'gw-1', leaseDurationMs: 500 });
    const lease = claimed.currentLeaseId!;
    clock.advance(501);
    await service.scanExpiredLeases();

    const { command, accepted } = await service.confirm({
      commandId: claimed.commandId,
      leaseId: lease,
      gatewayId: 'gw-1',
      confirmationCode: 'LATE',
      deviceTimestamp: clock.now(),
    });
    expect(accepted).toBe(false);
    expect(command.status).toBe('PENDING');
    expect(command.confirmationCode).toBeNull();
  });
});
