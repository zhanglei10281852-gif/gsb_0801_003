import {
  ClaimInput,
  Command,
  CommandEvent,
  ConfirmInput,
  ExpireResult,
  RenewInput,
  ReportDeliveryInput,
  SubmitCommandInput,
} from '../domain/types.js';
import {
  claimCommand,
  confirmCommand,
  createPendingCommand,
  expireLease,
  renewLease,
  reportDelivery,
  Clock,
  IdGenerator,
} from '../domain/stateMachine.js';
import {
  CommandNotFoundError,
  NoClaimableTaskError,
} from '../domain/errors.js';
import { CommandRepository, EventStore, UnitOfWork } from './ports.js';

export class CommandService {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly commands: CommandRepository,
    private readonly events: EventStore,
    private readonly clock: Clock,
    private readonly idGen: IdGenerator
  ) {}

  async submit(input: SubmitCommandInput): Promise<{ command: Command; created: boolean }> {
    return this.uow.transaction(async (tx) => {
      const existing = await tx.commands.findByIdempotencyKey(input.idempotencyKey);
      const decision = createPendingCommand(input, existing, this.idGen, this.clock);
      if (existing) {
        return { command: existing, created: false };
      }
      await tx.commands.save(decision.command, decision.events);
      return { command: decision.command, created: true };
    });
  }

  async claim(input: ClaimInput): Promise<Command> {
    return this.uow.transaction(async (tx) => {
      const now = this.clock.now();
      const candidate = await tx.commands.findClaimable(now, input.deviceId);
      if (!candidate) {
        throw new NoClaimableTaskError();
      }
      const decision = claimCommand(candidate, input, this.idGen, this.clock);
      await tx.commands.save(decision.command, decision.events);
      return decision.command;
    });
  }

  async renew(input: RenewInput): Promise<Command> {
    return this.uow.transaction(async (tx) => {
      const command = await tx.commands.findByCommandId(input.commandId);
      if (!command) throw new CommandNotFoundError(input.commandId);
      const decision = renewLease(command, input, this.idGen, this.clock);
      await tx.commands.save(decision.command, decision.events);
      return decision.command;
    });
  }

  async reportDelivery(input: ReportDeliveryInput): Promise<Command> {
    return this.uow.transaction(async (tx) => {
      const command = await tx.commands.findByCommandId(input.commandId);
      if (!command) throw new CommandNotFoundError(input.commandId);
      const decision = reportDelivery(command, input, this.idGen, this.clock);
      await tx.commands.save(decision.command, decision.events);
      return decision.command;
    });
  }

  async confirm(input: ConfirmInput): Promise<{ command: Command; accepted: boolean }> {
    return this.uow.transaction(async (tx) => {
      const command = await tx.commands.findByCommandId(input.commandId);
      if (!command) throw new CommandNotFoundError(input.commandId);
      const before = command.status;
      const decision = confirmCommand(command, input, this.idGen, this.clock);
      await tx.commands.save(decision.command, decision.events);
      const accepted = decision.command.status === 'SUCCEEDED' && before !== 'SUCCEEDED';
      return { command: decision.command, accepted };
    });
  }

  async scanExpiredLeases(batchSize = 50): Promise<ExpireResult[]> {
    return this.uow.transaction(async (tx) => {
      const now = this.clock.now();
      const expired = await tx.commands.findExpiredLeases(now, batchSize);
      const results: ExpireResult[] = [];
      for (const command of expired) {
        const outcome = expireLease(command, this.idGen, this.clock);
        if (outcome) {
          await tx.commands.save(outcome.decision.command, outcome.decision.events);
          results.push(outcome.result);
        }
      }
      return results;
    });
  }

  async getCommand(commandId: string): Promise<Command | null> {
    return this.commands.findByCommandId(commandId);
  }

  async getCommandByIdempotencyKey(key: string): Promise<Command | null> {
    return this.commands.findByIdempotencyKey(key);
  }

  async getEvents(commandId: string): Promise<CommandEvent[]> {
    return this.events.getEvents(commandId);
  }

  async getAllEvents(limit?: number): Promise<CommandEvent[]> {
    return this.events.getAllEvents(limit);
  }
}
