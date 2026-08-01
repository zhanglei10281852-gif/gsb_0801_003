import { Command, CommandEvent } from '../domain/types.js';

export interface CommandRepository {
  findByCommandId(commandId: string): Promise<Command | null>;
  findByIdempotencyKey(key: string): Promise<Command | null>;
  findClaimable(now: number, deviceId?: string): Promise<Command | null>;
  findExpiredLeases(now: number, limit: number): Promise<Command[]>;

  save(command: Command, events: CommandEvent[]): Promise<void>;
  saveAll(items: Array<{ command: Command; events: CommandEvent[] }>): Promise<void>;
}

export interface EventStore {
  append(events: CommandEvent[]): Promise<void>;
  getEvents(commandId: string): Promise<CommandEvent[]>;
  getAllEvents(limit?: number): Promise<CommandEvent[]>;
}

export interface UnitOfWork {
  transaction<T>(fn: (repos: { commands: CommandRepository; events: EventStore }) => Promise<T>): Promise<T>;
}
