import { Command, CommandEvent } from '../../domain/types.js';
import {
  CommandRepository,
  EventStore,
  UnitOfWork,
} from '../../application/ports.js';

type EventAppender = (events: CommandEvent[]) => void;

export class InMemoryCommandRepository implements CommandRepository {
  byId = new Map<string, Command>();
  byKey = new Map<string, string>();
  private readonly appender: EventAppender | null;

  constructor(appender: EventAppender | null = null) {
    this.appender = appender;
  }

  async findByCommandId(commandId: string): Promise<Command | null> {
    const c = this.byId.get(commandId);
    return c ? { ...c } : null;
  }

  async findByIdempotencyKey(key: string): Promise<Command | null> {
    const id = this.byKey.get(key);
    if (!id) return null;
    const c = this.byId.get(id);
    return c ? { ...c } : null;
  }

  async findClaimable(now: number, deviceId?: string): Promise<Command | null> {
    let best: Command | null = null;
    for (const c of this.byId.values()) {
      if (deviceId && c.payload.deviceId !== deviceId) continue;
      if (c.status === 'PENDING') {
        if (!best || c.updatedAt < best.updatedAt) best = c;
      } else if (
        c.status === 'CLAIMED' &&
        c.leaseExpiresAt !== null &&
        c.leaseExpiresAt <= now
      ) {
        if (!best || c.updatedAt < best.updatedAt) best = c;
      }
    }
    return best ? { ...best } : null;
  }

  async findExpiredLeases(now: number, limit: number): Promise<Command[]> {
    const result: Command[] = [];
    for (const c of this.byId.values()) {
      if (
        c.status === 'CLAIMED' &&
        c.leaseExpiresAt !== null &&
        c.leaseExpiresAt <= now
      ) {
        result.push({ ...c });
      }
    }
    result.sort((a, b) => (a.updatedAt < b.updatedAt ? -1 : 1));
    return result.slice(0, limit);
  }

  async save(command: Command, events: CommandEvent[]): Promise<void> {
    this.byId.set(command.commandId, { ...command });
    this.byKey.set(command.idempotencyKey, command.commandId);
    if (this.appender && events.length > 0) {
      this.appender(events.map((e) => ({ ...e })));
    }
  }

  async saveAll(
    items: Array<{ command: Command; events: CommandEvent[] }>
  ): Promise<void> {
    for (const item of items) {
      await this.save(item.command, item.events);
    }
  }
}

export class InMemoryEventStore implements EventStore {
  events: CommandEvent[] = [];

  async append(events: CommandEvent[]): Promise<void> {
    for (const e of events) this.events.push({ ...e });
  }

  async getEvents(commandId: string): Promise<CommandEvent[]> {
    return this.events
      .filter((e) => e.commandId === commandId)
      .sort((a, b) => a.timestamp - b.timestamp)
      .map((e) => ({ ...e }));
  }

  async getAllEvents(limit?: number): Promise<CommandEvent[]> {
    const sorted = [...this.events].sort((a, b) => a.timestamp - b.timestamp);
    return (limit ? sorted.slice(-limit) : sorted).map((e) => ({ ...e }));
  }
}

export class InMemoryUnitOfWork implements UnitOfWork {
  constructor(
    private readonly commands: InMemoryCommandRepository,
    private readonly eventStore: InMemoryEventStore
  ) {}

  async transaction<T>(
    fn: (repos: {
      commands: CommandRepository;
      events: EventStore;
    }) => Promise<T>
  ): Promise<T> {
    const snapshotCommands = new Map(this.commands.byId);
    const snapshotKey = new Map(this.commands.byKey);
    const snapshotEvents = [...this.eventStore.events];

    const txEvents = new InMemoryEventStore();
    txEvents.events = [...this.eventStore.events];
    const txCommands = new InMemoryCommandRepository((events) => {
      for (const e of events) txEvents.events.push(e);
    });
    txCommands.byId = new Map(this.commands.byId);
    txCommands.byKey = new Map(this.commands.byKey);

    try {
      const result = await fn({ commands: txCommands, events: txEvents });
      this.commands.byId = txCommands.byId;
      this.commands.byKey = txCommands.byKey;
      this.eventStore.events = txEvents.events;
      return result;
    } catch (err) {
      this.commands.byId = snapshotCommands;
      this.commands.byKey = snapshotKey;
      this.eventStore.events = snapshotEvents;
      throw err;
    }
  }
}
