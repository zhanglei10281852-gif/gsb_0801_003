/**
 * In-memory repository for deterministic tests. Mirrors the SQLite adapter's
 * semantics: unique idempotency key, atomic (all-or-nothing) transactions via a
 * snapshot/rollback, insertion-ordered events, and persisted line ownership.
 */

import { Command, DomainEvent, LineOwnership } from '../domain/types';
import { Repository, RepoTx } from '../ports';

export class InMemoryRepository implements Repository {
  private commands = new Map<string, Command>();
  private byKey = new Map<string, string>(); // idempotencyKey -> id
  private ownership = new Map<string, LineOwnership>(); // lineId -> record
  private events: DomainEvent[] = [];
  private inTx = false;

  transaction<T>(work: (tx: RepoTx) => T): T {
    if (this.inTx) {
      throw new Error('nested transactions are not supported');
    }
    this.inTx = true;
    // Snapshot for rollback on throw (keeps the "atomic" contract honest).
    const cmdSnap = new Map(this.commands);
    const keySnap = new Map(this.byKey);
    const ownSnap = new Map(this.ownership);
    const evtSnapLen = this.events.length;
    const tx = new MemTx(this);
    try {
      const result = work(tx);
      this.inTx = false;
      return result;
    } catch (err) {
      this.commands = cmdSnap;
      this.byKey = keySnap;
      this.ownership = ownSnap;
      this.events.length = evtSnapLen;
      this.inTx = false;
      throw err;
    }
  }

  // --- internal mutators used by MemTx ---
  _insert(c: Command): void {
    if (this.commands.has(c.id)) {
      throw new Error(`duplicate id ${c.id}`);
    }
    if (this.byKey.has(c.idempotencyKey)) {
      throw new Error(`duplicate idempotency_key ${c.idempotencyKey}`);
    }
    this.commands.set(c.id, c);
    this.byKey.set(c.idempotencyKey, c.id);
  }

  _update(c: Command): void {
    if (!this.commands.has(c.id)) {
      throw new Error(`update of missing id ${c.id}`);
    }
    this.commands.set(c.id, c);
  }

  _append(events: DomainEvent[]): void {
    for (const e of events) this.events.push(e);
  }

  _get(id: string): Command | null {
    return this.commands.get(id) ?? null;
  }

  _getByKey(key: string): Command | null {
    const id = this.byKey.get(key);
    return id ? (this.commands.get(id) ?? null) : null;
  }

  _getOwnership(lineId: string): LineOwnership | null {
    return this.ownership.get(lineId) ?? null;
  }

  _putOwnership(o: LineOwnership): void {
    this.ownership.set(o.lineId, o);
  }

  // --- read side (Repository) ---
  getById(id: string): Command | null {
    return this._get(id);
  }

  getByIdempotencyKey(key: string): Command | null {
    return this._getByKey(key);
  }

  eventsFor(subjectId: string): DomainEvent[] {
    return this.events.filter((e) => e.subjectId === subjectId);
  }

  recentEvents(limit: number): DomainEvent[] {
    return this.events.slice(-limit).reverse();
  }

  listCommands(status?: Command['status'], limit = 100): Command[] {
    let all = [...this.commands.values()].sort(
      (a, b) => a.createdAt - b.createdAt,
    );
    if (status) all = all.filter((c) => c.status === status);
    return all.slice(0, limit);
  }

  findPendingIds(
    filter: { lineId?: string; deviceId?: string },
    limit: number,
  ): string[] {
    return [...this.commands.values()]
      .filter(
        (c) =>
          c.status === 'PENDING' &&
          (filter.lineId === undefined || c.lineId === filter.lineId) &&
          (filter.deviceId === undefined || c.payload.deviceId === filter.deviceId),
      )
      .sort((a, b) => a.createdAt - b.createdAt)
      .slice(0, limit)
      .map((c) => c.id);
  }

  findExpiredLeaseIds(
    now: number,
    limit: number,
    filter: { lineId?: string; deviceId?: string } = {},
  ): string[] {
    return [...this.commands.values()]
      .filter(
        (c) =>
          c.status === 'LEASED' &&
          c.leaseExpiresAt !== null &&
          c.leaseExpiresAt <= now &&
          (filter.lineId === undefined || c.lineId === filter.lineId) &&
          (filter.deviceId === undefined || c.payload.deviceId === filter.deviceId),
      )
      .sort((a, b) => (a.leaseExpiresAt ?? 0) - (b.leaseExpiresAt ?? 0))
      .slice(0, limit)
      .map((c) => c.id);
  }

  getOwnership(lineId: string): LineOwnership | null {
    return this._getOwnership(lineId);
  }

  listOwnership(): LineOwnership[] {
    return [...this.ownership.values()].sort((a, b) =>
      a.lineId.localeCompare(b.lineId),
    );
  }

  close(): void {
    /* nothing to release */
  }
}

class MemTx implements RepoTx {
  constructor(private readonly repo: InMemoryRepository) {}
  getById(id: string): Command | null {
    return this.repo._get(id);
  }
  getByIdempotencyKey(key: string): Command | null {
    return this.repo._getByKey(key);
  }
  insert(c: Command): void {
    this.repo._insert(c);
  }
  update(c: Command): void {
    this.repo._update(c);
  }
  appendEvents(events: DomainEvent[]): void {
    this.repo._append(events);
  }
  getOwnership(lineId: string): LineOwnership | null {
    return this.repo._getOwnership(lineId);
  }
  putOwnership(o: LineOwnership): void {
    this.repo._putOwnership(o);
  }
}
