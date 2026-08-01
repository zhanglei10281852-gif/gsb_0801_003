import { randomUUID } from 'node:crypto';
import { Clock, IdGenerator } from '../domain/stateMachine.js';

export class SystemClock implements Clock {
  now(): number {
    return Date.now();
  }
}

export class UuidGenerator implements IdGenerator {
  newId(): string {
    return randomUUID();
  }
}
