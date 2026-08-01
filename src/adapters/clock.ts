/** System clock and crypto-random id adapters (production defaults). */

import { randomUUID } from 'node:crypto';
import { Clock, IdGenerator } from '../ports';

export const systemClock: Clock = {
  now: () => Date.now(),
};

export const uuidIds: IdGenerator = {
  newCommandId: () => `cmd_${randomUUID()}`,
  newLeaseId: () => `lease_${randomUUID()}`,
};
