/**
 * Pure ownership state machine for redundant (active/standby) gateways on one
 * production line.
 *
 * A line is owned by at most one gateway at a time via a heartbeat lease. The
 * owner renews (heartbeats) before `expiresAt`; if it fails to, a standby gateway
 * may TAKE OVER, which bumps the monotonic `generation` (代际). A standby cannot
 * steal ownership from a HEALTHY owner (one whose lease is still live) — takeover
 * is only permitted once the current owner's heartbeat has lapsed. This is the
 * split-brain guard: during a network partition the old owner's ownership lease
 * expires from the service's point of view, the standby takes over with a higher
 * generation, and the old owner is thereafter fenced (see the command state
 * machine's generation checks).
 *
 * Pure: no I/O, injected `now`. `generation` only ever increases.
 */

import { DomainEvent, LineOwnership, OwnershipTransition } from './types';

function lineEvent(
  lineId: string,
  type: DomainEvent['type'],
  at: number,
  generation: number | null,
  detail: Record<string, unknown>,
): DomainEvent {
  return {
    subject: 'line',
    subjectId: lineId,
    commandId: null,
    type,
    at,
    attemptEpoch: null,
    generation,
    detail,
  };
}

export interface ClaimInput {
  lineId: string;
  claimant: string; // gateway id requesting ownership
  now: number;
  ownershipTtlMs: number;
}

/**
 * Establish ownership of a line for the first time (generation 1). The caller
 * must ensure no ownership row exists yet (enforced by the repository's unique
 * lineId); this returns the initial record.
 */
export function acquire(input: ClaimInput): OwnershipTransition {
  const ownership: LineOwnership = {
    lineId: input.lineId,
    owner: input.claimant,
    generation: 1,
    expiresAt: input.now + input.ownershipTtlMs,
    createdAt: input.now,
    updatedAt: input.now,
  };
  return {
    ownership,
    changed: true,
    outcome: { kind: 'acquired' },
    events: [
      lineEvent(input.lineId, 'OWNERSHIP_ACQUIRED', input.now, 1, {
        owner: input.claimant,
        expiresAt: ownership.expiresAt,
      }),
    ],
  };
}

/**
 * Apply a claim against an EXISTING ownership record. Three cases:
 *  - the claimant already owns the line -> heartbeat renew (generation unchanged);
 *  - the current owner's lease has expired -> takeover (generation++);
 *  - a different gateway claims while the owner is still healthy -> rejected.
 */
export function claim(
  current: LineOwnership,
  input: ClaimInput,
): OwnershipTransition {
  const ownerAlive = current.expiresAt > input.now;

  // Case 1: current owner renewing its own heartbeat.
  if (current.owner === input.claimant) {
    const renewed: LineOwnership = {
      ...current,
      expiresAt: input.now + input.ownershipTtlMs,
      updatedAt: input.now,
    };
    return {
      ownership: renewed,
      changed: true,
      outcome: { kind: 'renewed' },
      events: [
        lineEvent(current.lineId, 'OWNERSHIP_RENEWED', input.now, current.generation, {
          owner: input.claimant,
          expiresAt: renewed.expiresAt,
        }),
      ],
    };
  }

  // Case 2: a different gateway wants ownership while the owner is still alive.
  // Reject — we never preempt a healthy primary (that would be split brain).
  if (ownerAlive) {
    return {
      ownership: current,
      changed: false,
      outcome: { kind: 'rejected', reason: 'owner_alive' },
      events: [
        lineEvent(current.lineId, 'OWNERSHIP_REJECTED', input.now, current.generation, {
          claimant: input.claimant,
          currentOwner: current.owner,
          reason: 'owner_alive',
          ownerExpiresAt: current.expiresAt,
        }),
      ],
    };
  }

  // Case 3: owner's heartbeat lapsed -> standby takes over, bumping generation.
  const nextGen = current.generation + 1;
  const takenOver: LineOwnership = {
    ...current,
    owner: input.claimant,
    generation: nextGen,
    expiresAt: input.now + input.ownershipTtlMs,
    updatedAt: input.now,
  };
  return {
    ownership: takenOver,
    changed: true,
    outcome: { kind: 'takeover' },
    events: [
      lineEvent(current.lineId, 'OWNERSHIP_TAKEOVER', input.now, nextGen, {
        newOwner: input.claimant,
        previousOwner: current.owner,
        previousGeneration: current.generation,
        previousExpiresAt: current.expiresAt,
        expiresAt: takenOver.expiresAt,
      }),
    ],
  };
}
