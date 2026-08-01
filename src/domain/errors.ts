export class DomainError extends Error {
  constructor(
    message: string,
    public readonly code: string,
    public readonly details?: Record<string, unknown>
  ) {
    super(message);
    this.name = 'DomainError';
  }
}

export class IdempotencyConflictError extends DomainError {
  constructor(public readonly existingCommandId: string) {
    super(
      `Idempotency key already maps to command ${existingCommandId}`,
      'IDEMPOTENCY_CONFLICT'
    );
    this.name = 'IdempotencyConflictError';
  }
}

export class CommandNotFoundError extends DomainError {
  constructor(commandId: string) {
    super(`Command ${commandId} not found`, 'COMMAND_NOT_FOUND');
    this.name = 'CommandNotFoundError';
  }
}

export class InvalidLeaseError extends DomainError {
  constructor(
    commandId: string,
    expectedLeaseId: string | null,
    providedLeaseId: string,
    reason: string
  ) {
    super(
      `Invalid lease for command ${commandId}: ${reason}`,
      'INVALID_LEASE',
      { expectedLeaseId, providedLeaseId, reason }
    );
    this.name = 'InvalidLeaseError';
  }
}

export class InvalidStateTransitionError extends DomainError {
  constructor(
    commandId: string,
    currentStatus: string,
    attemptedAction: string
  ) {
    super(
      `Cannot ${attemptedAction} command ${commandId} in state ${currentStatus}`,
      'INVALID_STATE_TRANSITION',
      { currentStatus, attemptedAction }
    );
    this.name = 'InvalidStateTransitionError';
  }
}

export class NoClaimableTaskError extends DomainError {
  constructor() {
    super('No claimable task available', 'NO_CLAIMABLE_TASK');
    this.name = 'NoClaimableTaskError';
  }
}
