export class KernelError extends Error {
  constructor(message, code = 'kernel-error', details = undefined) {
    super(message);
    this.name = 'KernelError';
    this.code = code;
    if (details !== undefined) this.details = details;
  }
}

export class ValidationError extends KernelError {
  constructor(message, details) {
    super(message, 'validation-error', details);
    this.name = 'ValidationError';
  }
}

export class NotFoundError extends KernelError {
  constructor(message, details) {
    super(message, 'not-found', details);
    this.name = 'NotFoundError';
  }
}

/**
 * Raised on optimistic-concurrency failure or when a GC precondition no longer
 * holds. `details.kind` distinguishes the cases so callers can render the
 * conflict explicitly instead of guessing.
 */
export class ConflictError extends KernelError {
  constructor(message, details) {
    super(message, 'conflict', details);
    this.name = 'ConflictError';
  }
}
