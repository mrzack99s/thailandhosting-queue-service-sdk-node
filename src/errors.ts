/** An error answer from the service, read from its Problem Details (RFC 9457).
 *  `code` is the machine-readable reason, for example `queue_not_found`,
 *  `queue_full`, `invalid_receipt` or `busy`; the message is the problem's title. */
export class QueueServiceError extends Error {
  readonly status: number;
  readonly code: string;
  /** The problem's `type` URI and `instance` (the request path). */
  readonly type?: string;
  readonly instance?: string;

  constructor(status: number, code: string, message: string, problem: { type?: string; instance?: string } = {}) {
    super(`${code} (HTTP ${status}): ${message}`);
    this.name = 'QueueServiceError';
    this.status = status;
    this.code = code;
    this.type = problem.type;
    this.instance = problem.instance;
  }
}

/** The queue does not exist, or the key may not reach it (code `queue_not_found`). */
export class NotFoundError extends QueueServiceError {
  constructor(code: string, message: string, problem?: { type?: string; instance?: string }) {
    super(404, code, message, problem);
    this.name = 'NotFoundError';
  }
}
