import { abortError } from "../abort.js";
import type { ApplyOutcome, TargetRecord } from "../domain.js";
import { Semaphore } from "../structures/semaphore.js";

export class RetryableTargetError extends Error {
  readonly retryable = true;
}

export class TargetUnavailableError extends RetryableTargetError {
  constructor(message = "target unavailable") {
    super(message);
    this.name = "TargetUnavailableError";
  }
}

export class TargetTimeoutError extends RetryableTargetError {
  constructor() {
    super("target write timed out");
    this.name = "TargetTimeoutError";
  }
}

export interface TargetWriter {
  apply(
    record: TargetRecord,
    signal: AbortSignal,
  ): ApplyOutcome | Promise<ApplyOutcome>;
}

export interface FaultPolicy {
  beforeWrite(
    record: TargetRecord,
    attempt: number,
    signal: AbortSignal,
  ): void | Promise<void>;
}

export interface TargetAdapterOptions {
  concurrency: number;
  timeoutMs: number;
  faultPolicy?: FaultPolicy;
}

const noFaults: FaultPolicy = {
  beforeWrite: () => undefined,
};

export class TargetAdapter {
  private readonly writer: TargetWriter;
  private readonly semaphore: Semaphore;
  private readonly timeoutMs: number;
  private readonly faultPolicy: FaultPolicy;

  constructor(writer: TargetWriter, options: TargetAdapterOptions) {
    if (!Number.isSafeInteger(options.timeoutMs) || options.timeoutMs <= 0) {
      throw new Error("target timeout must be a positive integer");
    }
    this.writer = writer;
    this.semaphore = new Semaphore(options.concurrency);
    this.timeoutMs = options.timeoutMs;
    this.faultPolicy = options.faultPolicy ?? noFaults;
  }

  get active(): number {
    return this.semaphore.active;
  }

  get pending(): number {
    return this.semaphore.pending;
  }

  async write(
    record: TargetRecord,
    attempt: number,
    signal?: AbortSignal,
  ): Promise<ApplyOutcome> {
    const release = await this.semaphore.acquire();
    const controller = new AbortController();
    const onExternalAbort = (): void => controller.abort(abortError(signal));
    signal?.addEventListener("abort", onExternalAbort, { once: true });
    if (signal?.aborted === true) {
      controller.abort(abortError(signal));
    }
    const timer = setTimeout(
      () => controller.abort(new TargetTimeoutError()),
      this.timeoutMs,
    );

    try {
      const operation = this.execute(record, attempt, controller.signal);
      const aborted = abortPromise(controller.signal);
      return await Promise.race([operation, aborted]);
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onExternalAbort);
      release();
    }
  }

  private async execute(
    record: TargetRecord,
    attempt: number,
    signal: AbortSignal,
  ): Promise<ApplyOutcome> {
    await this.faultPolicy.beforeWrite(record, attempt, signal);
    if (signal.aborted) {
      throw abortError(signal);
    }
    return await this.writer.apply(record, signal);
  }
}

function abortPromise(signal: AbortSignal): Promise<never> {
  if (signal.aborted) {
    return Promise.reject(abortError(signal));
  }
  return new Promise<never>((_resolve, reject) => {
    signal.addEventListener("abort", () => reject(abortError(signal)), {
      once: true,
    });
  });
}
