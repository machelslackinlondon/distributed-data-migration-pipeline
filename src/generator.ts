import { randomUUID } from "node:crypto";

import { abortError } from "./abort.js";
import type { SourceEventInput } from "./domain.js";
import type { AdmissionResult } from "./pipeline/migration-pipeline.js";

export interface GeneratorOptions {
  count: number;
  recordCount: number;
  ratePerSecond: number;
  admit: (input: SourceEventInput) => AdmissionResult;
  signal?: AbortSignal;
  eventId?: (index: number) => string;
  sleep?: (milliseconds: number, signal?: AbortSignal) => Promise<void>;
  now?: () => number;
}

export interface GeneratorResult {
  generated: number;
  throttled: number;
  durationMs: number;
}

export async function generateEvents(
  options: GeneratorOptions,
): Promise<GeneratorResult> {
  validatePositive("count", options.count);
  validatePositive("recordCount", options.recordCount);
  validatePositive("ratePerSecond", options.ratePerSecond);
  const sleep = options.sleep ?? abortableSleep;
  const now = options.now ?? (() => performance.now());
  const eventId = options.eventId ?? (() => randomUUID());
  const startedAt = now();
  let generated = 0;
  let throttled = 0;
  const subMillisecondBatch = Math.max(
    1,
    Math.floor(options.ratePerSecond / 1_000),
  );

  while (generated < options.count) {
    throwIfAborted(options.signal);
    const sequence = generated + 1;
    const recordIndex = generated % options.recordCount;
    const input: SourceEventInput = {
      eventId: eventId(sequence),
      recordId: `record-${recordIndex + 1}`,
      recordVersion: Math.floor(generated / options.recordCount) + 1,
      type: "upsert",
      payload: {
        name: `Record ${recordIndex + 1}`,
        email: `record-${recordIndex + 1}@example.com`,
        status: generated % 11 === 0 ? "inactive" : "active",
      },
    };

    while (true) {
      throwIfAborted(options.signal);
      const result = options.admit(input);
      if (result.accepted) {
        break;
      }
      throttled += 1;
      await sleep(10, options.signal);
    }

    generated += 1;
    if (options.ratePerSecond <= 1_000) {
      await sleep(1_000 / options.ratePerSecond, options.signal);
    } else if (generated % subMillisecondBatch === 0) {
      await sleep(1, options.signal);
    }
  }

  return {
    generated,
    throttled,
    durationMs: Math.max(0, now() - startedAt),
  };
}

function validatePositive(name: string, value: number): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
}

function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted === true) {
    throw abortError(signal);
  }
}

function abortableSleep(
  milliseconds: number,
  signal?: AbortSignal,
): Promise<void> {
  if (signal?.aborted === true) {
    return Promise.reject(abortError(signal));
  }
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, milliseconds);
    const onAbort = (): void => {
      clearTimeout(timer);
      reject(abortError(signal));
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
