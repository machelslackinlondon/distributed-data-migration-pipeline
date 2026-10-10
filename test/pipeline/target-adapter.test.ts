import { afterEach, describe, expect, jest, test } from "@jest/globals";

import type { ApplyOutcome, TargetRecord } from "../../src/domain.js";
import {
  TargetAdapter,
  TargetTimeoutError,
  TargetUnavailableError,
  type FaultPolicy,
  type TargetWriter,
} from "../../src/pipeline/target-adapter.js";

afterEach(() => {
  jest.useRealTimers();
});

describe("TargetAdapter", () => {
  test("enforces target concurrency independently of worker demand", async () => {
    const writer = new ControlledWriter();
    const adapter = new TargetAdapter(writer, {
      concurrency: 2,
      timeoutMs: 1_000,
    });

    const writes = [
      adapter.write(targetRecord("record-1"), 1),
      adapter.write(targetRecord("record-2"), 1),
      adapter.write(targetRecord("record-3"), 1),
    ];
    await settleMicrotasks();

    expect(writer.active).toBe(2);
    expect(writer.highWater).toBe(2);
    expect(adapter.active).toBe(2);
    expect(adapter.pending).toBe(1);

    writer.releaseNext();
    await settleMicrotasks();
    expect(writer.highWater).toBe(2);
    expect(writer.active).toBe(2);

    writer.releaseNext();
    writer.releaseNext();
    await expect(Promise.all(writes)).resolves.toEqual([
      "applied",
      "applied",
      "applied",
    ]);
    expect(adapter.active).toBe(0);
  });

  test("aborts and classifies a target timeout as retryable", async () => {
    jest.useFakeTimers({ now: 0 });
    const adapter = new TargetAdapter(new NeverWriter(), {
      concurrency: 1,
      timeoutMs: 100,
    });
    const write = adapter.write(targetRecord("record-1"), 1);
    const timedOut = expect(write).rejects.toBeInstanceOf(TargetTimeoutError);

    await jest.advanceTimersByTimeAsync(100);

    await timedOut;
    expect(adapter.active).toBe(0);
  });

  test("preserves an injected target outage classification", async () => {
    const adapter = new TargetAdapter(new ImmediateWriter(), {
      concurrency: 1,
      timeoutMs: 100,
      faultPolicy: new UnavailablePolicy(),
    });

    await expect(
      adapter.write(targetRecord("record-1"), 2),
    ).rejects.toBeInstanceOf(TargetUnavailableError);
  });

  test("a record-specific failure does not prevent a later write", async () => {
    const adapter = new TargetAdapter(new ImmediateWriter(), {
      concurrency: 1,
      timeoutMs: 100,
      faultPolicy: new OneRecordFailurePolicy(),
    });

    await expect(adapter.write(targetRecord("fail"), 1)).rejects.toBeInstanceOf(
      TargetUnavailableError,
    );
    await expect(adapter.write(targetRecord("succeed"), 1)).resolves.toBe(
      "applied",
    );
  });
});

class ControlledWriter implements TargetWriter {
  active = 0;
  highWater = 0;
  private readonly releases: Array<() => void> = [];

  async apply(): Promise<ApplyOutcome> {
    this.active += 1;
    this.highWater = Math.max(this.highWater, this.active);
    await new Promise<void>((resolve) => this.releases.push(resolve));
    this.active -= 1;
    return "applied";
  }

  releaseNext(): void {
    const release = this.releases.shift();
    if (release === undefined) {
      throw new Error("no target write is waiting");
    }
    release();
  }
}

class NeverWriter implements TargetWriter {
  apply(): Promise<ApplyOutcome> {
    return new Promise(() => undefined);
  }
}

class ImmediateWriter implements TargetWriter {
  apply(): ApplyOutcome {
    return "applied";
  }
}

class UnavailablePolicy implements FaultPolicy {
  beforeWrite(): never {
    throw new TargetUnavailableError("simulated outage");
  }
}

class OneRecordFailurePolicy implements FaultPolicy {
  beforeWrite(record: TargetRecord): void {
    if (record.recordId === "fail") {
      throw new TargetUnavailableError("record-specific failure");
    }
  }
}

function targetRecord(recordId: string): TargetRecord {
  return {
    recordId,
    recordVersion: 1,
    name: "Ada",
    email: "ada@example.com",
    status: "active",
    contentHash: "hash",
    lastEventId: `event-${recordId}`,
    updatedAt: "2026-10-10T12:00:00.000Z",
  };
}

async function settleMicrotasks(): Promise<void> {
  for (let index = 0; index < 5; index += 1) {
    await Promise.resolve();
  }
}
