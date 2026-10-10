import { afterEach, beforeEach, describe, expect, test } from "@jest/globals";

import { transform } from "../src/pipeline/transformer.js";
import { reconcile } from "../src/reconciliation.js";
import { openDatabase } from "../src/storage/database.js";
import {
  DlqRepository,
  SourceRepository,
  TargetRepository,
} from "../src/storage/repositories.js";

describe("reconcile", () => {
  let database: ReturnType<typeof openDatabase>;
  let source: SourceRepository;
  let target: TargetRepository;
  let dlq: DlqRepository;

  beforeEach(() => {
    database = openDatabase(":memory:");
    source = new SourceRepository(database);
    target = new TargetRepository(database);
    dlq = new DlqRepository(database);
  });

  afterEach(() => {
    database.close();
  });

  test("verifies matching records across multiple pages", () => {
    for (let index = 1; index <= 5; index += 1) {
      const event = appendSource(index);
      target.apply(transform(event));
    }

    expect(reconcile(source, target, dlq, 2)).toEqual({
      verified: true,
      sourceCount: 5,
      targetCount: 5,
      deadLetterCount: 0,
      missingTargetCount: 0,
      unexpectedTargetCount: 0,
      versionMismatchCount: 0,
      hashMismatchCount: 0,
      mismatchSamples: [],
    });
  });

  test("reports missing, unexpected, version, and hash mismatches", () => {
    const missing = appendSource(1);
    const version = appendSource(2);
    const hash = appendSource(3);
    target.apply(
      transform({ ...version, recordVersion: version.recordVersion - 1 }),
    );
    target.apply({ ...transform(hash), contentHash: "incorrect" });
    target.apply({
      ...transform(missing),
      recordId: "record-9",
      lastEventId: "event-9",
    });

    const report = reconcile(source, target, dlq, 2);

    expect(report).toMatchObject({
      verified: false,
      sourceCount: 3,
      targetCount: 3,
      missingTargetCount: 1,
      unexpectedTargetCount: 1,
      versionMismatchCount: 1,
      hashMismatchCount: 1,
    });
    expect(report.mismatchSamples.map((item) => item.kind).sort()).toEqual([
      "hash",
      "missing_target",
      "unexpected_target",
      "version",
    ]);
  });

  test("does not call a migration verified while the DLQ is non-empty", () => {
    const event = appendSource(1);
    target.apply(transform(event));
    dlq.add({
      position: 2,
      eventId: "failed-event",
      recordId: "failed-record",
      eventJson: "{}",
      failureKind: "retry_exhausted",
      failureMessage: "target unavailable",
      attempts: 3,
      failedAt: "2026-10-10T12:00:00.000Z",
    });

    expect(reconcile(source, target, dlq, 2)).toMatchObject({
      verified: false,
      deadLetterCount: 1,
      missingTargetCount: 0,
      unexpectedTargetCount: 0,
    });
  });

  function appendSource(index: number) {
    return source.append({
      eventId: `event-${index}`,
      recordId: `record-${index}`,
      recordVersion: 2,
      type: "upsert",
      payload: {
        name: ` Record ${index} `,
        email: `RECORD-${index}@EXAMPLE.COM`,
        status: "active",
      },
    });
  }
});
