import { afterEach, beforeEach, describe, expect, test } from "@jest/globals";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { DeadLetter, TargetRecord } from "../../src/domain.js";
import { openDatabase } from "../../src/storage/database.js";
import {
  CheckpointRepository,
  DlqRepository,
  DuplicateEventError,
  SourceRepository,
  TargetRepository,
} from "../../src/storage/repositories.js";

describe("SQLite repositories", () => {
  let directory: string;
  let databasePath: string;
  let database: ReturnType<typeof openDatabase>;

  beforeEach(() => {
    directory = mkdtempSync(join(tmpdir(), "migration-storage-"));
    databasePath = join(directory, "migration.sqlite");
    database = openDatabase(databasePath);
  });

  afterEach(() => {
    database.close();
    rmSync(directory, { recursive: true, force: true });
  });

  test("appends monotonic stream positions and retains the newest source version", () => {
    const source = new SourceRepository(database);

    const first = source.append({
      eventId: "event-1",
      recordId: "record-1",
      recordVersion: 2,
      type: "upsert",
      payload: { name: "Ada", email: "ADA@example.com", status: "active" },
    });
    const second = source.append({
      eventId: "event-2",
      recordId: "record-1",
      recordVersion: 1,
      type: "upsert",
      payload: {
        name: "Old Ada",
        email: "old@example.com",
        status: "inactive",
      },
    });

    expect([first.position, second.position]).toEqual([1, 2]);
    expect(source.maxPosition()).toBe(2);
    expect(source.eventsAfter(0, 10).map((event) => event.eventId)).toEqual([
      "event-1",
      "event-2",
    ]);
    expect(source.getRecord("record-1")).toMatchObject({
      version: 2,
      name: "Ada",
    });
  });

  test("rejects a duplicate event ID without changing source state", () => {
    const source = new SourceRepository(database);
    const event = {
      eventId: "event-1",
      recordId: "record-1",
      recordVersion: 1,
      type: "upsert" as const,
      payload: {
        name: "Ada",
        email: "ada@example.com",
        status: "active" as const,
      },
    };
    source.append(event);

    expect(() => source.append(event)).toThrow(DuplicateEventError);
    expect(source.maxPosition()).toBe(1);
    expect(source.countRecords()).toBe(1);
  });

  test("applies only newer target versions and identifies duplicate replay", () => {
    const target = new TargetRepository(database);
    const versionTwo = targetRecord({ version: 2, eventId: "event-2" });

    expect(target.apply(versionTwo)).toBe("applied");
    expect(target.apply(targetRecord({ version: 1, eventId: "event-1" }))).toBe(
      "stale",
    );
    expect(target.apply(versionTwo)).toBe("duplicate");
    expect(target.getRecord("record-1")).toEqual(versionTwo);
  });

  test("advances a checkpoint only from the expected durable position", () => {
    const checkpoints = new CheckpointRepository(database);

    expect(checkpoints.get()).toBe(0);
    expect(checkpoints.advance(0, 3)).toBe(true);
    expect(checkpoints.advance(0, 4)).toBe(false);
    expect(checkpoints.get()).toBe(3);
  });

  test("persists dead letters with failure metadata", () => {
    const dlq = new DlqRepository(database);
    const entry: DeadLetter = {
      position: 4,
      eventId: "event-4",
      recordId: "record-4",
      eventJson: '{"eventId":"event-4"}',
      failureKind: "retry_exhausted",
      failureMessage: "target unavailable",
      attempts: 5,
      failedAt: "2026-10-10T12:00:00.000Z",
    };

    dlq.add(entry);

    expect(dlq.count()).toBe(1);
    expect(dlq.list()).toEqual([entry]);
  });

  test("retains events, targets, checkpoint, and DLQ across restart", () => {
    const source = new SourceRepository(database);
    const target = new TargetRepository(database);
    const checkpoints = new CheckpointRepository(database);
    const dlq = new DlqRepository(database);
    source.append({
      eventId: "event-1",
      recordId: "record-1",
      recordVersion: 1,
      type: "upsert",
      payload: { name: "Ada", email: "ada@example.com", status: "active" },
    });
    target.apply(targetRecord({ version: 1, eventId: "event-1" }));
    checkpoints.advance(0, 1);
    dlq.add({
      position: 2,
      eventId: "event-2",
      recordId: "record-2",
      eventJson: "{}",
      failureKind: "malformed",
      failureMessage: "bad payload",
      attempts: 1,
      failedAt: "2026-10-10T12:00:00.000Z",
    });
    database.close();

    database = openDatabase(databasePath);

    expect(new SourceRepository(database).maxPosition()).toBe(1);
    expect(new TargetRepository(database).countRecords()).toBe(1);
    expect(new CheckpointRepository(database).get()).toBe(1);
    expect(new DlqRepository(database).count()).toBe(1);
  });
});

function targetRecord(input: {
  version: number;
  eventId: string;
}): TargetRecord {
  return {
    recordId: "record-1",
    recordVersion: input.version,
    name: "Ada",
    email: "ada@example.com",
    status: "active",
    contentHash: `hash-${input.version}`,
    lastEventId: input.eventId,
    updatedAt: "2026-10-10T12:00:00.000Z",
  };
}
