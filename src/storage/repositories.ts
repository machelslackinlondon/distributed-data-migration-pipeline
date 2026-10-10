import type Database from "better-sqlite3";

import type {
  ApplyOutcome,
  DeadLetter,
  FailureKind,
  MigrationEvent,
  SourceEventInput,
  SourcePayload,
  TargetRecord,
} from "../domain.js";

export class DuplicateEventError extends Error {
  constructor(eventId: string) {
    super(`event ID already exists: ${eventId}`);
    this.name = "DuplicateEventError";
  }
}

export interface StoredSourceRecord extends SourcePayload {
  recordId: string;
  version: number;
  updatedAt: string;
}

interface SourceEventRow {
  position: number;
  event_id: string;
  record_id: string;
  record_version: number;
  event_type: "upsert";
  payload_json: string;
  created_at: string;
}

interface SourceRecordRow {
  record_id: string;
  version: number;
  name: string;
  email: string;
  status: "active" | "inactive";
  updated_at: string;
}

interface TargetRecordRow {
  record_id: string;
  record_version: number;
  name: string;
  email: string;
  status: "active" | "inactive";
  content_hash: string;
  last_event_id: string;
  updated_at: string;
}

interface DeadLetterRow {
  position: number;
  event_id: string;
  record_id: string;
  event_json: string;
  failure_kind: FailureKind;
  failure_message: string;
  attempts: number;
  failed_at: string;
}

export class SourceRepository {
  private readonly database: Database.Database;

  constructor(database: Database.Database) {
    this.database = database;
  }

  append(input: SourceEventInput): MigrationEvent {
    const createdAt = new Date().toISOString();
    const appendTransaction = this.database.transaction(() => {
      const eventResult = this.database
        .prepare(
          `INSERT INTO source_events
            (event_id, record_id, record_version, event_type, payload_json, created_at)
           VALUES
            (@eventId, @recordId, @recordVersion, @type, @payloadJson, @createdAt)`,
        )
        .run({
          eventId: input.eventId,
          recordId: input.recordId,
          recordVersion: input.recordVersion,
          type: input.type,
          payloadJson: JSON.stringify(input.payload),
          createdAt,
        });

      this.database
        .prepare(
          `INSERT INTO source_records
            (record_id, version, name, email, status, updated_at)
           VALUES
            (@recordId, @version, @name, @email, @status, @updatedAt)
           ON CONFLICT(record_id) DO UPDATE SET
             version = excluded.version,
             name = excluded.name,
             email = excluded.email,
             status = excluded.status,
             updated_at = excluded.updated_at
           WHERE excluded.version > source_records.version`,
        )
        .run({
          recordId: input.recordId,
          version: input.recordVersion,
          name: input.payload.name,
          email: input.payload.email,
          status: input.payload.status,
          updatedAt: createdAt,
        });

      return {
        ...input,
        position: Number(eventResult.lastInsertRowid),
        createdAt,
      };
    });

    try {
      return appendTransaction();
    } catch (error) {
      if (isUniqueConstraint(error)) {
        throw new DuplicateEventError(input.eventId);
      }
      throw error;
    }
  }

  eventsAfter(position: number, limit: number): MigrationEvent[] {
    const rows = this.database
      .prepare(
        `SELECT position, event_id, record_id, record_version, event_type,
                payload_json, created_at
           FROM source_events
          WHERE position > ?
          ORDER BY position
          LIMIT ?`,
      )
      .all(position, limit) as SourceEventRow[];
    return rows.map(toMigrationEvent);
  }

  getRecord(recordId: string): StoredSourceRecord | undefined {
    const row = this.database
      .prepare(
        `SELECT record_id, version, name, email, status, updated_at
           FROM source_records WHERE record_id = ?`,
      )
      .get(recordId) as SourceRecordRow | undefined;
    return row === undefined ? undefined : toSourceRecord(row);
  }

  recordsPage(afterId: string, limit: number): StoredSourceRecord[] {
    const rows = this.database
      .prepare(
        `SELECT record_id, version, name, email, status, updated_at
           FROM source_records
          WHERE record_id > ?
          ORDER BY record_id
          LIMIT ?`,
      )
      .all(afterId, limit) as SourceRecordRow[];
    return rows.map(toSourceRecord);
  }

  maxPosition(): number {
    const row = this.database
      .prepare(
        "SELECT COALESCE(MAX(position), 0) AS position FROM source_events",
      )
      .get() as { position: number };
    return row.position;
  }

  countRecords(): number {
    const row = this.database
      .prepare("SELECT COUNT(*) AS count FROM source_records")
      .get() as { count: number };
    return row.count;
  }
}

export class TargetRepository {
  private readonly database: Database.Database;

  constructor(database: Database.Database) {
    this.database = database;
  }

  apply(record: TargetRecord): ApplyOutcome {
    const applyTransaction = this.database.transaction((): ApplyOutcome => {
      const existing = this.database
        .prepare(
          "SELECT record_version, last_event_id FROM target_records WHERE record_id = ?",
        )
        .get(record.recordId) as
        | { record_version: number; last_event_id: string }
        | undefined;

      if (existing !== undefined) {
        if (
          existing.record_version === record.recordVersion &&
          existing.last_event_id === record.lastEventId
        ) {
          return "duplicate";
        }
        if (existing.record_version >= record.recordVersion) {
          return "stale";
        }
      }

      this.database
        .prepare(
          `INSERT INTO target_records
            (record_id, record_version, name, email, status, content_hash,
             last_event_id, updated_at)
           VALUES
            (@recordId, @recordVersion, @name, @email, @status, @contentHash,
             @lastEventId, @updatedAt)
           ON CONFLICT(record_id) DO UPDATE SET
             record_version = excluded.record_version,
             name = excluded.name,
             email = excluded.email,
             status = excluded.status,
             content_hash = excluded.content_hash,
             last_event_id = excluded.last_event_id,
             updated_at = excluded.updated_at
           WHERE excluded.record_version > target_records.record_version`,
        )
        .run(record);
      return "applied";
    });
    return applyTransaction();
  }

  getRecord(recordId: string): TargetRecord | undefined {
    const row = this.database
      .prepare(
        `SELECT record_id, record_version, name, email, status, content_hash,
                last_event_id, updated_at
           FROM target_records WHERE record_id = ?`,
      )
      .get(recordId) as TargetRecordRow | undefined;
    return row === undefined ? undefined : toTargetRecord(row);
  }

  recordsPage(afterId: string, limit: number): TargetRecord[] {
    const rows = this.database
      .prepare(
        `SELECT record_id, record_version, name, email, status, content_hash,
                last_event_id, updated_at
           FROM target_records
          WHERE record_id > ?
          ORDER BY record_id
          LIMIT ?`,
      )
      .all(afterId, limit) as TargetRecordRow[];
    return rows.map(toTargetRecord);
  }

  countRecords(): number {
    const row = this.database
      .prepare("SELECT COUNT(*) AS count FROM target_records")
      .get() as { count: number };
    return row.count;
  }
}

export class CheckpointRepository {
  private readonly database: Database.Database;

  constructor(database: Database.Database) {
    this.database = database;
  }

  get(): number {
    const row = this.database
      .prepare(
        "SELECT position FROM checkpoints WHERE pipeline_key = 'migration'",
      )
      .get() as { position: number };
    return row.position;
  }

  advance(expectedCurrent: number, next: number): boolean {
    const result = this.database
      .prepare(
        `UPDATE checkpoints
            SET position = @next, updated_at = @updatedAt
          WHERE pipeline_key = 'migration'
            AND position = @expectedCurrent
            AND @next > position`,
      )
      .run({ expectedCurrent, next, updatedAt: new Date().toISOString() });
    return result.changes === 1;
  }
}

export class DlqRepository {
  private readonly database: Database.Database;

  constructor(database: Database.Database) {
    this.database = database;
  }

  add(entry: DeadLetter): void {
    this.database
      .prepare(
        `INSERT INTO dead_letters
          (position, event_id, record_id, event_json, failure_kind,
           failure_message, attempts, failed_at)
         VALUES
          (@position, @eventId, @recordId, @eventJson, @failureKind,
           @failureMessage, @attempts, @failedAt)`,
      )
      .run(entry);
  }

  count(): number {
    const row = this.database
      .prepare("SELECT COUNT(*) AS count FROM dead_letters")
      .get() as { count: number };
    return row.count;
  }

  list(): DeadLetter[] {
    const rows = this.database
      .prepare(
        `SELECT position, event_id, record_id, event_json, failure_kind,
                failure_message, attempts, failed_at
           FROM dead_letters ORDER BY position`,
      )
      .all() as DeadLetterRow[];
    return rows.map((row) => ({
      position: row.position,
      eventId: row.event_id,
      recordId: row.record_id,
      eventJson: row.event_json,
      failureKind: row.failure_kind,
      failureMessage: row.failure_message,
      attempts: row.attempts,
      failedAt: row.failed_at,
    }));
  }
}

function toMigrationEvent(row: SourceEventRow): MigrationEvent {
  return {
    position: row.position,
    eventId: row.event_id,
    recordId: row.record_id,
    recordVersion: row.record_version,
    type: row.event_type,
    payload: JSON.parse(row.payload_json) as SourcePayload,
    createdAt: row.created_at,
  };
}

function toSourceRecord(row: SourceRecordRow): StoredSourceRecord {
  return {
    recordId: row.record_id,
    version: row.version,
    name: row.name,
    email: row.email,
    status: row.status,
    updatedAt: row.updated_at,
  };
}

function toTargetRecord(row: TargetRecordRow): TargetRecord {
  return {
    recordId: row.record_id,
    recordVersion: row.record_version,
    name: row.name,
    email: row.email,
    status: row.status,
    contentHash: row.content_hash,
    lastEventId: row.last_event_id,
    updatedAt: row.updated_at,
  };
}

function isUniqueConstraint(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    error.code === "SQLITE_CONSTRAINT_UNIQUE"
  );
}
