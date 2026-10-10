import Database from "better-sqlite3";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";

const schema = `
  CREATE TABLE IF NOT EXISTS source_records (
    record_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL CHECK (version > 0),
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'inactive')),
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS source_events (
    position INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    record_id TEXT NOT NULL,
    record_version INTEGER NOT NULL CHECK (record_version > 0),
    event_type TEXT NOT NULL CHECK (event_type = 'upsert'),
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
  );

  CREATE INDEX IF NOT EXISTS source_events_record_version_idx
    ON source_events(record_id, record_version);

  CREATE TABLE IF NOT EXISTS target_records (
    record_id TEXT PRIMARY KEY,
    record_version INTEGER NOT NULL CHECK (record_version > 0),
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'inactive')),
    content_hash TEXT NOT NULL,
    last_event_id TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS checkpoints (
    pipeline_key TEXT PRIMARY KEY,
    position INTEGER NOT NULL CHECK (position >= 0),
    updated_at TEXT NOT NULL
  );

  INSERT OR IGNORE INTO checkpoints (pipeline_key, position, updated_at)
    VALUES ('migration', 0, '1970-01-01T00:00:00.000Z');

  CREATE TABLE IF NOT EXISTS dead_letters (
    position INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL,
    record_id TEXT NOT NULL,
    event_json TEXT NOT NULL,
    failure_kind TEXT NOT NULL,
    failure_message TEXT NOT NULL,
    attempts INTEGER NOT NULL CHECK (attempts > 0),
    failed_at TEXT NOT NULL
  );
`;

export function openDatabase(path: string): Database.Database {
  if (path !== ":memory:") {
    mkdirSync(dirname(path), { recursive: true });
  }

  const database = new Database(path);
  database.pragma("journal_mode = WAL");
  database.pragma("synchronous = FULL");
  database.pragma("foreign_keys = ON");
  database.pragma("busy_timeout = 5000");
  database.exec(schema);
  return database;
}
