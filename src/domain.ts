export type EventType = "upsert";

export interface SourcePayload {
  name: string;
  email: string;
  status: "active" | "inactive";
}

export interface SourceEventInput {
  eventId: string;
  recordId: string;
  recordVersion: number;
  type: EventType;
  payload: SourcePayload;
}

export interface MigrationEvent extends SourceEventInput {
  position: number;
  createdAt: string;
}

export interface TargetRecord extends SourcePayload {
  recordId: string;
  recordVersion: number;
  contentHash: string;
  lastEventId: string;
  updatedAt: string;
}

export interface WorkItem {
  event: MigrationEvent;
  attempt: number;
}

export type ApplyOutcome = "applied" | "duplicate" | "stale";
export type FailureKind = "malformed" | "non_retryable" | "retry_exhausted";

export interface DeadLetter {
  position: number;
  eventId: string;
  recordId: string;
  eventJson: string;
  failureKind: FailureKind;
  failureMessage: string;
  attempts: number;
  failedAt: string;
}

export interface PipelineStatus {
  state: "stopped" | "running" | "degraded";
  queueDepth: number;
  activeWorkers: number;
  scheduledRetries: number;
  checkpointPosition: number;
  sourcePosition: number;
  checkpointLag: number;
}

export interface ReconciliationMismatch {
  recordId: string;
  kind: "missing_target" | "unexpected_target" | "version" | "hash";
  sourceVersion?: number;
  targetVersion?: number;
}

export interface ReconciliationReport {
  verified: boolean;
  sourceCount: number;
  targetCount: number;
  deadLetterCount: number;
  missingTargetCount: number;
  unexpectedTargetCount: number;
  versionMismatchCount: number;
  hashMismatchCount: number;
  mismatchSamples: ReconciliationMismatch[];
}
