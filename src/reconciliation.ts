import type {
  ReconciliationMismatch,
  ReconciliationReport,
  TargetRecord,
} from "./domain.js";
import { canonicalHash } from "./pipeline/transformer.js";
import type {
  DlqRepository,
  SourceRepository,
  StoredSourceRecord,
  TargetRepository,
} from "./storage/repositories.js";

const defaultSampleLimit = 100;

export function reconcile(
  source: SourceRepository,
  target: TargetRepository,
  dlq: DlqRepository,
  pageSize: number,
  sampleLimit = defaultSampleLimit,
): ReconciliationReport {
  if (!Number.isSafeInteger(pageSize) || pageSize <= 0) {
    throw new Error("reconciliation page size must be a positive integer");
  }

  const sourceCursor = new RecordCursor<StoredSourceRecord>((afterId) =>
    source.recordsPage(afterId, pageSize),
  );
  const targetCursor = new RecordCursor<TargetRecord>((afterId) =>
    target.recordsPage(afterId, pageSize),
  );
  const samples: ReconciliationMismatch[] = [];
  let missingTargetCount = 0;
  let unexpectedTargetCount = 0;
  let versionMismatchCount = 0;
  let hashMismatchCount = 0;
  let sourceRecord = sourceCursor.next();
  let targetRecord = targetCursor.next();

  while (sourceRecord !== undefined || targetRecord !== undefined) {
    if (
      sourceRecord !== undefined &&
      (targetRecord === undefined ||
        sourceRecord.recordId < targetRecord.recordId)
    ) {
      missingTargetCount += 1;
      addSample(samples, sampleLimit, {
        recordId: sourceRecord.recordId,
        kind: "missing_target",
        sourceVersion: sourceRecord.version,
      });
      sourceRecord = sourceCursor.next();
      continue;
    }
    if (
      targetRecord !== undefined &&
      (sourceRecord === undefined ||
        targetRecord.recordId < sourceRecord.recordId)
    ) {
      unexpectedTargetCount += 1;
      addSample(samples, sampleLimit, {
        recordId: targetRecord.recordId,
        kind: "unexpected_target",
        targetVersion: targetRecord.recordVersion,
      });
      targetRecord = targetCursor.next();
      continue;
    }
    if (sourceRecord === undefined || targetRecord === undefined) {
      break;
    }

    if (sourceRecord.version !== targetRecord.recordVersion) {
      versionMismatchCount += 1;
      addSample(samples, sampleLimit, {
        recordId: sourceRecord.recordId,
        kind: "version",
        sourceVersion: sourceRecord.version,
        targetVersion: targetRecord.recordVersion,
      });
    }
    const sourceHash = canonicalHash({
      name: sourceRecord.name.trim(),
      email: sourceRecord.email.trim().toLowerCase(),
      status: sourceRecord.status,
    });
    if (sourceHash !== targetRecord.contentHash) {
      hashMismatchCount += 1;
      addSample(samples, sampleLimit, {
        recordId: sourceRecord.recordId,
        kind: "hash",
        sourceVersion: sourceRecord.version,
        targetVersion: targetRecord.recordVersion,
      });
    }
    sourceRecord = sourceCursor.next();
    targetRecord = targetCursor.next();
  }

  const deadLetterCount = dlq.count();
  return {
    verified:
      deadLetterCount === 0 &&
      missingTargetCount === 0 &&
      unexpectedTargetCount === 0 &&
      versionMismatchCount === 0 &&
      hashMismatchCount === 0,
    sourceCount: source.countRecords(),
    targetCount: target.countRecords(),
    deadLetterCount,
    missingTargetCount,
    unexpectedTargetCount,
    versionMismatchCount,
    hashMismatchCount,
    mismatchSamples: samples,
  };
}

class RecordCursor<T extends { recordId: string }> {
  private readonly loadPage: (afterId: string) => T[];
  private page: T[] = [];
  private index = 0;
  private afterId = "";
  private exhausted = false;

  constructor(loadPage: (afterId: string) => T[]) {
    this.loadPage = loadPage;
  }

  next(): T | undefined {
    if (this.index >= this.page.length && !this.exhausted) {
      this.page = this.loadPage(this.afterId);
      this.index = 0;
      if (this.page.length === 0) {
        this.exhausted = true;
      } else {
        this.afterId = this.page.at(-1)?.recordId ?? this.afterId;
      }
    }
    const value = this.page[this.index];
    if (value !== undefined) {
      this.index += 1;
    }
    return value;
  }
}

function addSample(
  samples: ReconciliationMismatch[],
  limit: number,
  mismatch: ReconciliationMismatch,
): void {
  if (samples.length < limit) {
    samples.push(mismatch);
  }
}
