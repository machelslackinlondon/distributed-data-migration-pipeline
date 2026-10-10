export interface CheckpointStore {
  get(): number;
  advance(expectedCurrent: number, next: number): boolean | Promise<boolean>;
}

export class CheckpointPersistenceError extends Error {
  constructor(cause?: unknown) {
    super("checkpoint persistence failed", { cause });
    this.name = "CheckpointPersistenceError";
  }
}

export class CheckpointTracker {
  private readonly store: CheckpointStore;
  private readonly terminal = new Set<number>();
  private current: number;
  private tail: Promise<void> = Promise.resolve();

  constructor(store: CheckpointStore) {
    this.store = store;
    this.current = store.get();
  }

  get position(): number {
    return this.current;
  }

  markTerminal(position: number): Promise<number> {
    if (!Number.isSafeInteger(position) || position <= 0) {
      return Promise.reject(new Error("terminal position must be positive"));
    }

    const result = this.tail.then(() => this.markInternal(position));
    this.tail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private async markInternal(position: number): Promise<number> {
    if (position <= this.current) {
      return this.current;
    }
    this.terminal.add(position);

    let candidate = this.current;
    while (this.terminal.has(candidate + 1)) {
      candidate += 1;
    }
    if (candidate === this.current) {
      return this.current;
    }

    let persisted: boolean;
    try {
      persisted = await this.store.advance(this.current, candidate);
    } catch (error) {
      throw new CheckpointPersistenceError(error);
    }
    if (!persisted) {
      throw new CheckpointPersistenceError();
    }

    for (
      let completed = this.current + 1;
      completed <= candidate;
      completed += 1
    ) {
      this.terminal.delete(completed);
    }
    this.current = candidate;
    return this.current;
  }
}
