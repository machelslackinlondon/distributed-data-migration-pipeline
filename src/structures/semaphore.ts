export class Semaphore {
  private readonly limit: number;
  private readonly waiters: Array<(release: () => void) => void> = [];
  private activeCount = 0;

  constructor(limit: number) {
    if (!Number.isSafeInteger(limit) || limit <= 0) {
      throw new Error("semaphore limit must be a positive integer");
    }
    this.limit = limit;
  }

  get active(): number {
    return this.activeCount;
  }

  get pending(): number {
    return this.waiters.length;
  }

  acquire(): Promise<() => void> {
    if (this.activeCount < this.limit) {
      this.activeCount += 1;
      return Promise.resolve(this.createRelease());
    }

    return new Promise<() => void>((resolve) => {
      this.waiters.push(resolve);
    });
  }

  private createRelease(): () => void {
    let released = false;
    return () => {
      if (released) {
        return;
      }
      released = true;

      const waiter = this.waiters.shift();
      if (waiter === undefined) {
        this.activeCount -= 1;
        return;
      }
      waiter(this.createRelease());
    };
  }
}
