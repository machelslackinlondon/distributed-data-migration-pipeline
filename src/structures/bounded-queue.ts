export class QueueClosedError extends Error {
  constructor() {
    super("queue is closed");
    this.name = "QueueClosedError";
  }
}

interface ConsumerWaiter<T> {
  resolve: (item: T | undefined) => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
}

interface ProducerWaiter<T> {
  item: T;
  resolve: () => void;
  reject: (error: Error) => void;
}

export class BoundedQueue<T> {
  readonly capacity: number;
  private readonly buffer: Array<T | undefined>;
  private readonly consumers: Array<ConsumerWaiter<T>> = [];
  private readonly producers: Array<ProducerWaiter<T>> = [];
  private head = 0;
  private tail = 0;
  private length = 0;
  private closed = false;

  constructor(capacity: number) {
    if (!Number.isSafeInteger(capacity) || capacity <= 0) {
      throw new Error("queue capacity must be a positive integer");
    }
    this.capacity = capacity;
    this.buffer = new Array<T | undefined>(capacity);
  }

  get size(): number {
    return this.length;
  }

  get isFull(): boolean {
    return this.length === this.capacity;
  }

  tryEnqueue(item: T): boolean {
    if (this.closed) {
      throw new QueueClosedError();
    }

    const consumer = this.consumers.shift();
    if (consumer !== undefined) {
      this.resolveConsumer(consumer, item);
      return true;
    }

    if (this.isFull) {
      return false;
    }

    this.push(item);
    return true;
  }

  enqueue(item: T): Promise<void> {
    if (this.tryEnqueue(item)) {
      return Promise.resolve();
    }

    return new Promise<void>((resolve, reject) => {
      this.producers.push({ item, resolve, reject });
    });
  }

  dequeue(signal?: AbortSignal): Promise<T | undefined> {
    if (this.length > 0) {
      const item = this.shift();
      this.promoteProducer();
      this.resolveClosedConsumersWhenDrained();
      return Promise.resolve(item);
    }

    if (this.closed) {
      return Promise.resolve(undefined);
    }

    if (signal?.aborted === true) {
      return Promise.reject(abortError(signal));
    }

    return new Promise<T | undefined>((resolve, reject) => {
      const consumer: ConsumerWaiter<T> = { resolve, reject };
      if (signal !== undefined) {
        const onAbort = (): void => {
          const index = this.consumers.indexOf(consumer);
          if (index >= 0) {
            this.consumers.splice(index, 1);
          }
          reject(abortError(signal));
        };
        consumer.signal = signal;
        consumer.onAbort = onAbort;
        signal.addEventListener("abort", onAbort, { once: true });
      }
      this.consumers.push(consumer);
    });
  }

  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;

    const error = new QueueClosedError();
    for (const producer of this.producers.splice(0)) {
      producer.reject(error);
    }
    this.resolveClosedConsumersWhenDrained();
  }

  private push(item: T): void {
    this.buffer[this.tail] = item;
    this.tail = (this.tail + 1) % this.capacity;
    this.length += 1;
  }

  private shift(): T {
    const item = this.buffer[this.head];
    this.buffer[this.head] = undefined;
    this.head = (this.head + 1) % this.capacity;
    this.length -= 1;
    return item as T;
  }

  private promoteProducer(): void {
    const producer = this.producers.shift();
    if (producer === undefined) {
      return;
    }
    this.push(producer.item);
    producer.resolve();
  }

  private resolveClosedConsumersWhenDrained(): void {
    if (!this.closed || this.length > 0) {
      return;
    }
    for (const consumer of this.consumers.splice(0)) {
      this.resolveConsumer(consumer, undefined);
    }
  }

  private resolveConsumer(
    consumer: ConsumerWaiter<T>,
    item: T | undefined,
  ): void {
    if (consumer.signal !== undefined && consumer.onAbort !== undefined) {
      consumer.signal.removeEventListener("abort", consumer.onAbort);
    }
    consumer.resolve(item);
  }
}
import { abortError } from "../abort.js";
