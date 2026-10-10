interface HeapNode<T> {
  value: T;
  order: number;
}

export class MinHeap<T> {
  private readonly values: Array<HeapNode<T>> = [];
  private readonly comparator: (left: T, right: T) => number;
  private nextOrder = 0;

  constructor(comparator: (left: T, right: T) => number) {
    this.comparator = comparator;
  }

  get size(): number {
    return this.values.length;
  }

  peek(): T | undefined {
    return this.values[0]?.value;
  }

  push(value: T): void {
    this.values.push({ value, order: this.nextOrder });
    this.nextOrder += 1;
    this.bubbleUp(this.values.length - 1);
  }

  pop(): T | undefined {
    const root = this.values[0];
    const last = this.values.pop();
    if (root === undefined || last === undefined) {
      return undefined;
    }

    if (this.values.length > 0) {
      this.values[0] = last;
      this.bubbleDown(0);
    }
    return root.value;
  }

  private compare(left: HeapNode<T>, right: HeapNode<T>): number {
    const result = this.comparator(left.value, right.value);
    return result === 0 ? left.order - right.order : result;
  }

  private bubbleUp(start: number): void {
    let child = start;
    while (child > 0) {
      const parent = Math.floor((child - 1) / 2);
      const childValue = this.values[child];
      const parentValue = this.values[parent];
      if (
        childValue === undefined ||
        parentValue === undefined ||
        this.compare(childValue, parentValue) >= 0
      ) {
        return;
      }
      this.values[parent] = childValue;
      this.values[child] = parentValue;
      child = parent;
    }
  }

  private bubbleDown(start: number): void {
    let parent = start;
    while (true) {
      const left = parent * 2 + 1;
      const right = left + 1;
      let smallest = parent;

      if (this.isSmaller(left, smallest)) {
        smallest = left;
      }
      if (this.isSmaller(right, smallest)) {
        smallest = right;
      }
      if (smallest === parent) {
        return;
      }

      const parentValue = this.values[parent];
      const smallestValue = this.values[smallest];
      if (parentValue === undefined || smallestValue === undefined) {
        return;
      }
      this.values[parent] = smallestValue;
      this.values[smallest] = parentValue;
      parent = smallest;
    }
  }

  private isSmaller(index: number, other: number): boolean {
    const value = this.values[index];
    const otherValue = this.values[other];
    return (
      value !== undefined &&
      otherValue !== undefined &&
      this.compare(value, otherValue) < 0
    );
  }
}
