import { describe, expect, test } from "@jest/globals";

import { Semaphore } from "../../src/structures/semaphore.js";

describe("Semaphore", () => {
  test("never grants more than its configured permits", async () => {
    const semaphore = new Semaphore(2);
    const releaseFirst = await semaphore.acquire();
    const releaseSecond = await semaphore.acquire();
    let thirdGranted = false;

    const third = semaphore.acquire().then((release) => {
      thirdGranted = true;
      return release;
    });
    await Promise.resolve();

    expect(semaphore.active).toBe(2);
    expect(semaphore.pending).toBe(1);
    expect(thirdGranted).toBe(false);

    releaseFirst();
    const releaseThird = await third;
    expect(thirdGranted).toBe(true);
    expect(semaphore.active).toBe(2);
    expect(semaphore.pending).toBe(0);

    releaseSecond();
    releaseThird();
    expect(semaphore.active).toBe(0);
  });

  test("a permit release is idempotent", async () => {
    const semaphore = new Semaphore(1);
    const release = await semaphore.acquire();

    release();
    release();

    expect(semaphore.active).toBe(0);
  });

  test("rejects a non-positive limit", () => {
    expect(() => new Semaphore(0)).toThrow(
      "semaphore limit must be a positive integer",
    );
  });
});
