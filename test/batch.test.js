import { describe, it, expect } from "vitest";
import { DirectBatch } from '../src/batch.js';

describe('DirectBatch', () => {
  it('trims the values array', () => {
    const b = new DirectBatch({
      length: 4,
      nullCount: 0,
      values: Int32Array.of(1, 2, 3, 4, 5, 6, 7, 8)
    });
    expect(b.length).toBe(4);
    expect([...b].length).toBe(4);
    expect(b.value(4)).toBeUndefined();
  });
});
