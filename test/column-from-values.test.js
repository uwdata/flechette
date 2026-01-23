import { describe, it, expect } from "vitest";
import { columnFromValues, int32, nullType, utf8 } from '../src/index.js';

function test(values, type, options) {
  compare(values, values, type, options);
  compare(values, callback => values.forEach(callback), type, options);
}

function compare(array, values, type, options) {
  const col = columnFromValues(values, type, options);
  if (type) expect(col.type).toStrictEqual(type);
  expect(col.length).toBe(array.length);
  expect(Array.from(col)).toStrictEqual(array);
}

describe('columnFromValues', () => {
  it('builds null columns', () => {
    test([null, null, null], nullType());
    test([null, null, null, null, null], nullType(), { maxBatchRows: 2 });
  });

  it('builds non-null columns', () => {
    test([1, 2, 3]);
    test([1, 2, 3], int32());
    test([1, 2, 3, 4, 5], int32(), { maxBatchRows: 2 });
    test(['a', 'b', 'c']);
    test(['a', 'b', 'c'], utf8());
    test(['a', 'b', 'c', 'd', 'e'], utf8(), { maxBatchRows: 2 });

    // create column using only values with odd-numbered indices
    const values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    const filter = values.filter((_, i) => i % 2);
    compare(filter, callback => {
      values.forEach((v, i) => { if (i % 2) callback(v); })
    });
  });
});
