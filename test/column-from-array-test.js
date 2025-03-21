import assert from 'node:assert';
import { IntervalUnit, TimeUnit, UnionMode, binary, bool, columnFromArray, dateDay, dateMillisecond, decimal, dictionary, duration, field, fixedSizeBinary, fixedSizeList, float16, float32, float64, int16, int32, int64, int8, interval, largeBinary, largeList, largeUtf8, list, map, nullType, runEndEncoded, struct, timeMicrosecond, timeMillisecond, timeNanosecond, timeSecond, timestamp, uint16, uint32, uint64, uint8, union, utf8 } from '../src/index.js';
import { isTypedArray } from '../src/util/arrays.js';

function test(values, type, options) {
  const col = columnFromArray(values, type, options);
  if (type) assert.deepEqual(col.type, type);
  if (!type && isTypedArray(values)) {
    // check that inferred data type maintains input array type
    assert.strictEqual(col.data[0].values.constructor, values.constructor);
  }
  assert.strictEqual(col.length, values.length);
  assert.deepStrictEqual(Array.from(col), Array.from(values));
  return col;
}

describe('columnFromArray', () => {
  it('builds null columns', () => {
    test([null, null, null], nullType());
  });

  it('builds integer columns', () => {
    // without nulls
    test([1, 2, 3], uint8());
    test([1, 2, 3], uint16());
    test([1, 2, 3], uint32());
    test([1, 2, 3], uint64());
    test([1, 2, 3], int8());
    test([1, 2, 3], int16());
    test([1, 2, 3], int32());
    test([1, 2, 3], int64());

    // with nulls
    test([1, 2, null, 3], uint8());
    test([1, 2, null, 3], uint16());
    test([1, 2, null, 3], uint32());
    test([1, 2, null, 3], uint64());
    test([1, 2, null, 3], int8());
    test([1, 2, null, 3], int16());
    test([1, 2, null, 3], int32());
    test([1, 2, null, 3], int64());

    // use big int
    const opt = { useBigInt: true };
    test([1n, 2n, 3n], uint64(), opt);
    test([1n, 2n, 3n], int64(), opt);
    test([1n, 2n, null, 3n], uint64(), opt);
    test([1n, 2n, null, 3n], int64(), opt);
  });

  it('builds float columns', () => {
    // without nulls
    test([1, 2, 3], float16());
    test([1, 2, 3], float32());
    test([1, 2, 3], float64());

    // with nulls
    test([1, 2, null, 3], float16());
    test([1, 2, null, 3], float32());
    test([1, 2, null, 3], float64());
  });

  it('builds binary columns', () => {
    test([
      Uint8Array.of(255, 2, 3, 1),
      null,
      Uint8Array.of(5, 9, 128)
    ], binary());
  });

  it('builds large binary columns', () => {
    test([
      Uint8Array.of(255, 2, 3, 1),
      null,
      Uint8Array.of(5, 9, 128)
    ], largeBinary());
  });

  it('builds utf8 columns', () => {
    test(['foo', 'bar', 'baz'], utf8());
    test(['foo', 'bar', null, 'baz'], utf8());
  });

  it('builds large utf8 columns', () => {
    test(['foo', 'bar', 'baz'], largeUtf8());
    test(['foo', 'bar', null, 'baz'], largeUtf8());
  });

  it('builds bool columns', () => {
    test([true, false, true, false], bool());
    test([true, false, null, true, false], bool());
  });

  it('builds decimal columns', () => {
    test([1.1, 2.3, 3.4], decimal(18, 1, 128));
    test([-1.1, -2.3, -3.4], decimal(18, 1, 128));
    test([0.12345678987, -0.12345678987], decimal(18, 11, 128));
    test([10000.12345678987, -10000.12345678987], decimal(18, 11, 128));
    test([0.1000012345678987, -0.1000012345678987], decimal(18, 16, 128));
    test([0.12345678987654321, -0.12345678987654321], decimal(18, 18, 128));

    test([1.1, 2.3, 3.4], decimal(40, 1, 256));
    test([-1.1, -2.3, -3.4], decimal(40, 1, 256));
    test([0.12345678987, -0.12345678987], decimal(40, 11, 256));
    test([10000.12345678987, -10000.12345678987], decimal(40, 11, 256));
    test([0.1000012345678987, -0.1000012345678987], decimal(40, 16, 256));
    test([0.12345678987654321, -0.12345678987654321], decimal(40, 18, 256));

    const opt = { useDecimalInt: true };

    test([11n, 23n, 34n], decimal(18, 1, 128), opt);
    test([-11n, -23n, -34n], decimal(18, 1, 128), opt);
    test([12345678987n, -12345678987n], decimal(18, 11, 128), opt);
    test([1000012345678987n, -1000012345678987n], decimal(18, 11, 128), opt);
    test([1000012345678987n, -1000012345678987n], decimal(18, 16, 128), opt);
    test([12345678987654321n, -12345678987654321n], decimal(18, 18, 128), opt);

    test([11n, 23n, 34n], decimal(40, 1, 256), opt);
    test([-11n, -23n, -34n], decimal(40, 1, 256), opt);
    test([12345678987n, -12345678987n], decimal(40, 11, 256), opt);
    test([1000012345678987n, -1000012345678987n], decimal(40, 11, 256), opt);
    test([1000012345678987n, -1000012345678987n], decimal(40, 16, 256), opt);
    test([12345678987654321n, -12345678987654321n], decimal(40, 18, 256), opt);
    test([2n ** 156n, (-2n) ** 157n], decimal(18, 24, 256), opt);
  });

  it('builds date columns', () => {
    const dates = [
      new Date(Date.UTC(2000, 0, 1)),
      new Date(Date.UTC(1973, 3, 20)),
      new Date(Date.UTC(1989, 4, 5))
    ];
    test(dates.map(v => +v), dateDay());
    test(dates.map(v => +v), dateMillisecond());
    test(dates, dateDay(), { useDate: true });
    test(dates, dateMillisecond(), { useDate: true });
  });

  it('builds time columns', () => {
    const values = [10, 1000, 1e6, 1e8];
    test(values, timeSecond());
    test(values, timeMillisecond());
    test(values, timeMicrosecond());
    test(values, timeNanosecond());
    test(values.concat(86400000000), timeMicrosecond());
    test(values.concat(86400000000000), timeNanosecond());

    const bigints = [10n, 1000n, 1000000n, 1000000000n];
    const opt = { useBigInt: true };
    test(bigints, timeMicrosecond(), opt);
    test(bigints, timeNanosecond(), opt);
    test(bigints.concat(86400000000n), timeMicrosecond(), opt);
    test(bigints.concat(86400000000000n), timeNanosecond(), opt);
  });

  it('builds timestamp columns', () => {
    const dates = [
      new Date(Date.UTC(2000, 0, 1)),
      new Date(Date.UTC(1973, 3, 20)),
      new Date(Date.UTC(1989, 4, 5))
    ];

    // from date objects
    test(dates, timestamp(TimeUnit.SECOND), { useDate: true });
    test(dates, timestamp(TimeUnit.MILLISECOND), { useDate: true });
    test(dates, timestamp(TimeUnit.MICROSECOND), { useDate: true });
    test(dates, timestamp(TimeUnit.NANOSECOND), { useDate: true });

    // from millisecond-level numerical timestamps
    const ms = dates.map(d => +d);
    test(ms, timestamp(TimeUnit.SECOND));
    test(ms, timestamp(TimeUnit.MILLISECOND));
    test(ms.map(ts => ts + 0.001), timestamp(TimeUnit.MICROSECOND));
    test(ms.map(ts => ts + 0.000001), timestamp(TimeUnit.NANOSECOND));
  });

  it('builds interval year-month columns', () => {
    test(
      Int32Array.of(1220, 34341, 987654, -1232),
      interval(IntervalUnit.YEAR_MONTH)
    );
  });

  it('builds interval day-time columns', () => {
    test(
      [
        Int32Array.of(12, 1340),
        Int32Array.of(-1, 32451),
      ],
      interval(IntervalUnit.DAY_TIME)
    );
  });

  it('builds interval month-day-nano columns', () => {
    test(
      [
        Float64Array.of(1992, 3, 1e10),
        Float64Array.of(2000, 6, 15),
        Float64Array.of(-2000, -6, -15)
      ],
      interval(IntervalUnit.MONTH_DAY_NANO)
    );
  });

  it('builds list columns', () => {
    test([
      Int32Array.of(1, 2, 3),
      Int32Array.of(4, 5),
      Int32Array.of(6, 7, 8)
    ], list(int32()));
    test([
      Int32Array.of(1, 2, 3),
      Int32Array.of(4, 5),
      null,
      Int32Array.of(6, 7, 8)
    ], list(int32()));
  });

  it('builds large list columns', () => {
    test([
      Int32Array.of(1, 2, 3),
      Int32Array.of(4, 5),
      Int32Array.of(6, 7, 8)
    ], largeList(int32()));
    test([
      Int32Array.of(1, 2, 3),
      Int32Array.of(4, 5),
      null,
      Int32Array.of(6, 7, 8)
    ], largeList(int32()));
  });

  it('builds struct columns', () => {
    const data = [
      { foo: 1, bar: 'a', baz: true },
      { foo: 2, bar: 'b', baz: false },
      { foo: 3, bar: 'd', baz: true },
      null,
      { foo: 4, bar: 'c', baz: null },
    ];

    test(data, struct({
      foo: int32(),
      bar: utf8(),
      baz: bool()
    }));

    test(data, struct([
      field('foo', int32()),
      field('bar', utf8()),
      field('baz', bool()),
    ]));
  });

  it('builds union columns', () => {
    const unionTypeId = value => {
      const vtype = typeof value;
      return vtype === 'number' ? 0 : vtype === 'boolean' ? 1 : 2;
    };
    const ids = Int8Array.of(0, 0, 2, 1, 2, 2, 0, 1);
    const values = [1, 2, 'foo', true, null, 'baz', 3, false];
    const childTypes = [int32(), bool(), utf8()];

    const sparse = union(UnionMode.Sparse, childTypes, null, unionTypeId);
    const sparseCol = test(values, sparse);
    const sparseBatch = sparseCol.data[0];
    assert.equal(sparseBatch.nullCount, 1);
    assert.deepStrictEqual(sparseBatch.children.map(b => b.length), [8, 8, 8]);
    assert.deepStrictEqual(sparseBatch.children.map(b => b.nullCount), [5, 6, 6]);
    assert.deepStrictEqual(sparseBatch.typeIds, ids);

    const dense = union(UnionMode.Dense, childTypes, null, unionTypeId);
    const denseCol = test(values, dense);
    const denseBatch = denseCol.data[0];
    assert.equal(denseBatch.nullCount, 1);
    assert.deepStrictEqual(denseBatch.children.map(b => b.length), [3, 2, 3]);
    assert.deepStrictEqual(denseBatch.children.map(b => b.nullCount), [0, 0, 1]);
    assert.deepStrictEqual(denseBatch.typeIds, ids);
  });

  it('builds fixed size binary columns', () => {
    test([
      Uint8Array.of(255, 2, 3),
      null,
      Uint8Array.of(5, 9, 128)
    ], fixedSizeBinary(3));
  });

  it('builds fixed size list columns', () => {
    test([
      Int32Array.of(1, 2, 3),
      Int32Array.of(4, 5, 6),
      null,
      Int32Array.of(7, 8, 9)
    ], fixedSizeList(int32(), 3));
  });

  it('builds map columns', () => {
    const asMap = d => d.map(a => a && new Map(a));
    const keyvals = [
      [['foo', 1], ['bar', 2], ['baz', 3]],
      [['foo', 4], ['bar', 5], ['baz', 6]],
      null
    ];
    const reverse = keyvals.map(a => a && a.map(kv => kv.slice().reverse()));
    test(keyvals, map(utf8(), int16()));
    test(reverse, map(int16(), utf8()));
    test(asMap(keyvals), map(utf8(), int16()), { useMap: true });
    test(asMap(reverse), map(int16(), utf8()), { useMap: true });
  });

  it('builds duration columns', () => {
    const values = [10, 1000, 1e6, 1e8];
    test(values, duration(TimeUnit.SECOND));
    test(values, duration(TimeUnit.MILLISECOND));
    test(values, duration(TimeUnit.MICROSECOND));
    test(values, duration(TimeUnit.MICROSECOND));
    test(values.concat(86400000000), duration(TimeUnit.MICROSECOND));
    test(values.concat(86400000000000), duration(TimeUnit.MICROSECOND));

    const bigints = [10n, 1000n, 1000000n, 1000000000n, 86400000000000n];
    const opt = { useBigInt: true };
    test(bigints, duration(TimeUnit.SECOND), opt);
    test(bigints, duration(TimeUnit.MILLISECOND), opt);
    test(bigints, duration(TimeUnit.MICROSECOND), opt);
    test(bigints, duration(TimeUnit.MICROSECOND), opt);
  });

  it('builds run-end encoded columns', () => {
    function check(values, runs, type) {
      const col = test(values, type);
      // check run-ends
      const colRuns = col.data[0].children[0];
      assert.deepStrictEqual([...colRuns], runs);
      assert.ok(colRuns.values instanceof type.children[0].type.values);
    }

    const strs = ['foo', 'foo', 'baz', 'bar', null, 'baz', 'baz'];
    const srun = [2, 3, 4, 5, 7];
    const ints = [12, 34, 12, 12, 12, 27, 27, null, 34, 34];
    const irun = [1, 2, 5, 7, 8, 10];
    const arrs = [[1,2,3], [1,2,3], null, [3,5], [3,5]].map(x => x && Int32Array.from(x));
    const arun = [2, 3, 5];

    // 32-bit runs
    check(strs, srun, runEndEncoded(int32(), utf8()));
    check(ints, irun, runEndEncoded(int32(), int32()));
    check(arrs, arun, runEndEncoded(int32(), list(int32())));

    // 64-bit runs
    check(strs, srun, runEndEncoded(int64(), utf8()));
    check(ints, irun, runEndEncoded(int64(), int32()));
    check(arrs, arun, runEndEncoded(int64(), list(int32())));
  });

  it('builds dictionary columns', () => {
    function check(values, type) {
      const col = test(values, type);
      // check array type of indices
      assert.ok(col.data[0].values instanceof type.indices.values);
    }

    const strs = ['foo', 'foo', 'baz', 'bar', null, 'baz', 'bar'];
    const ints = [12, 34, 12, 12, 12, 27, null, 34];
    const arrs = [[1,2,3], [1,2,3], null, [3,5], [3,5]].map(x => x && Int32Array.from(x));

    check(strs, dictionary(utf8()));
    check(ints, dictionary(int32()));
    check(arrs, dictionary(list(int32())));
    check(strs, dictionary(utf8(), int16()));
    check(ints, dictionary(int32(), int16()));
  });

  it('builds columns with multiple record batches', () => {
    const data = [
      ...Array(10).fill(0),
      ...Array(10).fill(null),
      ...Array(10).fill(1),
      4, 5, 6
    ];
    const col = test(data, int16(), { maxBatchRows: 10 });
    assert.strictEqual(col.nullCount, 10);
    assert.strictEqual(col.data.length, 4);
    assert.deepStrictEqual(col.data.map(d => d.length), [10, 10, 10, 3]);

    const floats = Float64Array.from({ length: 10 }, Math.random);
    const tcol = test(floats, null, { maxBatchRows: 4 });
    assert.strictEqual(tcol.nullCount, 0);
    assert.strictEqual(tcol.length, 10);
    assert.strictEqual(tcol.data.length, 3);
    assert.deepStrictEqual(tcol.data.map(d => d.length), [4, 4, 2]);
  });

  it('builds columns from typed arrays', () => {
    test(Int8Array.of(1, 2, 3));
    test(Int16Array.of(1, 2, 3));
    test(Int32Array.of(1, 2, 3));
    test(Uint8Array.of(1, 2, 3));
    test(Uint16Array.of(1, 2, 3));
    test(Uint32Array.of(1, 2, 3));
    test(Float32Array.of(1, 2, 3));
    test(Float64Array.of(1, 2, 3));
    test(BigInt64Array.of(1n, 2n, 3n), null, { useBigInt: true });
    test(BigUint64Array.of(1n, 2n, 3n), null, { useBigInt: true });
  });

  it('builds columns from inferred types', () => {
    test([null, null, null]);
    test([1, 2, 3]);
    test([1e3, 2e3, 3e3]);
    test([1e6, 2e6, 3e6]);
    test([1.1, 2.2, 3.3]);
    test([1n, 2n, 3n], null, { useBigInt: true });
    test([1n, 2n, 3n], null, { useBigInt: true });
    test([true, false, true]);
    test([Int8Array.of(1,2), Int8Array.of(3,4), Int8Array.of(5,6)]);
  });

  it('builds columns with resized buffers', () => {
    const floats = Array.from({ length: 20_000 }, () => Math.random());
    const strs = floats.map(f => f.toFixed(3));
    test(floats, float64());
    test(strs, utf8());
  });
});
