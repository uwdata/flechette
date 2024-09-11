import assert from 'node:assert';
import { IntervalUnit, UnionMode, binary, bool, columnFromArray, decimal, dictionary, field, fixedSizeBinary, fixedSizeList, float16, float32, float64, int16, int32, int64, int8, interval, list, map, runEndEncoded, struct, uint16, uint32, uint64, uint8, union, utf8 } from '../src/index.js';
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

describe('column', () => {
  it('creates integer columns', () => {
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

  it('creates float columns', () => {
    // without nulls
    test([1, 2, 3], float16());
    test([1, 2, 3], float32());
    test([1, 2, 3], float64());

    // with nulls
    test([1, 2, null, 3], float16());
    test([1, 2, null, 3], float32());
    test([1, 2, null, 3], float64());
  });

  it('creates bool columns', () => {
    test([true, false, true, false], bool());
    test([true, false, null, true, false], bool());
  });

  it('creates utf8 columns', () => {
    test(['foo', 'bar', 'baz'], utf8());
    test(['foo', 'bar', null, 'baz'], utf8());
  });

  it('creates binary columns', () => {
    test([
      Uint8Array.of(255, 2, 3, 1),
      null,
      Uint8Array.of(5, 9, 128)
    ], binary());
  });

  it('creates decimal columns', () => {
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

    const opt = { useDecimalBigInt: true };

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

  it('creates month-day-nano interval columns', () => {
    test(
      [
        Float64Array.of(1992, 3, 1e10),
        Float64Array.of(2000, 6, 15)
      ],
      interval(IntervalUnit.MONTH_DAY_NANO)
    );
  });

  it('creates fixed size binary columns', () => {
    test([
      Uint8Array.of(255, 2, 3),
      null,
      Uint8Array.of(5, 9, 128)
    ], fixedSizeBinary(3));
  });

  it('creates list columns', () => {
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

  it('creates fixed size list columns', () => {
    test([
      Int32Array.of(1, 2, 3),
      Int32Array.of(4, 5, 6),
      null,
      Int32Array.of(7, 8, 9)
    ], fixedSizeList(int32(), 3));
  });

  it('creates struct columns', () => {
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

  it('creates union columns', () => {
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

  it('creates map columns', () => {
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

  it('creates dictionary columns', () => {
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

  it('creates run-end encoded columns', () => {
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

  it('creates columns with multiple record batches', () => {
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
  });

  it('creates columns from typed arrays', () => {
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

  it('creates columns from inferred types', () => {
    test([1, 2, 3]);
    test([1e3, 2e3, 3e3]);
    test([1e6, 2e6, 3e6]);
    test([1.1, 2.2, 3.3]);
    test([1n, 2n, 3n], null, { useBigInt: true });
    test([1n, 2n, 3n], null, { useBigInt: true });
    test([true, false, true]);
    test([Int8Array.of(1,2), Int8Array.of(3,4), Int8Array.of(5,6)]);
  });
});
