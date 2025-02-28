import assert from 'node:assert';
import { tableFromIPC } from '../src/index.js';
import { arrowFromDuckDB } from './util/arrow-from-duckdb.js';
import { binaryView, bool, dateDay, decimal, decimal32, decimal128, decimal256, decimal64, empty, fixedListInt32, fixedListUtf8, float32, float64, int16, int32, int64, int8, intervalMonthDayNano, largeListView, listInt32, listUtf8, listView, map, runEndEncoded32, runEndEncoded64, struct, timestampMicrosecond, timestampMillisecond, timestampNanosecond, timestampSecond, uint16, uint32, uint64, uint8, union, utf8, utf8View } from './util/data.js';
import { RowIndex } from '../src/util/struct.js';

const toBigInt = v => BigInt(v);
const toDate = v => new Date(v);
const toFloat32 = v => Math.fround(v);
const toDecimalInt = v => Math.round(v * 100);
const toDecimalBigInt = v => BigInt(toDecimalInt(v));

async function test(dataMethod, arrayType, opt, transform) {
  const data = await dataMethod();
  for (const { bytes, values, nullCount } of data) {
    valueTest(bytes, values, nullCount ? Array : arrayType, opt, transform);
  }
}

function valueTest(bytes, values, arrayType, opt = undefined, transform = undefined, name = 'value') {
  const array = transform
    ? values.map((v, i) => v == null ? v : transform(v, i))
    : Array.from(values);
  const column = tableFromIPC(bytes, opt).getChild(name);
  compare(column, array, arrayType);
  return column;
}

function compare(column, array, arrayType = Array) {
  // test values extracted using toArray
  const data = column.toArray();
  assert.ok(data instanceof arrayType, 'toArray type check');
  assert.deepStrictEqual(data, arrayType.from(array), 'toArray equality');

  // test values extracted using column iterator
  assert.deepStrictEqual([...column], array, 'iterator equality');

  // test values extracted using column at() method
  const extract = Array.from(array, (_, i) => column.at(i));
  assert.deepStrictEqual(extract, array, 'at equality');
}

describe('tableFromIPC', () => {
  it('throws when coercing unsafe int64 values', async () => {
    const values = [
      BigInt(Number.MAX_SAFE_INTEGER) - 1n,
      BigInt(Number.MAX_SAFE_INTEGER) + 1n
    ];
    const bytes = await arrowFromDuckDB(values, 'BIGINT');

    // coerced to numbers
    assert.throws(() => tableFromIPC(bytes).getChild('value').toArray());

    // as bigints
    valueTest(bytes, values, BigInt64Array, { useBigInt: true });
  });

  it('decodes uint8 data', () => test(uint8, Uint8Array));
  it('decodes uint16 data', () => test(uint16, Uint16Array));
  it('decodes uint32 data', () => test(uint32, Uint32Array));
  it('decodes uint64 data', () => test(uint64, Float64Array));
  it('decodes uint64 data to bigint', () => test(uint64, BigUint64Array, { useBigInt: true }, toBigInt));

  it('decodes int8 data', () => test(int8, Int8Array));
  it('decodes int16 data', () => test(int16, Int16Array));
  it('decodes int32 data', () => test(int32, Int32Array));
  it('decodes int64 data', () => test(int64, Float64Array));
  it('decodes int64 data to bigint', () => test(int64, BigInt64Array, { useBigInt: true }, toBigInt));

  it('decodes float32 data', () => test(float32, Float32Array, {}, toFloat32));
  it('decodes float64 data', () => test(float64, Float64Array));

  it('decodes utf8 data', () => test(utf8));

  it('decodes boolean data', () => test(bool));

  it('decodes decimal data', () => test(decimal, Float64Array));
  it('decodes decimal32 data', () => test(decimal32, Float64Array));
  it('decodes decimal64 data', () => test(decimal64, Float64Array));
  it('decodes decimal128 data', () => test(decimal128, Float64Array));
  it('decodes decimal256 data', () => test(decimal256, Float64Array));
  it('decodes decimal32 data to int', () => test(decimal32, Int32Array, { useDecimalInt: true }, toDecimalInt));
  it('decodes decimal64 data to bigint', () => test(decimal64, Array, { useDecimalInt: true }, toDecimalBigInt));
  it('decodes decimal128 data to bigint', () => test(decimal128, Array, { useDecimalInt: true }, toDecimalBigInt));
  it('decodes decimal256 data to bigint', () => test(decimal256, Array, { useDecimalInt: true }, toDecimalBigInt));

  it('decodes date day data', () => test(dateDay, Float64Array));
  it('decodes date day data to dates', () => test(dateDay, Array, { useDate: true }, toDate));

  it('decodes timestamp nanosecond data', () => test(timestampNanosecond, Float64Array));
  it('decodes timestamp microsecond data', () => test(timestampMicrosecond, Float64Array));
  it('decodes timestamp millisecond data', () => test(timestampMillisecond, Float64Array));
  it('decodes timestamp second data', () => test(timestampSecond, Float64Array));
  it('decodes timestamp nanosecond data to dates', () => test(timestampNanosecond, Array, { useDate: true }, toDate));
  it('decodes timestamp microsecond data to dates', () => test(timestampMicrosecond, Array, { useDate: true }, toDate));
  it('decodes timestamp millisecond data to dates', () => test(timestampMillisecond, Array, { useDate: true }, toDate));
  it('decodes timestamp second data to dates', () => test(timestampSecond, Array, { useDate: true }, toDate));

  it('decodes interval year/month/nano data', () => test(intervalMonthDayNano));

  it('decodes list int32 data', () => test(listInt32));
  it('decodes list utf8 data', () => test(listUtf8));

  it('decodes fixed list int32 data', () => test(fixedListInt32));
  it('decodes fixed utf8 data', () => test(fixedListUtf8));

  it('decodes list view data', () => test(listView));
  it('decodes large list view data', () => test(largeListView));

  it('decodes union data', () => test(union));

  it('decodes map data', () => test(map, Array, {}, v => Array.from(v.entries())));
  it('decodes map data to maps', () => test(map, Array, { useMap: true }));

  it('decodes struct data', () => test(struct));
  it('decodes struct data with useProxy', async () => {
    const data = await struct();
    for (const { bytes, values } of data) {
      const column = tableFromIPC(bytes, { useProxy: true }).getChildAt(0);
      const proxies = column.toArray();
      assert.strictEqual(proxies.every(p => p === null || p[RowIndex] >= 0), true);
      assert.deepStrictEqual(proxies.map(p => p ? p.toJSON() : null), values);
    }
  });

  it('decodes run-end-encoded data with 32-bit run ends', async () => {
    const data = await runEndEncoded32();
    for (const { bytes, runs, values } of data) {
      const column = valueTest(bytes, values);
      const ree = column.data[0].children;
      assert.deepStrictEqual([...ree[0]], runs.counts);
      assert.deepStrictEqual([...ree[1]], runs.values);
    }
  });
  it('decodes run-end-encoded data with 64-bit run ends', async () => {
    const data = await runEndEncoded64();
    for (const { bytes, runs, values } of data) {
      const column = valueTest(bytes, values);
      const ree = column.data[0].children;
      assert.deepStrictEqual([...ree[0]], runs.counts);
      assert.deepStrictEqual([...ree[1]], runs.values);
    }
  });

  it('decodes binary view data', async () => {
    const data = await binaryView();
    for (const { bytes, values: { flat, spill } } of data) {
      valueTest(bytes, flat, Array, {}, null, 'flat');
      valueTest(bytes, spill, Array, {}, null, 'spill');
    }
  });

  it('decodes utf8 view data', async () => {
    const data = await utf8View();
    for (const { bytes, values: { flat, spill } } of data) {
      valueTest(bytes, flat, Array, {}, null, 'flat');
      valueTest(bytes, spill, Array, {}, null, 'spill');
    }
  });

  it('decodes empty data', async () => {
    const data = await empty();
    for (const { bytes } of data) {
      const table = tableFromIPC(bytes);
      table.schema.fields.map((f, i) => {
        assert.deepStrictEqual(table.getChildAt(i).type, f.type);
      });
      assert.strictEqual(table.numRows, 0);
      assert.strictEqual(table.numCols, table.schema.fields.length);
      assert.deepStrictEqual(table.toArray(), []);
      assert.deepStrictEqual([...table], []);
    }
  });
});
