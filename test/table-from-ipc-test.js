import assert from 'node:assert';
import { readFile } from 'node:fs/promises';
import { arrowFromDuckDB, arrowQuery } from './util/arrow-from-duckdb.js';
import { tableFromIPC } from '../src/index.js';
import { RowIndex } from '../src/struct.js';

const toDate = v => new Date(v);
const toBigInt = v => BigInt(v);

async function valueTest(values, dbType = null, arrayType, opt = undefined, transform = undefined) {
  const array = transform
    ? values.map((v, i) => v == null ? v : transform(v, i))
    : Array.from(values);
  const bytes = await arrowFromDuckDB(values, dbType);
  const column = tableFromIPC(bytes, opt).getChild('value');
  compare(column, array, arrayType);
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
    await valueTest(values, 'BIGINT', BigInt64Array, { useBigInt: true });
  });

  it('decodes boolean data', async () => {
    await valueTest([true, false, true], 'BOOLEAN', Array);
    await valueTest([true, false, null], 'BOOLEAN', Array);
  });

  it('decodes uint8 data', async () => {
    await valueTest([1, 2, 3], 'UTINYINT', Uint8Array);
    await valueTest([1, null, 3], 'UTINYINT', Array);
  });

  it('decodes uint16 data', async () => {
    await valueTest([1, 2, 3], 'USMALLINT', Uint16Array);
    await valueTest([1, null, 3], 'USMALLINT', Array);
  });

  it('decodes uint32 data', async () => {
    await valueTest([1, 2, 3], 'UINTEGER', Uint32Array);
    await valueTest([1, null, 3], 'UINTEGER', Array);
  });

  it('decodes uint64 data', async () => {
    // coerced to numbers
    await valueTest([1, 2, 3], 'UBIGINT', Float64Array);
    await valueTest([1, null, 3], 'UBIGINT', Array);
    // as bigints
    await valueTest([1, 2, 3], 'UBIGINT', BigUint64Array, { useBigInt: true }, toBigInt);
    await valueTest([1, null, 3], 'UBIGINT', Array, { useBigInt: true }, toBigInt);
  });

  it('decodes int8 data', async () => {
    await valueTest([1, 2, 3], 'TINYINT', Int8Array);
    await valueTest([1, null, 3], 'TINYINT', Array);
  });

  it('decodes int16 data', async () => {
    await valueTest([1, 2, 3], 'SMALLINT', Int16Array);
    await valueTest([1, null, 3], 'SMALLINT', Array);
  });

  it('decodes int32 data', async () => {
    await valueTest([1, 2, 3], 'INTEGER', Int32Array);
    await valueTest([1, null, 3], 'INTEGER', Array);
  });

  it('decodes int64 data', async () => {
    // coerced to numbers
    await valueTest([1, 2, 3], 'BIGINT', Float64Array);
    await valueTest([1, null, 3], 'BIGINT', Array);
    // as bigints
    await valueTest([1, 2, 3], 'BIGINT', BigInt64Array, { useBigInt: true }, toBigInt);
    await valueTest([1, null, 3], 'BIGINT', Array, { useBigInt: true }, toBigInt);
  });

  it('decodes float32 data', async () => {
    await valueTest([1.1, 2.2, 3.3], 'FLOAT', Float32Array, {}, v => Math.fround(v));
    await valueTest([1.1, null, 3.3], 'FLOAT', Array, {}, v => Math.fround(v));
  });

  it('decodes float64 data', async () => {
    await valueTest([1.1, 2.2, 3.3], 'DOUBLE', Float64Array);
    await valueTest([1.1, null, 3.3], 'DOUBLE', Array);
  });

  it('decodes decimal data', async () => {
    await valueTest([1.212, 3.443, 5.600], 'DECIMAL(18,3)', Float64Array);
    await valueTest([1.212, null, 5.600], 'DECIMAL(18,3)', Array);
  });

  it('decodes date day data', async () => {
    const values = ['2001-01-01', '2004-02-03', '2006-12-31'];
    const nulls = ['2001-01-01', null, '2006-12-31'];
    // as timestamps
    await valueTest(values, 'DATE', Float64Array, {}, v => +toDate(v));
    await valueTest(nulls, 'DATE', Array, {}, v => +toDate(v));
    // as Date objects
    await valueTest(values, 'DATE', Array, { useDate: true }, toDate);
    await valueTest(nulls, 'DATE', Array, { useDate: true }, toDate);
  });

  it('decodes timestamp data', async () => {
    const ns = ['1992-09-20T11:30:00.123456789Z', '2002-12-13T07:28:56.564738209Z'];
    const us = ['1992-09-20T11:30:00.123457Z', '2002-12-13T07:28:56.564738Z'];
    const ms = ['1992-09-20T11:30:00.123Z', '2002-12-13T07:28:56.565Z'];
    const sec = ['1992-09-20T11:30:00Z', '2002-12-13T07:28:57Z'];

    // From DuckDB docs: When defining timestamps as a TIMESTAMP_NS literal, the
    // decimal places beyond microseconds are ignored. The TIMESTAMP_NS type is
    // able to hold nanoseconds when created e.g., loading Parquet files.
    const _ns = [0.456, 0.738]; // DuckDB truncates here
    const _us = [0.457, 0.738]; // DuckDB rounds here

    // as timestamps
    await valueTest(ns, 'TIMESTAMP_NS', Float64Array, {}, (v, i) => +toDate(v) + _ns[i]);
    await valueTest(us, 'TIMESTAMP', Float64Array, {}, (v, i) => +toDate(v) + _us[i]);
    await valueTest(ms, 'TIMESTAMP_MS', Float64Array, {}, v => +toDate(v));
    await valueTest(sec, 'TIMESTAMP_S', Float64Array, {}, v => +toDate(v));

    // as timestamps with nulls
    await valueTest(ns.concat(null), 'TIMESTAMP_NS', Array, {}, (v, i) => +toDate(v) + _ns[i]);
    await valueTest(us.concat(null), 'TIMESTAMP', Array, {}, (v, i) => +toDate(v) + _us[i]);
    await valueTest(ms.concat(null), 'TIMESTAMP_MS', Array, {}, v => +toDate(v));
    await valueTest(sec.concat(null), 'TIMESTAMP_S', Array, {}, v => +toDate(v));

    // as dates
    await valueTest(ns, 'TIMESTAMP_NS', Array, { useDate: true }, toDate);
    await valueTest(us, 'TIMESTAMP', Array, { useDate: true }, toDate);
    await valueTest(ms, 'TIMESTAMP_MS', Array, { useDate: true }, toDate);
    await valueTest(sec, 'TIMESTAMP_S', Array, { useDate: true }, toDate);

    // as dates with nulls
    await valueTest(ns.concat(null), 'TIMESTAMP_NS', Array, { useDate: true }, toDate);
    await valueTest(us.concat(null), 'TIMESTAMP', Array, { useDate: true }, toDate);
    await valueTest(ms.concat(null), 'TIMESTAMP_MS', Array, { useDate: true }, toDate);
    await valueTest(sec.concat(null), 'TIMESTAMP_S', Array, { useDate: true }, toDate);
  });

  it('decodes interval data', async () => {
    const values = ['2 years', null, '12 years 2 month 1 day 5 seconds', '1 microsecond'];
    const js = [
      Float64Array.of(24, 0, 0),
      null,
      Float64Array.of(146, 1, 5000000000),
      Float64Array.of(0, 0, 1000)
    ];
    await valueTest(values, 'INTERVAL', Array, {}, (v, i) => js[i]);
  });

  it('decodes utf8 data', async () => {
    await valueTest(['foo', 'bar', 'baz'], 'VARCHAR', Array);
    await valueTest(['foo', null, 'baz'], 'VARCHAR', Array);
  });

  it('decodes list data', async () => {
    const values = [[1, 2, 3, 4], [5, 6], [7, 8, 9]];
    const pnulls = [[1, 2, 3, 4], null, [7, 8, 9]];
    const cnulls = [[1, 2, null, 4], [5, null, 6], [7, null, 9]];
    await valueTest(values, 'INTEGER[]', Array, {}, v => Int32Array.from(v));
    await valueTest(pnulls, 'INTEGER[]', Array, {}, v => Int32Array.from(v));
    await valueTest(cnulls, 'INTEGER[]', Array);

    const svalues = [['a', 'b', 'c', 'd'], ['e', 'f'], ['g', 'h', 'i']];
    const spnulls = [['a', 'b', 'c', 'd'], null, ['g', 'h', 'i']];
    const scnulls = [['a', 'b', null, 'd'], ['e', null, 'f'], ['g', null, 'i']];
    await valueTest(svalues, 'VARCHAR[]');
    await valueTest(spnulls, 'VARCHAR[]');
    await valueTest(scnulls, 'VARCHAR[]');
  });

  it('decodes fixed list data', async () => {
    const values = [[1, 2, 3], [4, 5, 6], [7, 8, 9]];
    const pnulls = [[1, 2, 3], null, [7, 8, 9]];
    const cnulls = [[1, null, 3], [null, 5, 6], [7, 8, null]];
    await valueTest(values, 'INTEGER[3]', Array, {}, v => Int32Array.from(v));
    await valueTest(pnulls, 'INTEGER[3]', Array, {}, v => Int32Array.from(v));
    await valueTest(cnulls, 'INTEGER[3]', Array);

    const svalues = [['a', 'b', 'c'], ['d', 'e', 'f'], ['g', 'h', 'i']];
    const spnulls = [['a', 'b', 'c'], null, ['g', 'h', 'i']];
    const scnulls = [['a', null, 'c'], [null, 'e', 'f'], ['g', 'h', null]];
    await valueTest(svalues, 'VARCHAR[]');
    await valueTest(spnulls, 'VARCHAR[]');
    await valueTest(scnulls, 'VARCHAR[]');
  });

  it('decodes list view data', async () => {
    const buf = await readFile(`test/data/listview.arrows`);
    const column = tableFromIPC(new Uint8Array(buf)).getChild('value');
    compare(column, [
      ['foo', 'bar', 'baz'],
      null,
      ['baz', null, 'foo'],
      ['foo']
    ]);
  });

  it('decodes large list view data', async () => {
    const buf = await readFile(`test/data/largelistview.arrows`);
    const column = tableFromIPC(new Uint8Array(buf)).getChild('value');
    compare(column, [
      ['foo', 'bar', 'baz'],
      null,
      ['baz', null, 'foo'],
      ['foo']
    ]);
  });

  it('decodes union data', async () => {
    const values = ['a', 2, 'c'];
    const nulls = ['a', null, 'c'];
    const type = 'UNION(i INTEGER, v VARCHAR)';
    await valueTest(values, type);
    await valueTest(nulls, type);
  });

  it('decodes map data', async () => {
    const data = [
      [ ['foo', 1], ['bar', 2] ],
      [ ['foo', null], ['baz', 3] ]
    ];
    const values = data.map(e => new Map(e));
    await valueTest(values, null, Array, {}, v => Array.from(v.entries()));
    await valueTest(values, null, Array, { useMap: true });
  });

  it('decodes struct data', async () => {
    await valueTest([ {a: 1, b: 'foo'}, {a: 2, b: 'baz'} ]);
    await valueTest([ {a: 1, b: 'foo'}, null, {a: 2, b: 'baz'} ]);
    await valueTest([ {a: null, b: 'foo'}, {a: 2, b: null} ]);
    await valueTest([ {a: ['a', 'b'], b: Math.E}, {a: ['c', 'd'], b: Math.PI} ]);
  });

  it('decodes struct data with useProxy', async () => {
    const data = [
      [ {a: 1, b: 'foo'}, {a: 2, b: 'baz'} ],
      [ {a: 1, b: 'foo'}, null, {a: 2, b: 'baz'} ],
      [ {a: null, b: 'foo'}, {a: 2, b: null} ],
      [ {a: ['a', 'b'], b: Math.E}, {a: ['c', 'd'], b: Math.PI} ]
    ];
    for (const values of data) {
      const bytes = await arrowFromDuckDB(values);
      const column = tableFromIPC(bytes, { useProxy: true }).getChild('value');
      const proxies = column.toArray();
      assert.strictEqual(proxies.every(p => p === null || p[RowIndex] >= 0), true);
      assert.deepStrictEqual(proxies.map(p => p ? p.toJSON() : null), values);
    }
  });

  it('decodes run-end-encoded data', async () => {
    const buf = await readFile(`test/data/runendencoded.arrows`);
    const table = tableFromIPC(new Uint8Array(buf));
    const column = table.getChild('value');
    const [{ children: [runs, vals] }] = column.data;
    assert.deepStrictEqual([...runs], [2, 3, 4, 6, 8, 9]);
    assert.deepStrictEqual([...vals], ['foo', null, 'bar', 'baz', null, 'foo']);
    compare(column, ['foo', 'foo', null, 'bar', 'baz', 'baz', null, null, 'foo']);
  });

  it('decodes run-end-encoded data with 64-bit run ends', async () => {
    const buf = await readFile(`test/data/runendencoded64.arrows`);
    const table = tableFromIPC(new Uint8Array(buf), { useBigInt: true });
    const column = table.getChild('value');
    const [{ children: [runs, vals] }] = column.data;
    assert.deepStrictEqual([...runs], [2n, 3n, 4n, 6n, 8n, 9n]);
    assert.deepStrictEqual([...vals], ['foo', null, 'bar', 'baz', null, 'foo']);
    compare(column, ['foo', 'foo', null, 'bar', 'baz', 'baz', null, null, 'foo']);
  });

  it('decodes binary view data', async () => {
    const f = ['foo', 'foo', null, 'bar', 'baz', 'baz', null, null, 'foo'];
    const s = ['foobazbarbipbopboodeedoozoo', 'foo', null, 'bar', 'baz', 'baz', null, null, 'foo'];
    const enc = new TextEncoder();
    const binary = v => v != null ? enc.encode(v) : null;
    const buf = await readFile(`test/data/binaryview.arrows`);
    const table = tableFromIPC(new Uint8Array(buf));
    const flat = table.getChild('flat'); // all strings under 12 bytes
    const spill = table.getChild('spill'); // some strings spill to data buffer
    compare(flat, f.map(binary));
    compare(spill, s.map(binary));
  });

  it('decodes utf8 view data', async () => {
    const f = ['foo', 'foo', null, 'bar', 'baz', 'baz', null, null, 'foo'];
    const s = ['foobazbarbipbopboodeedoozoo', 'foo', null, 'bar', 'baz', 'baz', null, null, 'foo'];
    const buf = await readFile(`test/data/utf8view.arrows`);
    const table = tableFromIPC(new Uint8Array(buf));
    const flat = table.getChild('flat'); // all strings under 12 bytes
    const spill = table.getChild('spill'); // some strings spill to data buffer
    compare(flat, f);
    compare(spill, s);
  });

  it('decodes empty data', async () => {
    // For empty result sets, DuckDB node only returns a zero byte
    // Other variants may include a schema message
    const sql = 'SELECT schema_name FROM information_schema.schemata WHERE false';
    const table = tableFromIPC(await arrowQuery(sql));
    assert.strictEqual(table.numRows, 0);
    assert.strictEqual(table.numCols, 0);
    assert.deepStrictEqual(table.toColumns(), {});
    assert.deepStrictEqual(table.toArray(), []);
    assert.deepStrictEqual([...table], []);
  });
});
