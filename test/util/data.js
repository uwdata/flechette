import { readFile } from 'node:fs/promises';
import { arrowFromDuckDB } from './arrow-from-duckdb.js';

const toTimestamp = (v, off = 0) => v == null ? null : (+new Date(v) + off);
const toInt32s = v => v == null ? null : v.some(x => x == null) ? v : Int32Array.of(...v);

async function dataQuery(data, type, jsValues) {
  return Promise.all(data.map(async (array, i) => {
    const values = jsValues?.[i] ?? array;
    return {
      values,
      bytes: await arrowFromDuckDB(array, type),
      nullCount: values.reduce((nc, v) => v == null ? ++nc : nc, 0)
    };
  }));
}

export function bool() {
  return dataQuery([
    [true, false, true],
    [true, false, null]
  ], 'BOOLEAN');
}

export function uint8() {
  return dataQuery([
    [1, 2, 3],
    [1, null, 3]
  ], 'UTINYINT');
}

export function uint16() {
  return dataQuery([
    [1, 2, 3],
    [1, null, 3]
  ], 'USMALLINT');
}

export function uint32() {
  return dataQuery([
    [1, 2, 3],
    [1, null, 3]
  ], 'UINTEGER');
}

export function uint64() {
  return dataQuery([
    [1, 2, 3],
    [1, null, 3]
  ], 'UBIGINT');
}

export function int8() {
  return dataQuery([
    [1, 2, 3],
    [1, null, 3]
  ], 'TINYINT');
}

export function int16() {
  return dataQuery([
    [1, 2, 3],
    [1, null, 3]
  ], 'SMALLINT');
}

export function int32() {
  return dataQuery([
    [1, 2, 3],
    [1, null, 3]
  ], 'INTEGER');
}

export function int64() {
  return dataQuery([
    [1, 2, 3],
    [1, null, 3]
  ], 'BIGINT');
}

export function float32() {
  return dataQuery([
    [1.1, 2.2, 3.3],
    [1.1, null, 3.3]
  ], 'FLOAT');
}

export function float64() {
  return dataQuery([
    [1.1, 2.2, 3.3],
    [1.1, null, 3.3]
  ], 'DOUBLE');
}

export function decimal() {
  return dataQuery([
    [1.212, 3.443, 5.600],
    [1.212, null, 5.600]
  ], 'DECIMAL(18,3)');
}

export function dateDay() {
  const data = [
    ['2001-01-01', '2004-02-03', '2006-12-31'],
    ['2001-01-01', null, '2006-12-31']
  ];
  const vals = data.map(v => v.map(d => toTimestamp(d)));
  return dataQuery(data, 'DATE', vals);
}

export function timestampNanosecond() {
  const ns = [0.4568, 0.7382]; // DuckDB truncates here
  const ts = ['1992-09-20T11:30:00.123456789Z', '2002-12-13T07:28:56.564738209Z'];
  const data = [ts, ts.concat(null)];
  const vals = data.map(v => v.map((d, i) => toTimestamp(d, ns[i])));
  return dataQuery(data, 'TIMESTAMP_NS', vals);
}

export function timestampMicrosecond() {
  const us = [0.457, 0.738]; // DuckDB rounds here
  const ts = ['1992-09-20T11:30:00.123457Z', '2002-12-13T07:28:56.564738Z'];
  const data = [ts, ts.concat(null)];
  const vals = data.map(v => v.map((d, i) => toTimestamp(d, us[i])));
  return dataQuery(data, 'TIMESTAMP_NS', vals);
}

export function timestampMillisecond() {
  const ts = ['1992-09-20T11:30:00.123Z', '2002-12-13T07:28:56.565Z'];
  const data = [ts, ts.concat(null)];
  const vals = data.map(v => v.map(d => toTimestamp(d)));
  return dataQuery(data, 'TIMESTAMP_MS', vals);
}

export function timestampSecond() {
  const ts = ['1992-09-20T11:30:00Z', '2002-12-13T07:28:57Z'];
  const data = [ts, ts.concat(null)];
  const vals = data.map(v => v.map(d => toTimestamp(d)));
  return dataQuery(data, 'TIMESTAMP_S', vals);
}

export function intervalMonthDayNano() {
  return dataQuery([
    ['2 years', null, '12 years 2 month 1 day 5 seconds', '1 microsecond']
  ], 'INTERVAL', [[
    Float64Array.of(24, 0, 0),
    null,
    Float64Array.of(146, 1, 5000000000),
    Float64Array.of(0, 0, 1000)
  ]]);
}

export function utf8() {
  return dataQuery([
    ['foo', 'bar', 'baz'],
    ['foo', null, 'baz']
  ], 'VARCHAR');
}

export function listInt32() {
  const data = [
    [[1, 2, 3, 4], [5, 6], [7, 8, 9]],
    [[1, 2, 3, 4], null, [7, 8, 9]],
    [[1, 2, null, 4], [5, null, 6], [7, null, 9]]
  ];
  const vals = data.map(v => v.map(toInt32s));
  return dataQuery(data, 'INTEGER[]', vals);
}

export function listUtf8() {
  return dataQuery([
    [['a', 'b', 'c', 'd'], ['e', 'f'], ['g', 'h', 'i']],
    [['a', 'b', 'c', 'd'], null, ['g', 'h', 'i']],
    [['a', 'b', null, 'd'], ['e', null, 'f'], ['g', null, 'i']]
  ], 'VARCHAR[]');
}

export function fixedListInt32() {
  const data = [
    [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
    [[1, 2, 3], null, [7, 8, 9]],
    [[1, null, 3], [null, 5, 6], [7, 8, null]]
  ];
  const vals = data.map(v => v.map(toInt32s));
  return dataQuery(data, 'INTEGER[3]', vals);
}

export function fixedListUtf8() {
  return dataQuery([
    [['a', 'b', 'c'], ['d', 'e', 'f'], ['g', 'h', 'i']],
    [['a', 'b', 'c'], null, ['g', 'h', 'i']],
    [['a', null, 'c'], [null, 'e', 'f'], ['g', 'h', null]]
  ], 'VARCHAR[3]');
}

export function union() {
  return dataQuery([
    ['a', 2, 'c'],
    ['a', null, 'c']
  ], 'UNION(i INTEGER, v VARCHAR)');
}

export function map() {
  return dataQuery([
    [
      new Map([ ['foo', 1], ['bar', 2] ]),
      new Map([ ['foo', null], ['baz', 3] ])
    ]
  ]);
}

export function struct() {
  return dataQuery([
    [ {a: 1, b: 'foo'}, {a: 2, b: 'baz'} ],
    [ {a: 1, b: 'foo'}, null, {a: 2, b: 'baz'} ],
    [ {a: null, b: 'foo'}, {a: 2, b: null} ],
    [ {a: ['a', 'b'], b: Math.E}, {a: ['c', 'd'], b: Math.PI} ]
  ]);
}

export async function listView() {
  const bytes = await readFile(`test/data/listview.arrows`);
  return [{
    values: [
      ['foo', 'bar', 'baz'],
      null,
      ['baz', null, 'foo'],
      ['foo']
    ],
    bytes,
    nullCount: 1
  }];
}

export async function largeListView() {
  const bytes = await readFile(`test/data/largelistview.arrows`);
  return [{
    values: [
      ['foo', 'bar', 'baz'],
      null,
      ['baz', null, 'foo'],
      ['foo']
    ],
    bytes,
    nullCount: 1
  }];
}

export async function runEndEncoded32() {
  const bytes = new Uint8Array(await readFile(`test/data/runendencoded.arrows`));
  return [{
    runs: {
      counts: [2, 3, 4, 6, 8, 9],
      values: ['foo', null, 'bar', 'baz', null, 'foo']
    },
    values: ['foo', 'foo', null, 'bar', 'baz', 'baz', null, null, 'foo'],
    bytes,
    nullCount: 3
  }];
}

export async function runEndEncoded64() {
  const bytes = new Uint8Array(await readFile(`test/data/runendencoded64.arrows`));
  return [{
    runs: {
      counts: [2, 3, 4, 6, 8, 9],
      values: ['foo', null, 'bar', 'baz', null, 'foo']
    },
    values: ['foo', 'foo', null, 'bar', 'baz', 'baz', null, null, 'foo'],
    bytes,
    nullCount: 3
  }];
}

export async function binaryView() {
  const encoder = new TextEncoder();
  const toBytes = v => v == null ? null : encoder.encode(v);
  const bytes = new Uint8Array(await readFile(`test/data/binaryview.arrows`));
  return [{
    values: {
      flat: ['foo', 'foo', null, 'bar', 'baz', 'baz', null, null, 'foo'].map(toBytes),
      spill: ['foobazbarbipbopboodeedoozoo', 'foo', null, 'bar', 'baz', 'baz', null, null, 'foo'].map(toBytes)
    },
    bytes,
    nullCount: 3
  }];
}

export async function utf8View() {
  const bytes = new Uint8Array(await readFile(`test/data/utf8view.arrows`));
  return [{
    values: {
      flat: ['foo', 'foo', null, 'bar', 'baz', 'baz', null, null, 'foo'],
      spill: ['foobazbarbipbopboodeedoozoo', 'foo', null, 'bar', 'baz', 'baz', null, null, 'foo']
    },
    bytes,
    nullCount: 3
  }];
}

// For empty result sets, DuckDB node only returns a zero byte
// Other variants may include a schema message
export async function empty() {
  return [
    {
      values: [],
      bytes: Uint8Array.of(0, 0, 0, 0),
      nullCount: 0
    },
    {
      values: [],
      bytes: new Uint8Array(await readFile(`test/data/empty.arrows`)),
      nullCount: 0
    }
  ];
}
