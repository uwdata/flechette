import { readFile } from 'node:fs/promises';

const toTimestamp = (v, off = 0) => v == null ? null : (+new Date(v) + off);
const toInt32s = v => v == null ? null : v.some(x => x == null) ? v : Int32Array.of(...v);

async function loadData(data, name, jsValues) {
  return Promise.all(data.map(async (array, i) => {
    const values = jsValues?.[i] ?? array;
    return {
      values,
      bytes: new Uint8Array(await readFile(`test/data/${name}_${i}.arrows`)),
      nullCount: values.reduce((nc, v) => v == null ? ++nc : nc, 0)
    };
  }));
}

export function bool() {
  return loadData([
    [true, false, true],
    [true, false, null]
  ], 'bool');
}

export function uint8() {
  return loadData([
    [1, 2, 3],
    [1, null, 3]
  ], 'uint8');
}

export function uint16() {
  return loadData([
    [1, 2, 3],
    [1, null, 3]
  ], 'uint16');
}

export function uint32() {
  return loadData([
    [1, 2, 3],
    [1, null, 3]
  ], 'uint32');
}

export function uint64() {
  return loadData([
    [1, 2, 3],
    [1, null, 3]
  ], 'uint64');
}

export function int8() {
  return loadData([
    [1, 2, 3],
    [1, null, 3]
  ], 'int8');
}

export function int16() {
  return loadData([
    [1, 2, 3],
    [1, null, 3]
  ], 'int16');
}

export function int32() {
  return loadData([
    [1, 2, 3],
    [1, null, 3]
  ], 'int32');
}

export function int64() {
  return loadData([
    [1, 2, 3],
    [1, null, 3]
  ], 'int64');
}

export function float32() {
  return loadData([
    [1.1, 2.2, 3.3],
    [1.1, null, 3.3]
  ], 'float32');
}

export function float64() {
  return loadData([
    [1.1, 2.2, 3.3],
    [1.1, null, 3.3]
  ], 'float64');
}

export function decimal() {
  return loadData([
    [1.212, 3.443, 5.600],
    [1.212, null, 5.600]
  ], 'decimal');
}

async function loadDecimal(bitWidth) {
  const bytes = new Uint8Array(await readFile(`test/data/decimal${bitWidth}.arrows`));
  return [{
    values: [123.45, 0, -123.45],
    bytes,
    nullCount: 0
  }];
}

export function decimal32() { return loadDecimal(32); }
export function decimal64() { return loadDecimal(64); }
export function decimal128() { return loadDecimal(128); }
export function decimal256() { return loadDecimal(256); }

export function dateDay() {
  const data = [
    ['2001-01-01', '2004-02-03', '2006-12-31'],
    ['2001-01-01', null, '2006-12-31']
  ];
  const vals = data.map(v => v.map(d => toTimestamp(d)));
  return loadData(data, 'dateDay', vals);
}

export function timestampNanosecond() {
  const ns = [0.4568, 0.7382]; // DuckDB truncates here
  const ts = ['1992-09-20T11:30:00.123456789Z', '2002-12-13T07:28:56.564738209Z'];
  const data = [ts, ts.concat(null)];
  const vals = data.map(v => v.map((d, i) => toTimestamp(d, ns[i])));
  return loadData(data, 'timestampNanosecond', vals);
}

export function timestampMicrosecond() {
  const us = [0.457, 0.738]; // DuckDB rounds here
  const ts = ['1992-09-20T11:30:00.123457Z', '2002-12-13T07:28:56.564738Z'];
  const data = [ts, ts.concat(null)];
  const vals = data.map(v => v.map((d, i) => toTimestamp(d, us[i])));
  return loadData(data, 'timestampMicrosecond', vals);
}

export function timestampMillisecond() {
  const ts = ['1992-09-20T11:30:00.123Z', '2002-12-13T07:28:56.565Z'];
  const data = [ts, ts.concat(null)];
  const vals = data.map(v => v.map(d => toTimestamp(d)));
  return loadData(data, 'timestampMillisecond', vals);
}

export function timestampSecond() {
  const ts = ['1992-09-20T11:30:00Z', '2002-12-13T07:28:57Z'];
  const data = [ts, ts.concat(null)];
  const vals = data.map(v => v.map(d => toTimestamp(d)));
  return loadData(data, 'timestampSecond', vals);
}

export function intervalMonthDayNano() {
  return loadData([
    ['2 years', null, '12 years 2 month 1 day 5 seconds', '1 microsecond']
  ], 'intervalMonthDayNano', [[
    Float64Array.of(24, 0, 0),
    null,
    Float64Array.of(146, 1, 5000000000),
    Float64Array.of(0, 0, 1000)
  ]]);
}

export function utf8() {
  return loadData([
    ['foo', 'bar', 'baz'],
    ['foo', null, 'baz']
  ], 'utf8');
}

export function listInt32() {
  const data = [
    [[1, 2, 3, 4], [5, 6], [7, 8, 9]],
    [[1, 2, 3, 4], null, [7, 8, 9]],
    [[1, 2, null, 4], [5, null, 6], [7, null, 9]]
  ];
  const vals = data.map(v => v.map(toInt32s));
  return loadData(data, 'listInt32', vals);
}

export function listUtf8() {
  return loadData([
    [['a', 'b', 'c', 'd'], ['e', 'f'], ['g', 'h', 'i']],
    [['a', 'b', 'c', 'd'], null, ['g', 'h', 'i']],
    [['a', 'b', null, 'd'], ['e', null, 'f'], ['g', null, 'i']]
  ], 'listUtf8');
}

export function fixedListInt32() {
  const data = [
    [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
    [[1, 2, 3], null, [7, 8, 9]],
    [[1, null, 3], [null, 5, 6], [7, 8, null]]
  ];
  const vals = data.map(v => v.map(toInt32s));
  return loadData(data, 'fixedListInt32', vals);
}

export function fixedListUtf8() {
  return loadData([
    [['a', 'b', 'c'], ['d', 'e', 'f'], ['g', 'h', 'i']],
    [['a', 'b', 'c'], null, ['g', 'h', 'i']],
    [['a', null, 'c'], [null, 'e', 'f'], ['g', 'h', null]]
  ], 'fixedListUtf8');
}

export function union() {
  return loadData([
    ['a', 2, 'c'],
    ['a', null, 'c']
  ], 'union');
}

export function map() {
  return loadData([
    [
      new Map([ ['foo', 1], ['bar', 2] ]),
      new Map([ ['foo', null], ['baz', 3] ])
    ]
  ], 'map');
}

export function struct() {
  return loadData([
    [ {a: 1, b: 'foo'}, {a: 2, b: 'baz'} ],
    [ {a: 1, b: 'foo'}, null, {a: 2, b: 'baz'} ],
    [ {a: null, b: 'foo'}, {a: 2, b: null} ],
    [ {a: ['a', 'b'], b: Math.E}, {a: ['c', 'd'], b: Math.PI} ]
  ], 'struct');
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
