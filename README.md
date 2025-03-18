# Flechette <a href="https://idl.uw.edu/flechette"><img align="right" src="https://raw.githubusercontent.com/uwdata/flechette/main/docs/assets/logo.svg" height="38"></img></a>

**Flechette** is a JavaScript library for reading and writing the [Apache Arrow](https://arrow.apache.org/) columnar in-memory data format. It provides a faster, lighter, zero-dependency alternative to the [Arrow JS reference implementation](https://github.com/apache/arrow/tree/main/js).

Flechette performs fast extraction and encoding of data columns in the Arrow binary IPC format, supporting ingestion of Arrow data from sources such as [DuckDB](https://duckdb.org/) and Arrow use in JavaScript data analysis tools like [Arquero](https://github.com/uwdata/arquero), [Mosaic](https://github.com/uwdata/mosaic), [Observable Plot](https://observablehq.com/plot/), and [Vega-Lite](https://vega.github.io/vega-lite/).

For documentation, see the [**API Reference**](https://idl.uw.edu/flechette/api). For code, see the [**Flechette GitHub repo**](https://github.com/uwdata/flechette).

## Why Flechette?

In the process of developing multiple data analysis packages that consume Arrow data (including Arquero, Mosaic, and Vega), we've had to develop workarounds for the performance and correctness of the Arrow JavaScript reference implementation. Instead of workarounds, Flechette addresses these issues head-on.

* _Speed_. Flechette provides better performance. Performance tests show 1.3-1.6x faster value iteration, 2-7x faster array extraction, 7-11x faster row object extraction, and 1.5-3.5x faster building of Arrow columns.

* _Size_. Flechette is smaller: ~43k minified (~14k gzip'd) versus 163k minified (~43k gzip'd) for Arrow JS. Flechette's encoders and decoders also tree-shake cleanly, so you only pay for what you need in custom bundles.

* _Coverage_. Flechette supports data types unsupported by the reference implementation, including decimal-to-number conversion, month/day/nanosecond time intervals (as used by DuckDB), run-end encoded data, binary views, and list views.

* _Flexibility_. Flechette includes options to control data value conversion, such as numerical timestamps vs. Date objects for temporal data, and numbers vs. bigint values for 64-bit integer data.

* _Simplicity_. Our goal is to provide a smaller, simpler code base in the hope that it will make it easier for ourselves and others to improve the library. If you'd like to see support for additional Arrow features, please [file an issue](https://github.com/uwdata/flechette/issues) or [open a pull request](https://github.com/uwdata/flechette/pulls).

That said, no tool is without limitations or trade-offs. Flechette assumes simpler inputs (byte buffers, no promises or streams), has less strict TypeScript typings, and may have a slightly slower initial parse (as it decodes dictionary data upfront for faster downstream access).

## What's with the name?

The project name stems from the French word [fléchette](https://en.wikipedia.org/wiki/Flechette), which means "little arrow" or "dart". 🎯

## Examples

### Load and Access Arrow Data

```js
import { tableFromIPC } from '@uwdata/flechette';

const url = 'https://cdn.jsdelivr.net/npm/vega-datasets@2/data/flights-200k.arrow';
const ipc = await fetch(url).then(r => r.arrayBuffer());
const table = tableFromIPC(ipc);

// print table size: (231083 x 3)
console.log(`${table.numRows} x ${table.numCols}`);

// inspect schema for column names, data types, etc.
// [
//   { name: "delay", type: { typeId: 2, bitWidth: 16, signed: true }, ...},
//   { name: "distance", type: { typeId: 2, bitWidth: 16, signed: true }, ...},
//   { name: "time", type: { typeId: 3, precision: 1 }, ...}
// ]
// typeId: 2 === Type.Int, typeId: 3 === Type.Float
console.log(JSON.stringify(table.schema.fields, 0, 2));

// convert a single Arrow column to a value array
// when possible, zero-copy access to binary data is used
const delay = table.getChild('delay').toArray();

// data columns are iterable
const time = [...table.getChild('time')];

// data columns provide random access
const time0 = table.getChild('time').at(0);

// extract all columns into a { name: array, ... } object
// { delay: Int16Array, distance: Int16Array, time: Float32Array }
const columns = table.toColumns();

// convert Arrow data to an array of standard JS objects
// [ { delay: 14, distance: 405, time: 0.01666666753590107 }, ... ]
const objects = table.toArray();

// create a new table with a selected subset of columns
// use this first to limit toColumns or toArray to fewer columns
const subtable = table.select(['delay', 'time']);
```

### Build and Encode Arrow Data

```js
import {
  bool, dictionary, float32, int32, tableFromArrays, tableToIPC, utf8
} from '@uwdata/flechette';

// data defined using standard JS types
// both arrays and typed arrays work well
const arrays = {
  ints: [1, 2, null, 4, 5],
  floats: [1.1, 2.2, 3.3, 4.4, 5.5],
  bools: [true, true, null, false, true],
  strings: ['a', 'b', 'c', 'b', 'a']
};

// create table with automatically inferred types
const tableInfer = tableFromArrays(arrays);

// encode table to bytes in Arrow IPC stream format
const ipcInfer = tableToIPC(tableInfer);

// create table using explicit types
const tableTyped = tableFromArrays(arrays, {
  types: {
    ints: int32(),
    floats: float32(),
    bools: bool(),
    strings: dictionary(utf8())
  }
});

// encode table to bytes in Arrow IPC file format
const ipcTyped = tableToIPC(tableTyped, { format: 'file' });
```

### Customize Data Extraction

Data extraction can be customized using options provided to table generation methods. By default, temporal data is returned as numeric timestamps, 64-bit integers are coerced to numbers, map-typed data is returned as an array of [key, value] pairs, and struct/row objects are returned as vanilla JS objects with extracted property values. These defaults can be changed via conversion options that push (or remove) transformations to the underlying data batches.

```js
const table = tableFromIPC(ipc, {
  useDate: true,          // map dates and timestamps to Date objects
  useDecimalInt: true,    // use BigInt for decimals, do not coerce to number
  useBigInt: true,        // use BigInt for 64-bit ints, do not coerce to number
  useMap: true,           // create Map objects for [key, value] pair lists
  useProxy: true          // use zero-copy proxies for struct and table row objects
});
```

The same extraction options can be passed to `tableFromArrays`. For more, see the [**API Reference**](https://idl.uw.edu/flechette/api).

## Build Instructions

To build and develop Flechette locally:

- Clone https://github.com/uwdata/flechette.
- Run `npm i` to install dependencies.
- Run `npm test` to run test cases, `npm run perf` to run performance benchmarks, and `npm run build` to build output files.
