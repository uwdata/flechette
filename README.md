# Flechette

**Flechette** is a JavaScript library for reading the [Apache Arrow](https://arrow.apache.org/) columnar in-memory data format. It provides a faster, lighter, zero-dependency alternative to the [Arrow JS reference implementation](https://github.com/apache/arrow/tree/main/js).

Flechette performs fast extraction of data columns in the Arrow binary IPC format, supporting ingestion of Arrow data (from sources such as [DuckDB](https://duckdb.org/)) for downstream use in JavaScript data analysis tools like [Arquero](https://github.com/uwdata/arquero), [Mosaic](https://github.com/uwdata/mosaic), [Observable Plot](https://observablehq.com/plot/), and [Vega-Lite](https://vega.github.io/vega-lite/).

## Why Flechette?

In the process of developing multiple data analysis packages that consume Arrow data (including Arquero, Mosaic, and Vega), we've had to develop workarounds for the performance and correctness of the Arrow JavaScript reference implementation. Instead of workarounds, Flechette addresses these issues head-on.

* _Speed_. Flechette provides faster decoding. Initial performance tests show 1.3-1.6x faster value iteration, 2-2.5x faster random value access, 2-7x faster array extraction, and 7-11x faster row object extraction.

* _Size_. Flechette is ~17k minified (~6k gzip'd), versus 163k minified (~43k gzip'd) for Arrow JS.

* _Coverage_. Flechette supports data types unsupported by the reference implementation at the time of writing, including decimal-to-number conversion, month/day/nanosecond time intervals (as used by DuckDB, for example), list views, and run-end encoded data.

* _Flexibility_. Flechette includes options to control data value conversion, such as numerical timestamps vs. Date objects for temporal data, numbers vs. bigint values for 64-bit integer data, and vanilla JS objects vs. optimized proxy objects for structs.

* _Simplicity_. Our goal is to provide a smaller, simpler code base in the hope that it will make it easier for ourselves and others to improve the library. If you'd like to see support for additional Arrow data types or features, please [file an issue](https://github.com/uwdata/flechette/issues) or [open a pull request](https://github.com/uwdata/flechette/pulls).

That said, no tool is without limitations or trade-offs. Flechette is *consumption oriented*: it does yet support encoding (though feel free to [upvote encoding support](https://github.com/uwdata/flechette/issues/1)!). Flechette also requires simpler inputs (byte buffers, no promises or streams), has less strict TypeScript typings, and at times has a slightly slower initial parse (as it decodes dictionary data upfront for faster downstream access).

## What's with the name?

The project name stems from the French word [fléchette](https://en.wikipedia.org/wiki/Flechette), which means "little arrow" or "dart". 🎯

## Examples

### Load and Access Arrow Data

```js
import { tableFromIPC } from '@uwdata/flechette';

const url = 'https://vega.github.io/vega-datasets/data/flights-200k.arrow';
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

// convert Arrow data to an array of JS objects
// [ { delay: 14, distance: 405, time: 0.01666666753590107 }, ... ]
const objects = table.toArray();

// create a new table with a selected subset of columns
// use this first to limit toColumns or toArray to fewer columns
const subtable = table.select(['delay', 'time']);
```

### Customize Data Extraction

Data extraction can be customized using options provided to the table generation method. By default, temporal data is returned as numeric timestamps, 64-bit integers are coerced to numbers, map-typed data is returned as an array of [key, value] pairs, and struct/row objects are returned as vanilla JS objects with extracted property values. These defaults can be changed via conversion options that push (or remove) transformations to the underlying data batches.

```js
const table = tableFromIPC(ipc, {
  useDate: true,   // use Date objects for temporal (Date, Timestamp) data
  useBigInt: true, // use BigInt for large integers, do not coerce to number
  useMap: true,    // use Map objects for [key, value] pair lists
  useProxy: true   // use zero-copy proxies for struct and table row objects
});
```

## Build Instructions

To build and develop Flechette locally:

- Clone https://github.com/uwdata/flechette.
- Run `npm i` to install dependencies.
- Run `npm test` to run test cases, `npm run perf` to run performance benchmarks, and `npm run build` to build output files.
