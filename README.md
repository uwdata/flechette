# Flechette

**Flechette** is a JavaScript library for reading [Apache Arrow](https://arrow.apache.org/) data. It provides a faster, lighter, zero-dependency alternative to the Apache Arrow [JavaScript reference implementation](https://github.com/apache/arrow/tree/main/js).

Flechette provides fast extraction of data in the Arrow binary IPC format, supporting ingestion of Arrow data (from sources such as [DuckDB](https://duckdb.org/)) for downstream use in JavaScript data analysis tools like [Arquero](https://github.com/uwdata/arquero), [Mosaic](https://github.com/uwdata/mosaic), [Observable Plot](https://observablehq.com/plot/), and [Vega-Lite](https://vega.github.io/vega-lite/).

## Why Flechette?

In the process of developing multiple data analysis packages that consume Apache Arrow data (including Arquero, Mosaic, and Vega), we had to develop workarounds for the performance and correctness of the Apache Arrow JavaScript reference implementation. Instead of workarounds, Flechette seeks to address these issues head-on.

* _Speed_. Flechette provides faster decoding. Across varied datasets, our initial performance tests find that Flechette provides 1.3-1.6x faster value iteration, 2-7x faster array extraction, and 5-9x faster row object extraction.

* _Size_. Flechette is ~16k minified (~6k gzip'd), versus 163k minified (~43k gzip'd) for Arrow JS.

* _Coverage_. At the time of writing, Flechette supports multiple data types unsupported by the reference implementation, including decimal-to-number conversion and support for month/day/nanosecond time intervals (as used, for example, by DuckDB).

* _Flexibility_. Flechette includes options to control data value conversion, such as numerical timestamps vs. Date objects for temporal data, and numbers vs. bigint values for 64-bit integer data.

* _Simplicity_. Our goal is to provide a smaller, simpler code base in the hope that it will make it easier for ourselves and others to improve the library. If you'd like to see support for additional Arrow data types or features, please [file an issue](https://github.com/uwdata/flechette/issues) or [open a pull request](https://github.com/uwdata/flechette/pulls).

That said, no tool is without limitations or trade-offs. Flechette is *consumption oriented*: it assumes Arrow data is generated elsewhere and then needs to be transformed and/or visualized in JavaScript. It does yet support encoding, though please [upvote encoding support](https://github.com/uwdata/flechette/issues/1) if you would use it. In addition, Flechette requires simpler inputs (byte buffers, no promises or streams), currently has less comprehensive TypeScript typings, and can have a slightly slower initial parse (as Flechette decodes dictionary-encoded data upfront for faster downstream access).

## What's with the name?

The project name stems from the French word [fléchette](https://en.wikipedia.org/wiki/Flechette), which means a "little arrow" or "dart". 🎯

## Examples

### Load and Access Arrow Data

```js
import { tableFromIPC } from 'flechette';

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

// convert Arrow data to an array of standard JS objects
// [ { delay: 14, distance: 405, time: 0.01666666753590107 }, ... ]
const objects = table.toArray();
```

### Customize Data Extraction

Data extraction can be customized using options provided to the table generation method. By default, temporal data is returned as numeric timestamps, 64-int integers are coerced to numbers, and map-typed data is returned as an array of [key, value] pairs. These defaults can be changed via conversion options that push (or remove) transformations to the underlying data batches.

```js
const table = tableFromIPC(ipc, {
  useDate: true,   // map temporal data to Date objects
  useBigInt: true, // use BigInt, do not coerce to number
  useMap: true     // create Map objects for [key, value] pair lists
});
```

## Build Instructions

To build and develop Flechette locally:

- Clone https://github.com/uwdata/flechette.
- Run `npm i` to install dependencies.
- Run `npm test` to run test cases, `npm run perf` to run performance benchmarks, and `npm run build` to build output files.
