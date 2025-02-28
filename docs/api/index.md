---
title: API Reference
---
# Flechette API Reference <a href="https://idl.uw.edu/flechette"><img align="right" src="../assets/logo.svg" height="38"/></a>

[**Top-Level**](/flechette/api) | [Data Types](data-types) | [Schema](schema) | [Table](table) | [Column](column)

## Top-Level Decoding and Encoding

* [tableFromIPC](#tableFromIPC)
* [tableToIPC](#tableToIPC)
* [tableFromArrays](#tableFromArrays)
* [columnFromArray](#columnFromArray)
* [columnFromValues](#columnFromValues)
* [tableFromColumns](#tableFromColumns)

<hr/><a id="tableFromIPC" href="#tableFromIPC">#</a>
<b>tableFromIPC</b>(<i>data</i>[, <i>options</i>])

Decode [Apache Arrow IPC data](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc) and return a new [`Table`](table). The input binary data may be either an `ArrayBuffer` or `Uint8Array`. For Arrow data in the [IPC 'stream' format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format), an array of `Uint8Array` values is also supported.

* *data* (`ArrayBuffer` \| `Uint8Array` \| `Uint8Array[]`): The source byte buffer, or an array of buffers. If an array, each byte array may contain one or more self-contained messages. Messages may NOT span multiple byte arrays.
* *options* (`ExtractionOptions`): Options for controlling how values are transformed when extracted from an Arrow binary representation.
  * *useBigInt* (`boolean`): If true, extract 64-bit integers as JavaScript `BigInt` values. Otherwise, coerce long integers to JavaScript number values (default `false`).
  * *useDate* (`boolean`): If true, extract dates and timestamps as JavaScript `Date` objects. Otherwise, return numerical timestamp values (default `false`).
  * *useDecimalInt* (`boolean`): If true, extract decimal-type data as scaled integer values, where fractional digits are scaled to integer positions. Returned integers are `BigInt` values for decimal bit widths of 64 bits or higher and 32-bit integers (as JavaScript `number`) otherwise. If false, decimals are converted to floating-point numbers (default).
  * *useMap* (`boolean`): If true, extract Arrow 'Map' values as JavaScript `Map` instances. Otherwise, return an array of [key, value] pairs compatible with both `Map` and `Object.fromEntries` (default `false`).
  * *useProxy* (`boolean`): If true, extract Arrow 'Struct' values and table row objects using zero-copy proxy objects that extract data from underlying Arrow batches. The proxy objects can improve performance and reduce memory usage, but do not support property enumeration (`Object.keys`, `Object.values`, `Object.entries`) or spreading (`{ ...object }`). Otherwise, use standard JS objects for structs and table rows (default `false`).

```js
import { tableFromIPC } from '@uwdata/flechette';
const url = 'https://vega.github.io/vega-datasets/data/flights-200k.arrow';
const ipc = await fetch(url).then(r => r.arrayBuffer());
const table = tableFromIPC(ipc);
```

<hr/><a id="tableToIPC" href="#tableToIPC">#</a>
<b>tableToIPC</b>(<i>table</i>[, <i>options</i>])

Encode an Arrow table into Arrow IPC binary format and return the result as a `Uint8Array`. Both the IPC ['stream'](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format) and ['file'](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format) formats are supported.

* *table* (`Table`): The Arrow table to encode.
* *options* (`object`): Encoding options object.
  * *format* (`string`): Arrow `'stream'` (the default) or `'file'` format.

```js
import { tableToIPC } from '@uwdata/flechette';
const bytes = tableFromIPC(table, { format: 'stream' });
```

<hr/><a id="tableFromArrays" href="#tableFromArrays">#</a>
<b>tableFromArrays</b>(<i>data</i>[, <i>options</i>])

Create a new table from a set of named arrays. Data types for the resulting Arrow columns can be automatically inferred or specified using the *types* option. If the *types* option provides data types for only a subset of columns, the rest are inferred. Each input array must have the same length.

* *data* (`object | array`): The input data as a collection of named arrays. If object-valued, the objects keys are column names and the values are arrays or typed arrays. If array-valued, the data should consist of [name, array] pairs in the style of `Object.entries`.
* *options* (`object`): Options for building new tables and controlling how values are transformed when extracted from an Arrow binary representation.
  * *types*: (`object`): An object mapping column names to [data types](data-types).
  * *maxBatchRows* (`number`): The maximum number of rows to include in a single record batch. If the array lengths exceed this number, the resulting table will consist of multiple record batches.
  * In addition, all [tableFromIPC](#tableFromIPC) extraction options are supported.

```js
import { tableFromArrays } from '@uwdata/flechette';

// create table with inferred types
const table = tableFromArrays({
  ints: [1, 2, null, 4, 5],
  floats: [1.1, 2.2, 3.3, 4.4, 5.5],
  bools: [true, true, null, false, true],
  strings: ['a', 'b', 'c', 'b', 'a']
});
```

```js
import {
  bool, dictionary, float32, int32, tableFromArrays, tableToIPC, utf8
} from '@uwdata/flechette';

// create table with specified types
const table = tableFromArrays({
  ints: [1, 2, null, 4, 5],
  floats: [1.1, 2.2, 3.3, 4.4, 5.5],
  bools: [true, true, null, false, true],
  strings: ['a', 'b', 'c', 'b', 'a']
}, {
  types: {
    ints: int32(),
    floats: float32(),
    bools: bool(),
    strings: dictionary(utf8())
  }
});
```

<hr/><a id="columnFromArray" href="#columnFromArray">#</a>
<b>columnFromArray</b>(<i>array</i>[, <i>type</i>, <i>options</i>])

Create a new column from a provided data array. The data types for the column can be automatically inferred or specified using the *type* argument.

* *array* (`Array | TypedArray`): The input data as an Array or TypedArray.
* *type*: (`DataType`): The [data type](data-types) for the column. If not specified, type inference is attempted.
* *options* (`object`): Options for building new columns and controlling how values are transformed when extracted from an Arrow binary representation.
  * *maxBatchRows* (`number`): The maximum number of rows to include in a single record batch. If the array length exceeds this number, the resulting column will consist of multiple record batches.
  * In addition, all [tableFromIPC](#tableFromIPC) extraction options are supported.

```js
import { columnFromArray, float32, int64 } from '@uwdata/flechette';

// create column with inferred type (here, float64)
columnFromArray([1.1, 2.2, 3.3, 4.4, 5.5]);

// create column with specified type
columnFromArray([1.1, 2.2, 3.3, 4.4, 5.5], float32());

// create column with specified type and options
columnFromArray(
  [1n, 32n, 2n << 34n], int64(),
  { maxBatchRows: 1000, useBigInt: true }
);
```

<hr/><a id="columnFromValues" href="#columnFromValues">#</a>
<b>columnFromValues</b>(<i>values</i>[, <i>type</i>, <i>options</i>])

Create a new column by iterating over provided *values*. The *values* argument can either be an iterable collection or a visitor function that applies a callback to successive data values (akin to `Array.forEach`). The data types for the column can be automatically inferred or specified using the *type* argument.

* *values* (`Iterable | (callback: (value: any) => void) => void`): An iterable object or a visitor function that applies a callback to successive data values (akin to `Array.forEach`). In most cases, providing a visitor function will be more efficient than an iterator, so is recommended for larger datasets.
* *type*: (`DataType`): The [data type](data-types) for the column. If not specified, type inference is attempted.
* *options* (`object`): Options for building new columns and controlling how values are transformed when extracted from an Arrow binary representation.
  * *maxBatchRows* (`number`): The maximum number of rows to include in a single record batch. If the number of values exceeds this number, the resulting column will consist of multiple record batches.
  * In addition, all [tableFromIPC](#tableFromIPC) extraction options are supported.

```js
import { columnFromValues, float32, int64 } from '@uwdata/flechette';

// an iterable set of values
const set = new Set([1.1, 1.1, 2.2, 3.4]);

// create column with inferred type (here, float64)
columnFromValues(set);

// create column with specified type
columnFromValues(set, float32());

// create column using only values with odd-numbered indices
const values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
columnFromValues(callback => {
  values.forEach((v, i) => { if (i % 2) callback(v); })
});
```

<hr/><a id="tableFromColumns" href="#tableFromColumns">#</a>
<b>tableFromColumns</b>(<i>columns</i>[, <i>useProxy</i>])

Create a new table from a collection of columns. This method is useful for creating new tables using one or more pre-existing column instances. Otherwise, [`tableFromArrays`](#tableFromArrays) should be preferred. Input columns are assumed to have the same record batch sizes.

* *data* (`object | array`): The input columns as an object with name keys, or an array of [name, column] pairs.
* *useProxy* (`boolean`): Flag indicating if row proxy objects should be used to represent table rows (default `false`). Typically this should match the value of the `useProxy` extraction option used for column generation.

```js
import { columnFromArray, tableFromColumns } from '@uwdata/flechette';

// create table from columns with inferred types (here, bool and float64)
const table = tableFromColumns({
  bools: columnFromArray([true, true, null, false, true]),
  floats: columnFromArray([1.1, 2.2, 3.3, 4.4, 5.5])
});
```
