---
title: Data Types | API Reference
---
# Flechette API Reference <a href="https://idl.uw.edu/flechette"><img align="right" src="../assets/logo.svg" height="38"/></a>

[Top-Level](/flechette/api) | [**Data Types**](data-types) | [Schema](schema) | [Table](table) | [Column](column)

## Data Type Overview

The table below provides an overview of all data types supported by the Apache Arrow format and how Flechette maps them to JavaScript types. The table indicates if Flechette can read the type (via [`tableFromIPC`](/flechette/api/#tableFromIPC)), write the type (via [`tableToIPC`](/flechette/api/#tableToIPC)), and build the type from JavaScript values (via [`tableFromArrays`](/flechette/api/#tableFromArrays) or [`columnFromArray`](/flechette/api/#columnFromArray)).

| Id  | Data Type                           | Read? | Write? | Build? | JavaScript Type |
| --: | ----------------------------------- | :---: | :----: | :----: | --------------- |
|  -1 | [Dictionary](#dictionary)           | ✅ | ✅ | ✅ | depends on dictionary value type |
|   1 | [Null](#null)                       | ✅ | ✅ | ✅ | `null` |
|   2 | [Int](#int)                         | ✅ | ✅ | ✅ | `number`, or `bigint` for 64-bit values via the `useBigInt` flag |
|   3 | [Float](#float)                     | ✅ | ✅ | ✅ | `number` |
|   4 | [Binary](#binary)                   | ✅ | ✅ | ✅ | `Uint8Array` |
|   5 | [Utf8](#utf8)                       | ✅ | ✅ | ✅ | `string` |
|   6 | [Bool](#bool)                       | ✅ | ✅ | ✅ | `boolean` |
|   7 | [Decimal](#decimal)                 | ✅ | ✅ | ✅ | `number`, or scaled integers via the `useDecimalInt` flag |
|   8 | [Date](#date)                       | ✅ | ✅ | ✅ | `number`, or `Date` via the `useDate` flag. |
|   9 | [Time](#time)                       | ✅ | ✅ | ✅ | `number`, or `bigint` for 64-bit values via the `useBigInt` flag |
|  10 | [Timestamp](#timestamp)             | ✅ | ✅ | ✅ | `number`, or `Date` via the `useDate` flag. |
|  11 | [Interval](#interval)               | ✅ | ✅ | ✅ | depends on the interval unit |
|  12 | [List](#list)                       | ✅ | ✅ | ✅ | `Array` or `TypedArray` of child type |
|  13 | [Struct](#struct)                   | ✅ | ✅ | ✅ | `object`, properties depend on child types |
|  14 | [Union](#union)                     | ✅ | ✅ | ✅ | depends on child types |
|  15 | [FixedSizeBinary](#fixedSizeBinary) | ✅ | ✅ | ✅ | `Uint8Array` |
|  16 | [FixedSizeList](#fixedSizeList)     | ✅ | ✅ | ✅ | `Array` or `TypedArray` of child type |
|  17 | [Map](#map)                         | ✅ | ✅ | ✅ | `[key, value][]`, or `Map` via the `useMap` flag |
|  18 | [Duration](#duration)               | ✅ | ✅ | ✅ | `number`, or `bigint` via the `useBigInt` flag |
|  19 | [LargeBinary](#largeBinary)         | ✅ | ✅ | ✅ | `Uint8Array` |
|  20 | [LargeUtf8](#largeUtf8)             | ✅ | ✅ | ✅ | `string` |
|  21 | [LargeList](#largeList)             | ✅ | ✅ | ✅ | `Array` or `TypedArray` of child type |
|  22 | [RunEndEncoded](#runEndEncoded)     | ✅ | ✅ | ✅ | depends on child type |
|  23 | [BinaryView](#binaryView)           | ✅ | ✅ | ❌ | `Uint8Array` |
|  24 | [Utf8View](#utf8View)               | ✅ | ✅ | ❌ | `string` |
|  25 | [ListView](#listView)               | ✅ | ✅ | ❌ | `Array` or `TypedArray` of child type |
|  26 | [LargeListView](#largeListView)     | ✅ | ✅ | ❌ | `Array` or `TypedArray` of child type |

## Data Type Methods

Each of the methods below returns a `DataType` instance as a standard JavaScript object. Data types are used to define a `Field` within a table `Schema`. See the [Schema documentation](./schema) for more.

* [dictionary](#dictionary)
* [nullType](#nullType)
* [int](#int), [int8](#int8), [int16](#int16), [int32](#int32), [int64](#int64), [uint8](#uint8), [uint16](#uint16), [uint32](#uint32), [uint64](#uint64)
* [float](#float), [float16](#float16), [float32](#float32), [float64](#float64)
* [binary](#binary)
* [utf8](#utf8)
* [bool](#bool)
* [decimal](#decimal), [decimal32](#decimal32), [decimal64](#decimal64), [decimal128](#decimal128), [decimal256](#decimal256)
* [date](#date), [dateDay](#dateDay), [dateMillisecond](#dateMillisecond)
* [time](#time), [timeSecond](#timeSecond), [timeMillisecond](#timeMillisecond), [timeMicrosecond](#timeMicrosecond), [timeNanosecond](#timeNanosecond)
* [timestamp](#timestamp)
* [interval](#interval)
* [list](#list)
* [struct](#struct)
* [union](#struct)
* [fixedSizeBinary](#fixedSizeBinary)
* [fixedSizeList](#fixedSizeList)
* [map](#map)
* [duration](#duration)
* [largeBinary](#largeBinary)
* [largeUtf8](#largeUtf8)
* [largeList](#largeList)
* [runEndEncoded](#runEndEncoded)
* [binaryView](#binaryView)
* [utf8View](#utf8View)
* [listView](#listView)
* [largeListView](#largeListView)

### Dictionary

<hr/><a id="dictionary" href="#dictionary">#</a>
<b>dictionary</b>(<i>type</i>[, <i>indexType</i>, <i>ordered</i>, <i>id</i>])

Create a Dictionary data type instance. A dictionary type consists of a dictionary of values (which may be of any type) and corresponding integer indices that reference those values. If values are repeated, a dictionary encoding can provide substantial space savings. In the IPC format, dictionary indices reside alongside other columns in a record batch, while dictionary values are written to special dictionary batches, linked by a unique dictionary *id* assigned at encoding time. Internally Flechette extracts dictionary values immediately upon decoding; while this incurs some initial overhead, it enables fast subsequent lookups.

* *type* (`DataType`): The data type of dictionary values.
* *indexType* (`DataType`): The data type of dictionary indices. Must be an integer type (default [`int32`](#int32)).
* *ordered* (`boolean`): Indicates if dictionary values are ordered (default `false`).
* *id* (`number`): Optional dictionary id. The default value (-1) indicates that the dictionary applies to a single column only. Provide an explicit id in order to reuse a dictionary across columns when building, in which case different dictionaries *must* have different unique ids. All dictionary ids are later resolved (possibly to new values) upon IPC encoding.

```js
import { dictionary, int16, utf8 } from '@uwdata/flechette';
// dictionary type with string values and int16 indices
// {
//   typeId: -1,
//   id: -1,
//   dictionary: { typeId: 5, ... },
//   indices: { typeId: 2, bitWidth: 16, signed: true, ... }
//   ordered: false
// }
dictionary(utf8(), int16())
```

### Null

<hr/><a id="nullType" href="#nullType">#</a>
<b>nullType</b>()

Create a Null data type instance. Null data requires no storage and all extracted values are `null`.

```js
import { nullType } from '@uwdata/flechette';
// { typeId: 1 }
nullType()
```

### Int

<hr/><a id="int" href="#int">#</a>
<b>int</b>([<i>bitWidth</i>, <i>signed</i>])

Create an Int data type instance. Integer values are stored within typed arrays and extracted to JavaScript `number` values by default.

* *bitWidth* (`number`): The integer bit width, must be `8`, `16`, `32` (default), or `64`.
* *signed* (`boolean`): Flag for signed or unsigned integers (default `true`).

```js
import { int } from '@uwdata/flechette';
// { typeId: 2, bitWidth: 32, signed: true, ... }
int()
```

<hr/><a id="int8" href="#int8">#</a>
<b>int8</b>()

Create an Int data type instance for 8-bit signed integers. 8-bit signed integers are stored within an `Int8Array` and accessed directly.

<hr/><a id="int16" href="#int16">#</a>
<b>int16</b>()

Create an Int data type instance for 16-bit signed integers. 16-bit signed integers are stored within an `Int16Array` and accessed directly.

<hr/><a id="int32" href="#int32">#</a>
<b>int32</b>()

Create an Int data type instance for 32-bit signed integers. 32-bit signed integers are stored within an `Int32Array` and accessed directly.

<hr/><a id="int64" href="#int64">#</a>
<b>int64</b>()

Create an Int data type instance for 64-bit signed integers. 64-bit signed integers are stored within a `BigInt64Array` and converted to JavaScript `number` values upon extraction. An error is raised if a value exceeds either `Number.MIN_SAFE_INTEGER` or `Number.MAX_SAFE_INTEGER`. Pass the `useBigInt` extraction option (e.g., to [`tableFromIPC`](/flechette/api/#tableFromIPC) or [`tableFromArrays`](/flechette/api/#tableFromArrays)) to instead extract 64-bit integers directly as `BigInt` values.

<hr/><a id="uint8" href="#uint8">#</a>
<b>uint8</b>()

Create an Int data type instance for 8-bit unsigned integers. 8-bit unsigned integers are stored within an `Uint8Array` and accessed directly.

<hr/><a id="uint16" href="#uint16">#</a>
<b>uint16</b>()

Create an Int data type instance for 16-bit unsigned integers. 16-bit unsigned integers are stored within an `Uint16Array` and accessed directly.

<hr/><a id="uint32" href="#uint32">#</a>
<b>uint32</b>()

Create an Int data type instance for 32-bit unsigned integers. 32-bit unsigned integers are stored within an `Uint32Array` and accessed directly.

<hr/><a id="uint64" href="#uint64">#</a>
<b>uint64</b>()

Create an Int data type instance for 64-bit unsigned integers. 64-bit unsigned integers are stored within a `BigUint64Array` and converted to JavaScript `number` values upon extraction. An error is raised if a value exceeds `Number.MAX_SAFE_INTEGER`. Pass the `useBigInt` extraction option (e.g., to [`tableFromIPC`](/flechette/api/#tableFromIPC) or [`tableFromArrays`](/flechette/api/#tableFromArrays)) to instead extract 64-bit integers directly as `BigInt` values.

### Float

<hr/><a id="float" href="#float">#</a>
<b>float</b>([<i>precision</i>])

Create a Float data type instance for floating point numbers. Floating point values are stored within typed arrays and extracted to JavaScript `number` values.

* *precision* (`number`): The floating point precision, one of `Precision.HALF` (16-bit), `Precision.SINGLE` (32-bit) or `Precision.DOUBLE` (64-bit, default).

```js
import { float } from '@uwdata/flechette';
// { typeId: 3, precision: 2, ... }
float()
```

<hr/><a id="float16" href="#float16">#</a>
<b>float16</b>()

Create a Float data type instance for 16-bit (half precision) floating point numbers. 16-bit floats are stored within a `Uint16Array` and converted to/from `number` values. We intend to use [`Float16Array`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Float16Array) once it is widespread among JavaScript engines.

<hr/><a id="float32" href="#float32">#</a>
<b>float32</b>()

Create a Float data type instance for 32-bit (single precision) floating point numbers. 32-bit floats are stored within a `Float32Array` and accessed directly.

<hr/><a id="float64" href="#float64">#</a>
<b>float64</b>()

Create a Float data type instance for 64-bit (double precision) floating point numbers. 64-bit floats are stored within a `Float64Array` and accessed directly.

### Binary

<hr/><a id="binary" href="#binary">#</a>
<b>binary</b>()

Create a Binary data type instance for variably-sized opaque binary data with 32-bit offsets. Binary values are stored in a `Uint8Array` using a 32-bit offset array and extracted to JavaScript `Uint8Array` subarray values.

```js
import { binary } from '@uwdata/flechette';
// { typeId: 4 }
binary()
```

### Utf8

<hr/><a id="utf8" href="#utf8">#</a>
<b>utf8</b>()

Create a Utf8 data type instance for Unicode string data of variable length with 32-bit offsets. [UTF-8](https://en.wikipedia.org/wiki/UTF-8) code points are stored as binary data and extracted to JavaScript `string` values using [`TextDecoder`](https://developer.mozilla.org/en-US/docs/Web/API/TextDecoder). Due to decoding overhead, repeated access to string data can be costly. If making multiple passes over Utf8 data, we recommended converting the string upfront (e.g., via [`Column.toArray`](column#toArray)) and accessing the result.

```js
import { utf8 } from '@uwdata/flechette';
// { typeId: 5 }
utf8()
```

### Bool

<hr/><a id="bool" href="#bool">#</a>
<b>bool</b>()

Create a Bool data type instance for boolean data. Bool values are stored compactly in `Uint8Array` bitmaps with eight values per byte, and extracted to JavaScript `boolean` values.

```js
import { bool } from '@uwdata/flechette';
// { typeId: 6 }
bool()
```

### Decimal

<hr/><a id="decimal" href="#decimal">#</a>
<b>decimal</b>(<i>precision</i>, <i>scale</i>[, <i>bitWidth</i>])

Create an Decimal data type instance for exact decimal values, represented as a 32, 64, 128, or 256-bit integer value in two's complement. Decimals are fixed point numbers with a set *precision* (total number of decimal digits) and *scale* (number of fractional digits). For example, the number `35.42` can be represented as `3542` with *precision* ≥ 4 and *scale* = 2.

By default, Flechette converts decimals to 64-bit floating point numbers upon extraction (e.g., mapping `3542` back to `35.42`). While useful for many downstream applications, this conversion may be lossy and introduce inaccuracies. Pass the `useDecimalInt` extraction option (e.g., to [`tableFromIPC`](/flechette/api/#tableFromIPC) or [`tableFromArrays`](/flechette/api/#tableFromArrays)) to instead extract decimal data as `BigInt` values (64-bit or larger decimals) or integer `number` values (32-bit decimals).

* *precision* (`number`): The total number of decimal digits that can be represented.
* *scale* (`number`): The number of fractional digits, beyond the decimal point.
* *bitWidth* (`number`): The decimal bit width, one of `32`, `64`, `128` (default) or `256`.

```js
import { decimal } from '@uwdata/flechette';
// decimal with 18 total digits, including 3 fractional digits
// { typeId: 7, precision: 18, scale: 3, bitWidth: 128, ... }
decimal(18, 3)
```

<hr/><a id="decimal32" href="#decimal32">#</a>
<b>decimal32</b>(<i>precision</i>, <i>scale</i>)

Create a Decimal data type instance that uses 32 bits per decimal. 32-bit decimals are stored within an `Int32Array`.

* *precision* (`number`): The total number of decimal digits that can be represented.
* *scale* (`number`): The number of fractional digits, beyond the decimal point.

<hr/><a id="decimal64" href="#decimal64">#</a>
<b>decimal64</b>(<i>precision</i>, <i>scale</i>)

Create a Decimal data type instance that uses 64 bits per decimal. 64-bit decimals are stored within a `Uint64Array`.

* *precision* (`number`): The total number of decimal digits that can be represented.
* *scale* (`number`): The number of fractional digits, beyond the decimal point.

<hr/><a id="decimal128" href="#decimal128">#</a>
<b>decimal128</b>(<i>precision</i>, <i>scale</i>)

Create a Decimal data type instance that uses 128 bits per decimal. 128-bit decimals are stored within a `Uint64Array` with a stride of 2 (two array entries per decimal value).

* *precision* (`number`): The total number of decimal digits that can be represented.
* *scale* (`number`): The number of fractional digits, beyond the decimal point.

<hr/><a id="decimal256" href="#decimal256">#</a>
<b>decimal256</b>(<i>precision</i>, <i>scale</i>)

Create a Decimal data type instance that uses 256 bits per decimal. 256-bit decimals are stored within a `Uint64Array` with a stride of 4 (four array entries per decimal value).

* *precision* (`number`): The total number of decimal digits that can be represented.
* *scale* (`number`): The number of fractional digits, beyond the decimal point.

### Date

<hr/><a id="date" href="#date">#</a>
<b>date</b>(<i>unit</i>)

Create a Date data type instance. Date values are 32-bit or 64-bit signed integers representing an elapsed time since the UNIX epoch (Jan 1, 1970 UTC), either in units of days (32 bits) or milliseconds (64 bits, with values evenly divisible by 86400000). Dates are stored in either an `Int32Array` (days) or `BigInt64Array` (milliseconds).

By default, extracted date values are converted to JavaScript `number` values representing milliseconds since the UNIX epoch. Pass the `useDate` extraction option (e.g., to [`tableFromIPC`](/flechette/api/#tableFromIPC) or [`tableFromArrays`](/flechette/api/#tableFromArrays)) to instead extract date values as JavaScript `Date` objects.

* *unit* (`number`): The date unit, one of `DateUnit.DAY` or `DateUnit.MILLISECOND`.

```js
import { DateUnit, date } from '@uwdata/flechette';
// { typeId: 8, unit: 0, ... }
date(DateUnit.DAY)
```

<hr/><a id="dateDay" href="#dateDay">#</a>
<b>dateDay</b>()

Create a Date data type instance with units of `DateUnit.DAY`.

```js
import { dateDay } from '@uwdata/flechette';
// { typeId: 8, unit: 0, ... }
dateDay()
```

<hr/><a id="dateMillisecond" href="#dateMillisecond">#</a>
<b>dateMillisecond</b>()

Create a Date data type instance with units of `DateUnit.MILLISECOND`.

```js
import { dateMillisecond } from '@uwdata/flechette';
// { typeId: 8, unit: 1, ... }
dateMillisecond()
```

### Time

<hr/><a id="time" href="#time">#</a>
<b>time</b>([<i>unit</i>, <i>bitWidth</i>])

Create a Time data type instance, stored in one of four *unit*s: seconds, milliseconds, microseconds or nanoseconds. The integer *bitWidth* depends on the *unit* and must be 32 bits for seconds and milliseconds or 64 bits for microseconds and nanoseconds. The allowed values are between 0 (inclusive) and 86400 (=24*60*60) seconds (exclusive), adjusted for the time unit (for example, up to 86400000 exclusive for the `DateUnit.MILLISECOND` unit.

This definition doesn't allow for leap seconds. Time values from measurements with leap seconds will need to be corrected when ingesting into Arrow (for example by replacing the value 86400 with 86399).

Time values are stored as integers in either an `Int32Array` (*bitWidth* = 32) or `BigInt64Array` (*bitWidth* = 64). By default, all time values are returned as untransformed `number` values. 64-bit values are stored within a `BigInt64Array` and converted to JavaScript `number` values upon extraction. An error is raised if a value exceeds either `Number.MIN_SAFE_INTEGER` or `Number.MAX_SAFE_INTEGER`. Pass the `useBigInt` extraction option (e.g., to [`tableFromIPC`](/flechette/api/#tableFromIPC) or [`tableFromArrays`](/flechette/api/#tableFromArrays)) to instead extract 64-bit time values directly as `BigInt` values.

* *unit* (`number`): The time unit, one of `TimeUnit.SECOND`, `TimeUnit.MILLISECOND` (default), `TimeUnit.MICROSECOND`, or `TimeUnit.NANOSECOND`.
* *bitWidth (`number`): The time bit width, one of `32` (for seconds and milliseconds) or `64` (for microseconds and nanoseconds).

```js
import { TimeUnit, time } from '@uwdata/flechette';
// { typeId: 9, unit: 1, bitWidth: 32, ... }
time()
// { typeId: 9, unit: 2, bitWidth: 64, ... }
time(TimeUnit.MICROSECONDS, 64)
```

<hr/><a id="timeSecond" href="#timeSecond">#</a>
<b>timeSecond</b>()

Create a Time data type instance with units of `TimeUnit.SECOND`.

```js
import { timeSecond } from '@uwdata/flechette';
// { typeId: 9, unit: 0, bitWidth: 32, ... }
timeSecond()
```

<hr/><a id="timeMillisecond" href="#timeMillisecond">#</a>
<b>timeMillisecond</b>()

Create a Time data type instance with units of `TimeUnit.MILLISECOND`.

```js
import { timeMillisecond } from '@uwdata/flechette';
// { typeId: 9, unit: 1, bitWidth: 32, ... }
timeMillisecond()
```

<hr/><a id="timeMicrosecond" href="#timeMicrosecond">#</a>
<b>timeMicrosecond</b>()

Create a Time data type instance with units of `TimeUnit.MICROSECOND`.

```js
import { timeMicrosecond } from '@uwdata/flechette';
// { typeId: 9, unit: 2, bitWidth: 64, ... }
timeMicrosecond()
```

<hr/><a id="timeNanosecond" href="#timeNanosecond">#</a>
<b>timeNanosecond</b>()

Create a Time data type instance with units of `TimeUnit.NANOSECOND`.

```js
import { timeNanosecond } from '@uwdata/flechette';
// { typeId: 9, unit: 3, bitWidth: 64, ... }
timeNanosecond()
```

### Timestamp

<hr/><a id="timestamp" href="#timestamp">#</a>
<b>timestamp</b>([<i>unit</i>, <i>timezone</i>])

Create a Timestamp data type instance. Timestamp values are 64-bit signed integers representing an elapsed time since a fixed epoch, stored in either of four *unit*s: seconds, milliseconds, microseconds or nanoseconds, and are optionally annotated with a *timezone*. Timestamp values do not include any leap seconds (in other words, all days are considered 86400 seconds long).

Timestamp values are stored in a `BigInt64Array` and converted to millisecond-based JavaScript `number` values (potentially with fractional digits) upon extraction. An error is raised if a value exceeds either `Number.MIN_SAFE_INTEGER` or `Number.MAX_SAFE_INTEGER`. Pass the `useDate` extraction option (e.g., to [`tableFromIPC`](/flechette/api/#tableFromIPC) or [`tableFromArrays`](/flechette/api/#tableFromArrays)) to instead extract timestamp values as JavaScript `Date` objects.

* *unit* (`number`): The time unit, one of `TimeUnit.SECOND`, `TimeUnit.MILLISECOND` (default), `TimeUnit.MICROSECOND`, or `TimeUnit.NANOSECOND`.
* *timezone* (`string`): An optional string for the name of a timezone. If provided, the value should either be a string as used in the Olson timezone database (the "tz database" or "tzdata"), such as "America/New_York", or an absolute timezone offset of the form "+XX:XX" or "-XX:XX", such as "+07:30". Whether a timezone string is present indicates different semantics about the data. That said, Flechette does not process the timezone information.

```js
import { timestamp } from '@uwdata/flechette';
// { typeId: 10, unit: 1, timezone: null, ... }
timestamp()
// { typeId: 10, unit: 2, timezone: 'Europe/Berlin', ... }
timestamp(TimeUnit.MICROSECOND, 'Europe/Berlin')
```

### Interval

<hr/><a id="interval" href="#interval">#</a>
<b>interval</b>([<i>unit</i>])

Create an Interval data type instance. Values represent calendar intervals stored using integers for each date part. The supported *unit*s are year/moth, day/time, and month/day/nanosecond intervals.

`IntervalUnit.YEAR_MONTH` indicates the number of elapsed whole months, stored as 32-bit signed integers. Flechette extracts these month count integers directly.

`IntervalUnit.DAY_TIME` indicates the number of elapsed days and milliseconds (no leap seconds), stored as 2 contiguous 32-bit signed integers (8-bytes in total). Flechette extracts these values to two-element [day, time] `Int32Array` instances.

`IntervalUnit.MONTH_DAY_NANO` is a triple of the number of elapsed months, days, and nanoseconds. The values are stored contiguously in 16-byte blocks. Months and days are encoded as 32-bit signed integers and nanoseconds is encoded as a 64-bit signed integer. Nanoseconds does not allow for leap seconds. Each field is independent (e.g. there is no constraint that nanoseconds have the same sign as days or that the quantity of nanoseconds represents less than a day's worth of time). Flechette extracts these values to three-element [month, day, nano] `Float64Array` instances.

* *unit* (`number`): The interval unit. One of `IntervalUnit.YEAR_MONTH`, `IntervalUnit.DAY_TIME`, or `IntervalUnit.MONTH_DAY_NANO` (default).

```js
import { interval } from '@uwdata/flechette';
// { typeId: 11, unit: 0, ... }
interval(IntervalUnit.YEAR_MONTH)
// { typeId: 11, unit: 2, ... }
interval(IntervalUnit.MONTH_DAY_NANO)
```

### List

<hr/><a id="list" href="#list">#</a>
<b>list</b>(<i>child</i>)

Create a List type instance, representing variably-sized lists (arrays) with 32-bit offsets. A list has a single child data type for list entries. Lists are represented using integer offsets that indicate list extents within a single child array containing all list values. Lists are extracted to either `Array` or `TypedArray` instances, depending on the child type.

* *child* (`DataType | Field`): The child (list item) field or data type.

```js
import { int32, list } from '@uwdata/flechette';
// {
//   typeId: 12,
//   children: [{
//     name: '',
//     type: type: { typeId: 2, bitWidth: 32, signed: true, ... },
//     ...
//   }],
//   ...
// }
list(int32())
```

### Struct

<hr/><a id="struct" href="#struct">#</a>
<b>struct</b>(<i>children</i>)

Create a Struct type instance. A struct consists of multiple named child data types. Struct values are stored as parallel child batches, one per child type.

By default, structs are fully extracted to standard JavaScript objects. Pass the `useProxy` extraction option (e.g., to [`tableFromIPC`](/flechette/api/#tableFromIPC) or [`tableFromArrays`](/flechette/api/#tableFromArrays)) to instead use zero-copy proxy row objects that extract data on-demand from underlying Arrow batches. Proxy objects can improve performance and reduce memory usage, but do not support convenient property enumeration (`Object.keys`, `Object.values`, `Object.entries`) or spreading (`{ ...object }`). A proxy object can be converted to a standard object by calling its `toJSON()` method.

* *children* (`Field[] | object`): An array of property fields, or an object mapping property names to data types. If an object, the instantiated fields are assumed to be nullable and have no metadata.


```js
import { bool, field, float32, int16, struct } from '@uwdata/flechette';
// using an object with property names and types
// {
//   typeId: 13,
//   children: [
//     { name: 'foo', type: { typeId: 2, bitWidth: 16, ... }, ... },
//     { name: 'bar', type: { typeId: 6 }, ... },
//     { name: 'baz', type: { typeId: 3, precision: 1, ... }, ... }
//   ]
// }
struct({ foo: int16(), bar: bool(), baz: float32() })

// using an array of Field instances
struct([
  field('foo', int16()),
  field('bar', bool()),
  field('baz', float32())
])
```

### Union

<hr/><a id="union" href="#union">#</a>
<b>union</b>(<i>mode</i>, <i>children</i>[, <i>typeIds</i>, <i>typeIdForValue</i>])

Create a Union type instance. A union is a complex type with parallel *children* data types. Union values are stored in either a sparse (`UnionMode.Sparse`) or dense (`UnionMode.Dense`) layout *mode*. In a sparse layout, child types are stored in parallel arrays with the same lengths, resulting in many unused, empty values. In a dense layout, child types have variable lengths and an offsets array is used to index the appropriate value.

By default, ids in the type vector refer to the index in the children array. Optionally, *typeIds* provide an indirection between the child index and the type id. For each child, `typeIds[index]` is the id used in the type vector. The *typeIdForValue* argument provides a lookup function for mapping input data to the proper child type id, and is required if using builder methods.

Extracted JavaScript values depend on the child types.

* *mode* (`number`): The union mode. One of `UnionMode.Sparse` or `UnionMode.Dense`.
* *children* (`(DataType[] | Field)[]`): The children fields or data types. Types are mapped to nullable fields with no metadata.
* *typeIds* (`number[]`): Children type ids, in the same order as the children types. Type ids provide a level of indirection over children types. If not provided, the children indices are used as the type ids.
* *typeIdForValue* (`(value: any, index: number) => number`): A function that takes an arbitrary value and a row index and returns a correponding union type id. This function is required to build union-typed data with [`tableFromArrays`](/flechette/api/#tableFromArrays) or [`columnFromArray`](/flechette/api/#columnFromArray).

```js
import { float64, utf8, union } from '@uwdata/flechette';
// {
//   typeId: 14,
//   mode: 1,
//   typeIds: [ 0, 1 ],
//   typeMap: { '0': 0, '1': 1 },
//   children: [
//     { name: '_0', type: { typeId: 3, precision: 2, ... }, ... },
//     { name: '_1', type: { typeId: 5 }, ... }
//   ],
//   typeIdForValue: <<function>>
// }
union(
  UnionMode.Dense,
  [float64(), utf8()],
  [0, 1],
  v => typeof v === 'string' ? 1 : 0
)
```

### FixedSizeBinary

<hr/><a id="fixedSizeBinary" href="#fixedSizeBinary">#</a>
<b>fixedSizeBinary</b>(<i>stride</i>)

Create a FixedSizeBinary data type instance for opaque binary data where each entry has the same fixed size. Fixed binary data are stored in a single `Uint8Array`, indexed using the known stride and extracted to JavaScript `Uint8Array` subarray values.

* *stride* (`number`): The fixed size in bytes.

```js
import { fixedSizeBinary } from '@uwdata/flechette';
// { typeId: 15, stride: 128 }
fixedSizeBinary(128)
```

### FixedSizeList

<hr/><a id="fixedSizeList" href="#fixedSizeList">#</a>
<b>fixedSizeList</b>(<i>child</i>, <i>stride</i>)

Create a FixedSizeList type instance for list (array) data where every list has the same fixed size. A list has a single child data type for list entries. Fixed size lists are represented as a single child array containing all list values, indexed using the known stride. Lists are extracted to either `Array` or `TypedArray` instances, depending on the child type.

* *child* (`DataType | Field`): The child (list item) field or data type.
* *stride* (`number`): The fixed list size.

```js
import { fixedSizeList, float32 } from '@uwdata/flechette';
// {
//   typeId: 16,
//   stride: 8,
//   children: [ { name: '', type: { typeId: 3, precision: 1, ... }, ... } ]
// }
fixedSizeList(float32(), 8)
```

### Map

<hr/><a id="map" href="#map">#</a>
<b>map</b>(<i>keyField</i>, <i>valueField</i>[, <i>keysSorted</i>])

Create a Map type instance representing collections of key-value pairs. A Map is a logical nested type that is represented as a list of key-value structs. The key and value types are not constrained, so the application is responsible for ensuring that the keys are hashable and unique, and that keys are properly sorted if *keysSorted* is `true`.

By default, map data is extracted to arrays of `[key, value]` pairs, in the style of `Object.entries`. Pass the `useMap` extraction option (e.g., to [`tableFromIPC`](/flechette/api/#tableFromIPC) or [`tableFromArrays`](/flechette/api/#tableFromArrays)) to instead extract JavaScript [`Map`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Map) instances.

* *keyField* (`DataType | Field`): The map key field or data type.
* *valueField* (`DataType | Field`): The map value field or data type.
* *keysSorted* (`boolean`): Flag indicating if the map keys are sorted (default `false`).

```js
import { int64, map, utf8 } from '@uwdata/flechette';
// {
//   typeId: 17,
//   keysSorted: false,
//   children: [{
//     name: 'entries',
//     type: {
//       typeId: 13,
//       children: [
//         { name: 'key', type: { typeId: 5 }, ... },
//         { name: 'value', type: { typeId: 2, bitWidth: 64, ... }, ... }
//       ]
//     }, ...
//   }
//  ]}, ...
// }
map(utf8(), int64())
```

### Duration

<hr/><a id="duration" href="#duration">#</a>
<b>duration</b>([<i>unit</i>])

Create a Duration data type instance. Durations represent an absolute length of time unrelated to any calendar artifacts. The resolution defaults to millisecond, but can be any of the other `TimeUnit` values. This type is always represented as a 64-bit integer.

Duration values are stored as integers in a `BigInt64Array`. By default, duration values are extracted as JavaScript `number` values. An error is raised if a value exceeds either `Number.MIN_SAFE_INTEGER` or `Number.MAX_SAFE_INTEGER`. Pass the `useBigInt` extraction option (e.g., to [`tableFromIPC`](/flechette/api/#tableFromIPC) or [`tableFromArrays`](/flechette/api/#tableFromArrays)) to instead extract duration values directly as `BigInt` values.

* *unit* (`number`): The duration time unit, one of `TimeUnit.SECOND`, `TimeUnit.MILLISECOND` (default), `TimeUnit.MICROSECOND`, or `TimeUnit.NANOSECOND`.

```js
import { duration } from '@uwdata/flechette';
// { typeId: 18, unit: 1, ... }
duration()
```

### LargeBinary

<hr/><a id="largeBinary" href="#largeBinary">#</a>
<b>largeBinary</b>()

Create a LargeBinary data type instance for variably-sized opaque binary data with 64-bit offsets, allowing representation of extremely large data values. Large binary values are stored in a `Uint8Array`, indexed using a 64-bit offset array and extracted to JavaScript `Uint8Array` subarray values.

```js
import { largeBinary } from '@uwdata/flechette';
// { typeId: 19, ... }
largeBinary()
```

### LargeUtf8

<hr/><a id="largeUtf8" href="#largeUtf8">#</a>
<b>largeUtf8</b>()

Create a LargeUtf8 data type instance for Unicode string data of variable length with 64-bit offsets, allowing representation of extremely large data values. [UTF-8](https://en.wikipedia.org/wiki/UTF-8) code points are stored as binary data and extracted to JavaScript `string` values using [`TextDecoder`](https://developer.mozilla.org/en-US/docs/Web/API/TextDecoder). Due to decoding overhead, repeated access to string data can be costly. If making multiple passes over Utf8 data, we recommended converting the string upfront (e.g., via [`Column.toArray`](column#toArray)) and accessing the result.

```js
import { largeUtf8 } from '@uwdata/flechette';
// { typeId: 20, ... }
largeUtf8()
```

### LargeList

<hr/><a id="largeList" href="#largeList">#</a>
<b>largeList</b>(<i>child</i>)

Create a LargeList type instance, representing variably-sized lists (arrays) with 64-bit offsets, allowing representation of extremely large data values. A list has a single child data type for list entries. Lists are represented using integer offsets that indicate list extents within a single child array containing all list values. Lists are extracted to either `Array` or `TypedArray` instances, depending on the child type.

* *child* (`DataType | Field`): The child (list item) field or data type.

```js
import { largeList, utf8 } from '@uwdata/flechette';
// { typeId: 21, children: [ { name: '', type: { typeId: 5 }, ... } ], ... }
largeList(utf8())
```

### RunEndEncoded

<hr/><a id="runEndEncoded" href="#runEndEncoded">#</a>
<b>runEndEncoded</b>(<i>runsField</i>, <i>valuesField</i>)

Create a RunEndEncoded type instance, which compresses data by representing consecutive repeated values as a run. This data type uses two child arrays, `run_ends` and `values`. The `run_ends` child array must be a 16, 32, or 64 bit integer array which encodes the indices at which the run with the value in each corresponding index in the values child array ends. Like list and struct types, the `values` array can be of any type.

To extract values by index, binary search is performed over the run_ends to locate the correct value. The extracted value depends on the `values` data type.

* *runsField* (`DataType | Field`): The run-ends field or data type.
* *valuesField* (`DataType | Field`): The values field or data type.

```js
import { int32, runEndEncoded, utf8 } from '@uwdata/flechette';
// 32-bit integer run ends and utf8 string values
// {
//   typeId: 22,
//   children: [
//     { name: 'run_ends', type: { typeId: 2, bitWidth: 32, ... }, ... },
//     { name: 'values', type: { typeId: 5 }, ... }
//   ]
// }
runEndEncoded(int32(), utf8())
```

### BinaryView

<hr/><a id="binaryView" href="#binaryView">#</a>
<b>binaryView</b>()

Create a BinaryView type instance. BinaryView data is logically the same as the [Binary](#binary) type, but the internal representation uses a view struct that contains the string length and either the string's entire data inline (for small strings) or an inlined prefix, an index of another buffer, and an offset pointing to a slice in that buffer (for non-small strings). For more details, see the [Apache Arrow format documentation](https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-view-layout).

Flechette can encode and decode BinaryView data, extracting `Uint8Array` values. However, Flechette does not currently support building BinaryView columns from JavaScript values.

```js
import { binaryView } from '@uwdata/flechette';
// { typeId: 23 }
binaryView()
```

### Utf8View

<hr/><a id="utf8View" href="#utf8View">#</a>
<b>utf8View</b>()

Create a Utf8View type instance. Utf8View data is logically the same as the [Utf8](#utf8) type, but the internal representation uses a view struct that contains the string length and either the string's entire data inline (for small strings) or an inlined prefix, an index of another buffer, and an offset pointing to a slice in that buffer (for non-small strings). For more details, see the [Apache Arrow format documentation](https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-view-layout).

Flechette can encode and decode Utf8View data, extracting `string` values. However, Flechette does not currently support building Utf8View columns from JavaScript values.

```js
import { utf8View } from '@uwdata/flechette';
// { typeId: 24 }
utf8View()
```

### ListView

<hr/><a id="listView" href="#listView">#</a>
<b>listView</b>(<i>child</i>)

Create a ListView type instance, representing variably-sized lists (arrays) with 32-bit offsets. ListView data represents the same logical types that [List](#list) can, but contains both offsets and sizes allowing for writes in any order and sharing of child values among list values. For more details, see the [Apache Arrow format documentation](https://arrow.apache.org/docs/format/Columnar.html#listview-layout).

ListView data are extracted to either `Array` or `TypedArray` instances, depending on the child type. Flechette can encode and decode ListView data; however, Flechette does not currently support building ListView columns from JavaScript values.

* *child* (`DataType | Field`): The child (list item) field or data type.

```js
import { float16, listView } from '@uwdata/flechette';
// {
//   typeId: 25,
//   children: [ { name: 'value', type: { typeId: 3, ... }, ... } ]
// }
listView(float16())
```

### LargeListView

<hr/><a id="largeListView" href="#largeListView">#</a>
<b>largeListView</b>(<i>child</i>)

Create a LargeListView type instance, representing variably-sized lists (arrays) with 64-bit offsets, allowing representation of extremely large data values. LargeListView data represents the same logical types that [LargeList](#largeList) can, but contains both offsets and sizes allowing for writes in any order and sharing of child values among list values. For more details, see the [Apache Arrow format documentation](https://arrow.apache.org/docs/format/Columnar.html#listview-layout).

LargeListView data are extracted to either `Array` or `TypedArray` instances, depending on the child type. Flechette can encode and decode LargeListView data; however, Flechette does not currently support building LargeListView columns from JavaScript values.

* *child* (`DataType | Field`): The child (list item) field or data type.

```js
import { float16, largeListView } from '@uwdata/flechette';
// {
//   typeId: 26,
//   children: [ { name: 'value', type: { typeId: 3, ... }, ... } ]
// }
largeListView(float16())
```
