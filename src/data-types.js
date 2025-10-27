/**
 * @import { BinaryType, BinaryViewType, BoolType, DataType, DateType, DateUnit_, DecimalType, DictionaryType, DurationType, Field, FixedSizeBinaryType, FixedSizeListType, FloatType, IntBitWidth, IntervalType, IntervalUnit_, IntType, LargeBinaryType, LargeListType, LargeListViewType, LargeUtf8Type, ListType, ListViewType, MapType, NullType, Precision_, RunEndEncodedType, StructType, TimestampType, TimeType, TimeUnit_, UnionMode_, UnionType, Utf8Type, Utf8ViewType } from './types.js'
 */
import { DateUnit, IntervalUnit, Precision, TimeUnit, Type, UnionMode } from './constants.js';
import { intArrayType, float32Array, float64Array, int32Array, int64Array, uint16Array, uint64Array } from './util/arrays.js';
import { check, checkOneOf, keyFor } from './util/objects.js';

/**
 * @typedef {Field | DataType} FieldInput
 */

export const invalidDataType = (typeId) =>
  `Unsupported data type: "${keyFor(Type, typeId)}" (id ${typeId})`;

/**
 * Return a new field instance for use in a schema or type definition. A field
 * represents a field name, data type, and additional metadata. Fields are used
 * to represent child types within nested types like List, Struct, and Union.
 * @param {string} name The field name.
 * @param {DataType} type The field data type.
 * @param {boolean} [nullable=true] Flag indicating if the field is nullable
 *  (default `true`).
 * @param {Map<string,string>|null} [metadata=null] Custom field metadata
 *  annotations (default `null`).
 * @returns {Field} The field instance.
 */
export const field = (name, type, nullable = true, metadata = null) => ({
  name,
  type,
  nullable,
  metadata
});

/**
 * Checks if a value is a field instance.
 * @param {any} value
 * @returns {value is Field}
 */
function isField(value) {
  return Object.hasOwn(value, 'name') && isDataType(value.type)
}

/**
 * Checks if a value is a data type instance.
 * @param {any} value
 * @returns {value is DataType}
 */
function isDataType(value) {
  return typeof value?.typeId === 'number';
}

/**
 * Return a field instance from a field or data type input.
 * @param {FieldInput} value
 *  The value to map to a field.
 * @param {string} [defaultName] The default field name.
 * @param {boolean} [defaultNullable=true] The default nullable value.
 * @returns {Field} The field instance.
 */
function asField(value, defaultName = '', defaultNullable = true) {
  return isField(value)
    ? value
    : field(
        defaultName,
        check(value, isDataType, () => `Data type expected.`),
        defaultNullable
      );
}

/////

/**
 * Return a basic type with only a type id.
 * @template {typeof Type[keyof typeof Type]} T
 * @param {T} typeId The type id.
 */
const basicType = (typeId) => ({ typeId });

/**
 * Return a Dictionary data type instance.  A dictionary type consists of a
 * dictionary of values (which may be of any type) and corresponding integer
 * indices that reference those values. If values are repeated, a dictionary
 * encoding can provide substantial space savings. In the IPC format,
 * dictionary indices reside alongside other columns in a record batch, while
 * dictionary values are written to special dictionary batches, linked by a
 * unique dictionary *id*.
 * @param {DataType} type The data type of dictionary
 *  values.
 * @param {IntType} [indexType] The data type of
 *  dictionary indices. Must be an integer type (default `int32`).
 * @param {boolean} [ordered=false] Indicates if dictionary values are
 *  ordered (default `false`).
 * @param {number} [id=-1] The dictionary id. The default value (-1) indicates
 *  the dictionary applies to a single column only. Provide an explicit id in
 *  order to reuse a dictionary across columns when building, in which case
 *  different dictionaries *must* have different unique ids. All dictionary
 *  ids are later resolved (possibly to new values) upon IPC encoding.
 * @returns {DictionaryType}
 */
export const dictionary = (type, indexType, ordered = false, id = -1) => ({
  typeId: Type.Dictionary,
  id,
  dictionary: type,
  indices: indexType || int32(),
  ordered
});

/**
 * Return a Null data type instance. Null data requires no storage and all
 * extracted values are `null`.
 * @returns {NullType} The null data type.
 */
export const nullType = () => basicType(Type.Null);

/**
 * Return an Int data type instance.
 * @param {IntBitWidth} [bitWidth=32] The integer bit width.
 *  One of `8`, `16`, `32` (default), or `64`.
 * @param {boolean} [signed=true] Flag for signed or unsigned integers
 *  (default `true`).
 * @returns {IntType} The integer data type.
 */
export const int = (bitWidth = 32, signed = true) => ({
  typeId: Type.Int,
  bitWidth: checkOneOf(bitWidth, [8, 16, 32, 64]),
  signed,
  values: intArrayType(bitWidth, signed)
});
/**
 * Return an Int data type instance for 8 bit signed integers.
 * @returns {IntType} The integer data type.
 */
export const int8 = () => int(8);
/**
 * Return an Int data type instance for 16 bit signed integers.
 * @returns {IntType} The integer data type.
 */
export const int16 = () => int(16);
/**
 * Return an Int data type instance for 32 bit signed integers.
 * @returns {IntType} The integer data type.
 */
export const int32 = () => int(32);
/**
 * Return an Int data type instance for 64 bit signed integers.
 * @returns {IntType} The integer data type.
 */
export const int64 = () => int(64);
/**
 * Return an Int data type instance for 8 bit unsigned integers.
 * @returns {IntType} The integer data type.
 */
export const uint8 = () => int(8, false);
/**
 * Return an Int data type instance for 16 bit unsigned integers.
 * @returns {IntType} The integer data type.
 */
export const uint16 = () => int(16, false);
/**
 * Return an Int data type instance for 32 bit unsigned integers.
 * @returns {IntType} The integer data type.
 */
export const uint32 = () => int(32, false);
/**
 * Return an Int data type instance for 64 bit unsigned integers.
 * @returns {IntType} The integer data type.
 */
export const uint64 = () => int(64, false);

/**
 * Return a Float data type instance for floating point numbers.
 * @param {Precision_} [precision=2] The floating point
 *  precision. One of `Precision.HALF` (16-bit), `Precision.SINGLE` (32-bit)
 *  or `Precision.DOUBLE` (64-bit, default).
 * @returns {FloatType} The floating point data type.
 */
export const float = (precision = 2) => ({
  typeId: Type.Float,
  precision: checkOneOf(precision, Precision),
  values: [uint16Array, float32Array, float64Array][precision]
});
/**
 * Return a Float data type instance for half-precision (16 bit) numbers.
 * @returns {FloatType} The floating point data type.
 */
export const float16 = () => float(Precision.HALF);
/**
 * Return a Float data type instance for single-precision (32 bit) numbers.
 * @returns {FloatType} The floating point data type.
 */
export const float32 = () => float(Precision.SINGLE);
/**
 * Return a Float data type instance for double-precision (64 bit) numbers.
 * @returns {FloatType} The floating point data type.
 */
export const float64 = () => float(Precision.DOUBLE);

/**
 * Return a Binary data type instance for variably-sized opaque binary data
 * with 32-bit offsets.
 * @returns {BinaryType} The binary data type.
 */
export const binary = () => ({
  typeId: Type.Binary,
  offsets: int32Array
});

/**
 * Return a Utf8 data type instance for Unicode string data.
 * [UTF-8](https://en.wikipedia.org/wiki/UTF-8) code points are stored as
 * binary data.
 * @returns {Utf8Type} The utf8 data type.
 */
export const utf8 = () => ({
  typeId: Type.Utf8,
  offsets: int32Array
});

/**
 * Return a Bool data type instance. Bool values are stored compactly in
 * bitmaps with eight values per byte.
 * @returns {BoolType} The bool data type.
 */
export const bool = () => basicType(Type.Bool);

/**
 * Return a Decimal data type instance. Decimal values are represented as 32,
 * 64, 128, or 256 bit integers in two's complement. Decimals are fixed point
 * numbers with a set *precision* (total number of decimal digits) and *scale*
 * (number of fractional digits). For example, the number `35.42` can be
 * represented as `3542` with *precision* ≥ 4 and *scale* = 2.
 * @param {number} precision The decimal precision: the total number of
 *  decimal digits that can be represented.
 * @param {number} scale The number of fractional digits, beyond the
 *  decimal point.
 * @param {32 | 64 | 128 | 256} [bitWidth] The decimal bit width.
 *  One of 32, 64, 128 (default), or 256.
 * @returns {DecimalType} The decimal data type.
 */
export const decimal = (precision, scale, bitWidth = 128) => ({
  typeId: Type.Decimal,
  precision,
  scale,
  bitWidth: checkOneOf(bitWidth, [32, 64, 128, 256]),
  values: bitWidth === 32 ? int32Array : uint64Array
});
/**
 * Return an Decimal data type instance with a bit width of 32.
 * @param {number} precision The decimal precision: the total number of
 *  decimal digits that can be represented.
 * @param {number} scale The number of fractional digits, beyond the
 *  decimal point.
 * @returns {DecimalType} The decimal data type.
 */
export const decimal32 = (precision, scale) => decimal(precision, scale, 32);
/**
 * Return an Decimal data type instance with a bit width of 64.
 * @param {number} precision The decimal precision: the total number of
 *  decimal digits that can be represented.
 * @param {number} scale The number of fractional digits, beyond the
 *  decimal point.
 * @returns {DecimalType} The decimal data type.
 */
export const decimal64 = (precision, scale) => decimal(precision, scale, 64);
/**
 * Return an Decimal data type instance with a bit width of 128.
 * @param {number} precision The decimal precision: the total number of
 *  decimal digits that can be represented.
 * @param {number} scale The number of fractional digits, beyond the
 *  decimal point.
 * @returns {DecimalType} The decimal data type.
 */
export const decimal128 = (precision, scale) => decimal(precision, scale, 128);
/**
 * Return an Decimal data type instance with a bit width of 256.
 * @param {number} precision The decimal precision: the total number of
 *  decimal digits that can be represented.
 * @param {number} scale The number of fractional digits, beyond the
 *  decimal point.
 * @returns {DecimalType} The decimal data type.
 */
export const decimal256 = (precision, scale) => decimal(precision, scale, 256);

/**
 * Return a Date data type instance. Date values are 32-bit or 64-bit signed
 * integers representing elapsed time since the UNIX epoch (Jan 1, 1970 UTC),
 * either in units of days (32 bits) or milliseconds (64 bits, with values
 * evenly divisible by 86400000).
 * @param {DateUnit_} unit The date unit.
 *  One of `DateUnit.DAY` or `DateUnit.MILLISECOND`.
 * @returns {DateType} The date data type.
 */
export const date = (unit) => ({
  typeId: Type.Date,
  unit: checkOneOf(unit, DateUnit),
  values: unit === DateUnit.DAY ? int32Array : int64Array
});
/**
 * Return a Date data type instance with units of days.
 * @returns {DateType} The date data type.
 */
export const dateDay = () => date(DateUnit.DAY);
/**
 * Return a Date data type instance with units of milliseconds.
 * @returns {DateType} The date data type.
 */
export const dateMillisecond = () => date(DateUnit.MILLISECOND);

/**
 * Return a Time data type instance, stored in one of four *unit*s: seconds,
 * milliseconds, microseconds or nanoseconds. The integer *bitWidth* is
 * inferred from the *unit* and is 32 bits for seconds and milliseconds or
 * 64 bits for microseconds and nanoseconds. The allowed values are between 0
 * (inclusive) and 86400 (=24*60*60) seconds (exclusive), adjusted for the
 * time unit (for example, up to 86400000 exclusive for the
 * `DateUnit.MILLISECOND` unit.
 *
 * This definition doesn't allow for leap seconds. Time values from
 * measurements with leap seconds will need to be corrected when ingesting
 * into Arrow (for example by replacing the value 86400 with 86399).
 * @param {TimeUnit_} unit The time unit.
 *  One of `TimeUnit.SECOND`, `TimeUnit.MILLISECOND` (default),
 *  `TimeUnit.MICROSECOND`, or `TimeUnit.NANOSECOND`.
 * @returns {TimeType} The time data type.
 */
export const time = (unit = TimeUnit.MILLISECOND) => {
  unit = checkOneOf(unit, TimeUnit);
  const bitWidth = unit === TimeUnit.SECOND || unit === TimeUnit.MILLISECOND ? 32 : 64;
  return {
    typeId: Type.Time,
    unit,
    bitWidth,
    values: bitWidth === 32 ? int32Array : int64Array
  };
};
/**
 * Return a Time data type instance, represented as seconds.
 * @returns {TimeType} The time data type.
 */
export const timeSecond = () => time(TimeUnit.SECOND);
/**
 * Return a Time data type instance, represented as milliseconds.
 * @returns {TimeType} The time data type.
 */
export const timeMillisecond = () => time(TimeUnit.MILLISECOND);
/**
 * Return a Time data type instance, represented as microseconds.
 * @returns {TimeType} The time data type.
 */
export const timeMicrosecond = () => time(TimeUnit.MICROSECOND);
/**
 * Return a Time data type instance, represented as nanoseconds.
 * @returns {TimeType} The time data type.
 */
export const timeNanosecond = () => time(TimeUnit.NANOSECOND);

/**
 * Return a Timestamp data type instance. Timestamp values are 64-bit signed
 * integers representing an elapsed time since a fixed epoch, stored in either
 * of four units: seconds, milliseconds, microseconds or nanoseconds, and are
 * optionally annotated with a timezone. Timestamp values do not include any
 * leap seconds (in other words, all days are considered 86400 seconds long).
 * @param {TimeUnit_} [unit] The time unit.
 *  One of `TimeUnit.SECOND`, `TimeUnit.MILLISECOND` (default),
 *  `TimeUnit.MICROSECOND`, or `TimeUnit.NANOSECOND`.
 * @param {string|null} [timezone=null] An optional string for the name of a
 *  timezone. If provided, the value should either be a string as used in the
 *  Olson timezone database (the "tz database" or "tzdata"), such as
 *  "America/New_York", or an absolute timezone offset of the form "+XX:XX" or
 *  "-XX:XX", such as "+07:30".Whether a timezone string is present indicates
 *  different semantics about the data.
 * @returns {TimestampType} The time data type.
 */
export const timestamp = (unit = TimeUnit.MILLISECOND, timezone = null) => ({
  typeId: Type.Timestamp,
  unit: checkOneOf(unit, TimeUnit),
  timezone,
  values: int64Array
});

/**
 * Return an Interval type instance. Values represent calendar intervals stored
 * as integers for each date part. The supported *unit*s are year/moth,
 * day/time, and month/day/nanosecond intervals.
 *
 * `IntervalUnit.YEAR_MONTH` indicates the number of elapsed whole months,
 * stored as 32-bit signed integers.
 *
 * `IntervalUnit.DAY_TIME` indicates the number of elapsed days and
 * milliseconds (no leap seconds), stored as 2 contiguous 32-bit signed
 * integers (8-bytes in total).
 *
 * `IntervalUnit.MONTH_DAY_NANO` is a triple of the number of elapsed months,
 * days, and nanoseconds. The values are stored contiguously in 16-byte blocks.
 * Months and days are encoded as 32-bit signed integers and nanoseconds is
 * encoded as a 64-bit signed integer. Nanoseconds does not allow for leap
 * seconds. Each field is independent (e.g. there is no constraint that
 * nanoseconds have the same sign as days or that the quantity of nanoseconds
 * represents less than a day's worth of time).
 * @param {IntervalUnit_} unit  The interval unit.
 *  One of `IntervalUnit.YEAR_MONTH`, `IntervalUnit.DAY_TIME`, or
 *  `IntervalUnit.MONTH_DAY_NANO` (default).
 * @returns {IntervalType} The interval data type.
 */
export const interval = (unit = IntervalUnit.MONTH_DAY_NANO) => ({
  typeId: Type.Interval,
  unit: checkOneOf(unit, IntervalUnit),
  values: unit === IntervalUnit.MONTH_DAY_NANO ? undefined : int32Array
});

/**
 * Return a List data type instance, representing variably-sized lists
 * (arrays) with 32-bit offsets. A list has a single child data type for
 * list entries. Lists are represented using integer offsets that indicate
 * list extents within a single child array containing all list values.
 * @param {FieldInput} child The child (list item) field or data type.
 * @returns {ListType} The list data type.
 */
export const list = (child) => ({
  typeId: Type.List,
  children: [ asField(child) ],
  offsets: int32Array
});

/**
 * Return a Struct data type instance. A struct consists of multiple named
 * child data types. Struct values are stored as parallel child batches, one
 * per child type, and extracted to standard JavaScript objects.
 * @param {Field[] | Record<string, DataType>} children
 *  An array of property fields, or an object mapping property names to data
 *  types. If an object, the instantiated fields are assumed to be nullable
 *  and have no metadata.
 * @returns {StructType} The struct data type.
 */
export const struct = (children) => ({
  typeId: Type.Struct,
  children: Array.isArray(children) && isField(children[0])
    ? /** @type {Field[]} */ (children)
    : Object.entries(children).map(([name, type]) => field(name, type))
});

/**
 * Return a Union type instance. A union is a complex type with parallel
 * *children* data types. Union values are stored in either a sparse
 * (`UnionMode.Sparse`) or dense (`UnionMode.Dense`) layout *mode*. In a
 * sparse layout, child types are stored in parallel arrays with the same
 * lengths, resulting in many unused, empty values. In a dense layout, child
 * types have variable lengths and an offsets array is used to index the
 * appropriate value.
 *
 * By default, ids in the type vector refer to the index in the children
 * array. Optionally, *typeIds* provide an indirection between the child
 * index and the type id. For each child, `typeIds[index]` is the id used
 * in the type vector. The *typeIdForValue* argument provides a lookup
 * function for mapping input data to the proper child type id, and is
 * required if using builder methods.
 * @param {UnionMode_} mode The union mode.
 *  One of `UnionMode.Sparse` or `UnionMode.Dense`.
 * @param {FieldInput[]} children The children fields or data types.
 *  Types are mapped to nullable fields with no metadata.
 * @param {number[]} [typeIds]  Children type ids, in the same order as the
 *  children types. Type ids provide a level of indirection over children
 *  types. If not provided, the children indices are used as the type ids.
 * @param {(value: any, index: number) => number} [typeIdForValue]
 *  A function that takes an arbitrary value and a row index and returns a
 *  correponding union type id. Required by builder methods.
 * @returns {UnionType} The union data type.
 */
export const union = (mode, children, typeIds, typeIdForValue) => {
  typeIds ??= children.map((v, i) => i);
  return {
    typeId: Type.Union,
    mode: checkOneOf(mode, UnionMode),
    typeIds,
    typeMap: typeIds.reduce((m, id, i) => ((m[id] = i), m), {}),
    children: children.map((v, i) => asField(v, `_${i}`)),
    typeIdForValue,
    offsets: int32Array,
  };
};

/**
 * Create a FixedSizeBinary data type instance for opaque binary data where
 * each entry has the same fixed size.
 * @param {number} stride The fixed size in bytes.
 * @returns {FixedSizeBinaryType} The fixed size binary data type.
 */
export const fixedSizeBinary = (stride) => ({
  typeId: Type.FixedSizeBinary,
  stride
});

/**
 * Return a FixedSizeList type instance for list (array) data where every list
 * has the same fixed size. A list has a single child data type for list
 * entries. Fixed size lists are represented as a single child array containing
 * all list values, indexed using the known stride.
 * @param {FieldInput} child The list item data type.
 * @param {number} stride The fixed list size.
 * @returns {FixedSizeListType} The fixed size list data type.
 */
export const fixedSizeList = (child, stride) => ({
  typeId: Type.FixedSizeList,
  stride,
  children: [ asField(child) ]
});

/**
 * Internal method to create a Map type instance.
 * @param {boolean} keysSorted Flag indicating if the map keys are sorted.
 * @param {Field} child The child fields.
 * @returns {MapType} The map data type.
 */
export const mapType = (keysSorted, child) => ({
  typeId: Type.Map,
  keysSorted,
  children: [child],
  offsets: int32Array
});

/**
 * Return a Map data type instance representing collections of key-value pairs.
 * A Map is a logical nested type that is represented as a list of key-value
 * structs. The key and value types are not constrained, so the application is
 * responsible for ensuring that the keys are hashable and unique, and that
 * keys are properly sorted if *keysSorted* is `true`.
 * @param {FieldInput} keyField The map key field or data type.
 * @param {FieldInput} valueField The map value field or data type.
 * @param {boolean} [keysSorted=false] Flag indicating if the map keys are
 *  sorted (default `false`).
 * @returns {MapType} The map data type.
 */
export const map = (keyField, valueField, keysSorted = false) => mapType(
  keysSorted,
  field(
    'entries',
    struct([ asField(keyField, 'key', false), asField(valueField, 'value') ]),
    false
  )
);

/**
 * Return a Duration data type instance. Durations represent an absolute length
 * of time unrelated to any calendar artifacts. The resolution defaults to
 * millisecond, but can be any of the other `TimeUnit` values. This type is
 * always represented as a 64-bit integer.
 * @param {TimeUnit_} unit
 * @returns {DurationType} The duration data type.
 */
export const duration = (unit = TimeUnit.MILLISECOND) => ({
  typeId: Type.Duration,
  unit: checkOneOf(unit, TimeUnit),
  values: int64Array
});

/**
 * Return a LargeBinary data type instance for variably-sized opaque binary
 * data with 64-bit offsets, allowing representation of extremely large data
 * values.
 * @returns {LargeBinaryType} The large binary data type.
 */
export const largeBinary = () => ({
  typeId: Type.LargeBinary,
  offsets: int64Array
});

/**
 * Return a LargeUtf8 data type instance for Unicode string data of variable
 * length with 64-bit offsets, allowing representation of extremely large data
 * values. [UTF-8](https://en.wikipedia.org/wiki/UTF-8) code points are stored
 * as binary data.
 * @returns {LargeUtf8Type} The large utf8 data type.
 */
export const largeUtf8 = () => ({
  typeId: Type.LargeUtf8,
  offsets: int64Array
});

/**
 * Return a LargeList data type instance, representing variably-sized lists
 * (arrays) with 64-bit offsets, allowing representation of extremely large
 * data values. A list has a single child data type for list entries. Lists
 * are represented using integer offsets that indicate list extents within a
 * single child array containing all list values.
 * @param {FieldInput} child The child (list item) field or data type.
 * @returns {LargeListType} The large list data type.
 */
export const largeList = (child) => ({
  typeId: Type.LargeList,
  children: [ asField(child) ],
  offsets: int64Array
});

/**
 * Return a RunEndEncoded data type instance, which compresses data by
 * representing consecutive repeated values as a run. This data type uses two
 * child arrays, `run_ends` and `values`. The `run_ends` child array must be
 * a 16, 32, or 64 bit integer array which encodes the indices at which the
 * run with the value in each corresponding index in the values child array
 * ends. Like list and struct types, the `values` array can be of any type.
 * @param {FieldInput} runsField The run-ends field or data type.
 * @param {FieldInput} valuesField The values field or data type.
 * @returns {RunEndEncodedType} The large list data type.
 */
export const runEndEncoded = (runsField, valuesField) => ({
  typeId: Type.RunEndEncoded,
  children: [
    check(
      asField(runsField, 'run_ends'),
      (field) => field.type.typeId === Type.Int,
      () => 'Run-ends must have an integer type.'
    ),
    asField(valuesField, 'values')
  ]
});

/**
 * Return a BinaryView data type instance. BinaryView data is logically the
 * same as the Binary type, but the internal representation uses a view struct
 * that contains the string length and either the string's entire data inline
 * (for small strings) or an inlined prefix, an index of another buffer, and an
 * offset pointing to a slice in that buffer (for non-small strings).
 *
 * Flechette can encode and decode BinaryView data; however, Flechette does
 * not currently support building BinaryView columns from JavaScript values.
 * @returns {BinaryViewType} The binary view data type.
 */
export const binaryView = () => /** @type {BinaryViewType} */
  (basicType(Type.BinaryView));

/**
 * Return a Utf8View data type instance. Utf8View data is logically the same as
 * the Utf8 type, but the internal representation uses a view struct that
 * contains the string length and either the string's entire data inline (for
 * small strings) or an inlined prefix, an index of another buffer, and an
 * offset pointing to a slice in that buffer (for non-small strings).
 *
 * Flechette can encode and decode Utf8View data; however, Flechette does
 * not currently support building Utf8View columns from JavaScript values.
 * @returns {Utf8ViewType} The utf8 view data type.
 */
export const utf8View = () => /** @type {Utf8ViewType} */
  (basicType(Type.Utf8View));

/**
 * Return a ListView data type instance, representing variably-sized lists
 * (arrays) with 32-bit offsets. ListView data represents the same logical
 * types that List can, but contains both offsets and sizes allowing for
 * writes in any order and sharing of child values among list values.
 *
 * Flechette can encode and decode ListView data; however, Flechette does not
 * currently support building ListView columns from JavaScript values.
 * @param {FieldInput} child The child (list item) field or data type.
 * @returns {ListViewType} The list view data type.
 */
export const listView = (child) => ({
  typeId: Type.ListView,
  children: [ asField(child, 'value') ],
  offsets: int32Array
});

/**
 * Return a LargeListView data type instance, representing variably-sized lists
 * (arrays) with 64-bit offsets, allowing representation of extremely large
 * data values. LargeListView data represents the same logical types that
 * LargeList can, but contains both offsets and sizes allowing for writes
 * in any order and sharing of child values among list values.
 *
 * Flechette can encode and decode LargeListView data; however, Flechette does
 * not currently support building LargeListView columns from JavaScript values.
 * @param {FieldInput} child The child (list item) field or data type.
 * @returns {LargeListViewType} The large list view data type.
 */
export const largeListView = (child) => ({
  typeId: Type.LargeListView,
  children: [ asField(child, 'value') ],
  offsets: int64Array
});
