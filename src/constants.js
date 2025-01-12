/** Magic bytes 'ARROW1' indicating the Arrow 'file' format. */
export const MAGIC = Uint8Array.of(65, 82, 82, 79, 87, 49);

/** Bytes for an 'end of stream' message. */
export const EOS = Uint8Array.of(255, 255, 255, 255, 0, 0, 0, 0);

/**
 * Apache Arrow version.
 */
export const Version = /** @type {const} */ ({
  /** 0.1.0 (October 2016). */
  V1: 0,
  /** 0.2.0 (February 2017). Non-backwards compatible with V1. */
  V2: 1,
  /** 0.3.0 -> 0.7.1 (May - December 2017). Non-backwards compatible with V2. */
  V3: 2,
  /** >= 0.8.0 (December 2017). Non-backwards compatible with V3. */
  V4: 3,
  /**
   * >= 1.0.0 (July 2020). Backwards compatible with V4 (V5 readers can read V4
   * metadata and IPC messages). Implementations are recommended to provide a
   * V4 compatibility mode with V5 format changes disabled.
   *
   * Incompatible changes between V4 and V5:
   * - Union buffer layout has changed.
   *   In V5, Unions don't have a validity bitmap buffer.
   */
  V5: 4
});

/**
 * Endianness of Arrow-encoded data.
 */
export const Endianness = /** @type {const} */ ({
  Little: 0,
  Big: 1
});

/**
 * Message header type codes.
 */
export const MessageHeader = /** @type {const} */ ({
  NONE: 0,
  /**
   * A Schema describes the columns in a record batch.
   */
  Schema: 1,
  /**
   * For sending dictionary encoding information. Any Field can be
   * dictionary-encoded, but in this case none of its children may be
   * dictionary-encoded.
   * There is one vector / column per dictionary, but that vector / column
   * may be spread across multiple dictionary batches by using the isDelta
   * flag.
   */
  DictionaryBatch: 2,
  /**
   * A data header describing the shared memory layout of a "record" or "row"
   * batch. Some systems call this a "row batch" internally and others a "record
   * batch".
   */
  RecordBatch: 3,
  /**
   * EXPERIMENTAL: Metadata for n-dimensional arrays, aka "tensors" or
   * "ndarrays". Arrow implementations in general are not required to implement
   * this type.
   *
   * Not currently supported by Flechette.
   */
  Tensor: 4,
  /**
   * EXPERIMENTAL: Metadata for n-dimensional sparse arrays, aka "sparse
   * tensors". Arrow implementations in general are not required to implement
   * this type.
   *
   * Not currently supported by Flechette.
   */
  SparseTensor: 5
});

/**
 * Field data type ids.
 * Only non-negative values ever occur in IPC flatbuffer binary data.
 */
export const Type = /** @type {const} */ ({
  /**
   * Dictionary types compress data by using a set of integer indices to
   * lookup potentially repeated vales in a separate dictionary of values.
   *
   * This type entry is provided for API convenience, it does not occur
   * in actual Arrow IPC binary data.
   */
  Dictionary: -1,
  /** No data type. Included for flatbuffer compatibility. */
  NONE: 0,
  /** Null values only. */
  Null: 1,
  /** Integers, either signed or unsigned, with 8, 16, 32, or 64 bit widths. */
  Int: 2,
  /** Floating point numbers with 16, 32, or 64 bit precision. */
  Float: 3,
  /** Opaque binary data. */
  Binary: 4,
  /** Unicode with UTF-8 encoding. */
  Utf8: 5,
  /** Booleans represented as 8 bit bytes. */
  Bool: 6,
  /**
   * Exact decimal value represented as an integer value in two's complement.
   * Currently only 128-bit (16-byte) and 256-bit (32-byte) integers are used.
   * The representation uses the endianness indicated in the schema.
   */
  Decimal: 7,
  /**
   * Date is either a 32-bit or 64-bit signed integer type representing an
   * elapsed time since UNIX epoch (1970-01-01), stored in either of two units:
   * - Milliseconds (64 bits) indicating UNIX time elapsed since the epoch (no
   * leap seconds), where the values are evenly divisible by 86400000
   * - Days (32 bits) since the UNIX epoch
   */
  Date: 8,
  /**
   * Time is either a 32-bit or 64-bit signed integer type representing an
   * elapsed time since midnight, stored in either of four units: seconds,
   * milliseconds, microseconds or nanoseconds.
   *
   * The integer `bitWidth` depends on the `unit` and must be one of the following:
   * - SECOND and MILLISECOND: 32 bits
   * - MICROSECOND and NANOSECOND: 64 bits
   *
   * The allowed values are between 0 (inclusive) and 86400 (=24*60*60) seconds
   * (exclusive), adjusted for the time unit (for example, up to 86400000
   * exclusive for the MILLISECOND unit).
   * This definition doesn't allow for leap seconds. Time values from
   * measurements with leap seconds will need to be corrected when ingesting
   * into Arrow (for example by replacing the value 86400 with 86399).
   */
  Time: 9,
  /**
   * Timestamp is a 64-bit signed integer representing an elapsed time since a
   * fixed epoch, stored in either of four units: seconds, milliseconds,
   * microseconds or nanoseconds, and is optionally annotated with a timezone.
   *
   * Timestamp values do not include any leap seconds (in other words, all
   * days are considered 86400 seconds long).
   *
   * The timezone is an optional string for the name of a timezone, one of:
   *
   *  - As used in the Olson timezone database (the "tz database" or
   *    "tzdata"), such as "America/New_York".
   *  - An absolute timezone offset of the form "+XX:XX" or "-XX:XX",
   *    such as "+07:30".
   *
   * Whether a timezone string is present indicates different semantics about
   * the data.
   */
  Timestamp: 10,
  /**
   * A "calendar" interval which models types that don't necessarily
   * have a precise duration without the context of a base timestamp (e.g.
   * days can differ in length during day light savings time transitions).
   * All integers in the units below are stored in the endianness indicated
   * by the schema.
   *
   *  - YEAR_MONTH - Indicates the number of elapsed whole months, stored as
   *    4-byte signed integers.
   *  - DAY_TIME - Indicates the number of elapsed days and milliseconds (no
   *    leap seconds), stored as 2 contiguous 32-bit signed integers (8-bytes
   *    in total). Support of this IntervalUnit is not required for full arrow
   *    compatibility.
   *  - MONTH_DAY_NANO - A triple of the number of elapsed months, days, and
   *    nanoseconds. The values are stored contiguously in 16-byte blocks.
   *    Months and days are encoded as 32-bit signed integers and nanoseconds
   *    is encoded as a 64-bit signed integer. Nanoseconds does not allow for
   *    leap seconds. Each field is independent (e.g. there is no constraint
   *    that nanoseconds have the same sign as days or that the quantity of
   *    nanoseconds represents less than a day's worth of time).
   */
  Interval: 11,
  /**
   * List (vector) data supporting variably-sized lists.
   * A list has a single child data type for list entries.
   */
  List: 12,
  /**
   * A struct consisting of multiple named child data types.
   */
  Struct: 13,
  /**
   * A union is a complex type with parallel child data types. By default ids
   * in the type vector refer to the offsets in the children. Optionally
   * typeIds provides an indirection between the child offset and the type id.
   * For each child `typeIds[offset]` is the id used in the type vector.
   */
  Union: 14,
  /**
   * Binary data where each entry has the same fixed size.
   */
  FixedSizeBinary: 15,
  /**
   * List (vector) data where every list has the same fixed size.
   * A list has a single child data type for list entries.
   */
  FixedSizeList: 16,
  /**
   * A Map is a logical nested type that is represented as
   * List<entries: Struct<key: K, value: V>>
   *
   * In this layout, the keys and values are each respectively contiguous. We do
   * not constrain the key and value types, so the application is responsible
   * for ensuring that the keys are hashable and unique. Whether the keys are sorted
   * may be set in the metadata for this field.
   *
   * In a field with Map type, the field has a child Struct field, which then
   * has two children: key type and the second the value type. The names of the
   * child fields may be respectively "entries", "key", and "value", but this is
   * not enforced.
   *
   * Map
   * ```text
   *   - child[0] entries: Struct
   *   - child[0] key: K
   *   - child[1] value: V
   *  ```
   * Neither the "entries" field nor the "key" field may be nullable.
   *
   * The metadata is structured so that Arrow systems without special handling
   * for Map can make Map an alias for List. The "layout" attribute for the Map
   * field must have the same contents as a List.
   */
  Map: 17,
  /**
   * An absolute length of time unrelated to any calendar artifacts. For the
   * purposes of Arrow implementations, adding this value to a Timestamp
   * ("t1") naively (i.e. simply summing the two numbers) is acceptable even
   * though in some cases the resulting Timestamp (t2) would not account for
   * leap-seconds during the elapsed time between "t1" and "t2". Similarly,
   * representing the difference between two Unix timestamp is acceptable, but
   * would yield a value that is possibly a few seconds off from the true
   * elapsed time.
   *
   * The resolution defaults to millisecond, but can be any of the other
   * supported TimeUnit values as with Timestamp and Time types. This type is
   * always represented as an 8-byte integer.
   */
  Duration: 18,
  /**
   * Same as Binary, but with 64-bit offsets, allowing representation of
   * extremely large data values.
   */
  LargeBinary: 19,
  /**
   * Same as Utf8, but with 64-bit offsets, allowing representation of
   * extremely large data values.
   */
  LargeUtf8: 20,
  /**
   * Same as List, but with 64-bit offsets, allowing representation of
   * extremely large data values.
   */
  LargeList: 21,
  /**
   * Contains two child arrays, run_ends and values. The run_ends child array
   * must be a 16/32/64-bit integer array which encodes the indices at which
   * the run with the value in each corresponding index in the values child
   * array ends. Like list/struct types, the value array can be of any type.
   */
  RunEndEncoded: 22,
  /**
   * Logically the same as Binary, but the internal representation uses a view
   * struct that contains the string length and either the string's entire data
   * inline (for small strings) or an inlined prefix, an index of another buffer,
   * and an offset pointing to a slice in that buffer (for non-small strings).
   *
   * Since it uses a variable number of data buffers, each Field with this type
   * must have a corresponding entry in `variadicBufferCounts`.
   */
  BinaryView: 23,
  /**
   * Logically the same as Utf8, but the internal representation uses a view
   * struct that contains the string length and either the string's entire data
   * inline (for small strings) or an inlined prefix, an index of another buffer,
   * and an offset pointing to a slice in that buffer (for non-small strings).
   *
   * Since it uses a variable number of data buffers, each Field with this type
   * must have a corresponding entry in `variadicBufferCounts`.
   */
  Utf8View: 24,
  /**
   * Represents the same logical types that List can, but contains offsets and
   * sizes allowing for writes in any order and sharing of child values among
   * list values.
   */
  ListView: 25,
  /**
   * Same as ListView, but with 64-bit offsets and sizes, allowing to represent
   * extremely large data values.
   */
  LargeListView: 26
});

/**
 * Floating point number precision.
 */
export const Precision = /** @type {const} */ ({
  /** 16-bit floating point number. */
  HALF: 0,
  /** 32-bit floating point number. */
  SINGLE: 1,
  /** 64-bit floating point number. */
  DOUBLE: 2
});

/**
 * Date units.
 */
export const DateUnit = /** @type {const} */ ({
  /* Days (as 32 bit int) since the UNIX epoch. */
  DAY: 0,
  /**
   * Milliseconds (as 64 bit int) indicating UNIX time elapsed since the epoch
   * (no leap seconds), with values evenly divisible by 86400000.
   */
  MILLISECOND: 1
});

/**
 * Time units.
 */
export const TimeUnit = /** @type {const} */ ({
  /** Seconds. */
  SECOND: 0,
  /** Milliseconds. */
  MILLISECOND: 1,
  /** Microseconds. */
  MICROSECOND: 2,
  /** Nanoseconds. */
  NANOSECOND: 3
});

/**
 * Date/time interval units.
 */
export const IntervalUnit = /** @type {const} */ ({
  /**
   * Indicates the number of elapsed whole months, stored as 4-byte signed
   * integers.
   */
  YEAR_MONTH: 0,
  /**
   * Indicates the number of elapsed days and milliseconds (no leap seconds),
   * stored as 2 contiguous 32-bit signed integers (8-bytes in total). Support
   * of this IntervalUnit is not required for full arrow compatibility.
   */
  DAY_TIME: 1,
  /**
   * A triple of the number of elapsed months, days, and nanoseconds.
   * The values are stored contiguously in 16-byte blocks. Months and days are
   * encoded as 32-bit signed integers and nanoseconds is encoded as a 64-bit
   * signed integer. Nanoseconds does not allow for leap seconds. Each field is
   * independent (e.g. there is no constraint that nanoseconds have the same
   * sign as days or that the quantity of nanoseconds represents less than a
   * day's worth of time).
   */
  MONTH_DAY_NANO: 2
});

/**
 * Union type modes.
 */
export const UnionMode = /** @type {const} */ ({
  /** Sparse union layout with full arrays for each sub-type. */
  Sparse: 0,
  /** Dense union layout with offsets into value arrays. */
  Dense: 1
});
