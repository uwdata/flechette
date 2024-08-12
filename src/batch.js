import { float64 } from './array-types.js';
import { decodeBit, decodeUtf8, divide, readInt32, readInt64AsNum, toNumber } from './util.js';

/**
 * Column values from a single record batch.
 * A column may contain multiple batches.
 * @template T
 */
export class Batch {
  /**
   * The array type to use when extracting data from the batch.
   * A null value indicates that the array type should match
   * the type of the batch's values array.
   * @type {ArrayConstructor | import('./types.js').TypedArrayConstructor | null}
   */
  static ArrayType = null;

  /**
   * Create a new column batch.
   * @param {object} options
   * @param {import('./types.js').DataType} options.type The field data type
   * @param {number} options.length The length of the batch
   * @param {number} options.nullCount The null value count
   * @param {Uint8Array} [options.validity] Validity bitmap buffer
   * @param {import('./types.js').TypedArray} [options.values] Values buffer
   * @param {import('./types.js').OffsetArray} [options.offsets] Offsets buffer
   * @param {Int8Array} [options.typeIds] Union type ids buffer
   * @param {Batch[]} [options.children] Children batches
   */
  constructor({
    type,
    length,
    nullCount,
    validity,
    values,
    offsets,
    typeIds,
    children
  }) {
    this.type = type;
    this.length = length;
    this.nullCount = nullCount;
    this.validity = validity;
    this.values = values;
    this.offsets = offsets;
    this.typeIds = typeIds;
    this.children = children;

    // optimize access if this batch has no null values
    if (!nullCount) {
      /** @type {(index: number) => T | null} */
      this.at = index => this.value(index);
    }
  }

  /**
   * Provide an informative object string tag.
   */
  get [Symbol.toStringTag]() {
    return 'Batch';
  }

  /**
   * Return the value at the given index.
   * @param {number} index The value index.
   * @returns {T | null} The value.
   */
  at(index) {
    return this.isValid(index) ? this.value(index) : null;
  }

  /**
   * Check if a value at the given index is valid (non-null).
   * @param {number} index The value index.
   * @returns {boolean} True if valid, false otherwise.
   */
  isValid(index) {
    return decodeBit(this.validity, index);
  }

  /**
   * Return the value at the given index. This method does not check the
   * validity bitmap and is intended primarily for internal use. In most
   * cases, callers should use the `at()` method instead.
   * @param {number} index The value index
   * @returns {T} The value, ignoring the validity bitmap.
   */
  value(index) {
    return /** @type {T} */ (this.values[index]);
  }

  /**
   * Extract an array of values within the given index range.
   * @param {number} start The starting index, inclusive
   * @param {number} end The ending index, exclusive
   * @returns {import('./types.js').ValueArray<T?>} The slice of values
   */
  slice(start, end) {
    const n = end - start;
    const values = Array(n);
    for (let i = 0; i < n; ++i) {
      values[i] = this.at(start + i);
    }
    return values;
  }

  /**
   * Return an iterator over the values in this batch.
   * @returns {Iterator<T?>}
   */
  *[Symbol.iterator]() {
    const { length } = this;
    for (let i = 0; i < length; ++i) {
      yield this.at(i);
    }
  }
}

/**
 * A batch whose value buffer can be used directly, without transformation.
 * @template T
 * @extends {Batch<T>}
 */
export class DirectBatch extends Batch {
  /**
   * Extract an array of values within the given index range.
   * When feasible, a zero-copy subarray of a typed array is returned.
   * @param {number} start The starting index, inclusive
   * @param {number} end The ending index, exclusive
   * @returns {import('./types.js').ValueArray<T?>} The slice of values
   */
  slice(start, end) {
    // @ts-ignore
    return this.nullCount
      ? super.slice(start, end)
      : this.values.subarray(start, end);
  }

  /**
   * Return an iterator over the values in this batch.
   * @returns {Iterator<T?>}
   */
  [Symbol.iterator]() {
    return this.nullCount
      ? super[Symbol.iterator]()
      : /** @type {Iterator<T?>} */ (this.values[Symbol.iterator]());
  }
}

/**
 * A batch whose values are transformed to 64-bit numbers.
 * @extends {Batch<number>}
 */
export class NumberBatch extends Batch {
  static ArrayType = float64;
}

/**
 * A batch whose values should be returned in a standard array.
 * @template T
 * @extends {Batch<T>}
 */
export class ArrayBatch extends Batch {
  static ArrayType = Array;
}

/**
 * A batch of null values only.
 * @extends {ArrayBatch<null>}
 */
export class NullBatch extends ArrayBatch {
  /**
   * @param {number} index The value index
   * @returns {null}
   */
  value(index) { // eslint-disable-line no-unused-vars
    return null;
  }
}

/**
 * A batch that coerces BigInt values to 64-bit numbers.
 * * @extends {NumberBatch}
 */
export class Int64Batch extends NumberBatch {
  /**
   * @param {number} index The value index
   */
  value(index) {
    return toNumber(/** @type {bigint} */ (this.values[index]));
  }
}

/**
 * A batch of 16-bit floating point numbers, accessed as unsigned
 * 16-bit ints and transformed to 64-bit numbers.
 */
export class Float16Batch extends NumberBatch {
  /**
   * @param {number} index The value index
   */
  value(index) {
    const v = /** @type {number} */ (this.values[index]);
    const expo = (v & 0x7C00) >> 10;
    const sigf = (v & 0x03FF) / 1024;
    const sign = (-1) ** ((v & 0x8000) >> 15);
    switch (expo) {
      case 0x1F: return sign * (sigf ? Number.NaN : 1 / 0);
      case 0x00: return sign * (sigf ? 6.103515625e-5 * sigf : 0);
    }
    return sign * (2 ** (expo - 15)) * (1 + sigf);
  }
}

/**
 * A batch of boolean values stored as a bitmap.
 * @extends {ArrayBatch<boolean>}
 */
export class BoolBatch extends ArrayBatch {
  /**
   * @param {number} index The value index
   */
  value(index) {
    return decodeBit(/** @type {Uint8Array} */ (this.values), index);
  }
}

// generate base values for big integers represented in a Uint32Array
const BASE32 = Array.from(
  { length: 8 },
  (_, i) => Math.pow(2, i * 32)
);

/**
 * A batch of 128- or 256-bit decimal numbers, accessed as unsigned
 * 32-bit ints and coerced to 64-bit numbers. The number coercion
 * may be lossy if the decimal precision can not be represented in
 * a 64-bit floating point format.
 */
export class DecimalBatch extends NumberBatch {
  constructor(options) {
    super(options);
    const type = /** @type {import('./types.js').DecimalType} */ (this.type);
    this.stride = type.bitWidth >> 5; // 8 bits/byte and 4 bytes/uint32;
    this.scale = Math.pow(10, type.scale);
  }

  /**
   * @param {number} index The value index
   */
  value(index) {
    // TODO: check magnitude, use slower but more accurate BigInt ops if needed?
    // Using numbers we can prep with integers up to MAX_SAFE_INTEGER (2^53 - 1)
    const v = /** @type {Uint32Array} */ (this.values);
    const n = this.stride;
    const off = index << 2;
    let x = 0;
    if ((v[n - 1] | 0) < 0) {
      for (let i = 0; i < n; ++i) {
        x += ~v[i + off] * BASE32[i];
      }
      x = -(x + 1);
    } else {
      for (let i = 0; i < n; ++i) {
        x += v[i + off] * BASE32[i];
      }
    }
    return x / this.scale;
  }
}

/**
 * A batch of date or timestamp values that are coerced to UNIX epoch timestamps
 * and returned as JS Date objects. This batch wraps a source batch that provides
 * timestamp values.
 * @extends {ArrayBatch<Date>}
 */
export class DateBatch extends ArrayBatch {
  constructor(batch) {
    super(batch);
    this.source = batch;
  }

  /**
   * @param {number} index The value index
   */
  value(index) {
    return new Date(this.source.value(index));
  }
}

/**
 * A batch of dates as day counts, coerced to timestamp numbers.
 */
export class DateDayBatch extends NumberBatch {
  /**
   * @param {number} index The value index
   * @returns {number}
   */
  value(index) {
    // epoch days to milliseconds
    return 86400000 * /** @type {number} */ (this.values[index]);
  }
}

/**
 * A batch of dates as millisecond timestamps, coerced to numbers.
 */
export const DateDayMillisecondBatch = Int64Batch;

/**
 * A batch of timestaps in seconds, coerced to millisecond numbers.
 */
export class TimestampSecondBatch extends Int64Batch {
  /**
   * @param {number} index The value index
   */
  value(index) {
    return super.value(index) * 1e3; // seconds to milliseconds
  }
}

/**
 * A batch of timestaps in milliseconds, coerced to numbers.
 */
export const TimestampMillisecondBatch = Int64Batch;

/**
 * A batch of timestaps in microseconds, coerced to millisecond numbers.
 */
export class TimestampMicrosecondBatch extends Int64Batch {
  /**
   * @param {number} index The value index
   */
  value(index) {
    // microseconds to milliseconds
    return divide(/** @type {bigint} */ (this.values[index]), 1000n);
  }
}

/**
 * A batch of timestaps in nanoseconds, coerced to millisecond numbers.
 */
export class TimestampNanosecondBatch extends Int64Batch {
  /**
   * @param {number} index The value index
   */
  value(index) {
    // nanoseconds to milliseconds
    return divide(/** @type {bigint} */ (this.values[index]), 1000000n);
  }
}

/**
 * A batch of day/time intervals, returned as two-element 32-bit int arrays.
 * @extends {ArrayBatch<Int32Array>}
 */
export class IntervalDayTimeBatch extends ArrayBatch {
  /**
   * @param {number} index The value index
   * @returns {Int32Array}
   */
  value(index) {
    const values = /** @type {Int32Array} */ (this.values);
    return values.subarray(index << 1, (index + 1) << 1);
  }
}

/**
 * A batch of year/month intervals, returned as two-element 32-bit int arrays.
 * @extends {ArrayBatch<Int32Array>}
 */
export class IntervalYearMonthBatch extends ArrayBatch {
  /**
   * @param {number} index The value index
   */
  value(index) {
    const v = /** @type {number} */ (this.values[index]);
    return Int32Array.of(
      Math.trunc(v / 12), // years
      Math.trunc(v % 12)  // months
    );
  }
}

/**
 * A batch of month/day/nanosecond intervals, returned as three-element arrays.
 * @extends {ArrayBatch<Float64Array>}
 */
export class IntervalMonthDayNanoBatch extends ArrayBatch {
  /**
   * @param {number} index The value index
   */
  value(index) {
    const values = /** @type {Uint8Array} */ (this.values);
    const base = index << 2;
    return Float64Array.of(
      readInt32(values, base),
      readInt32(values, base + 4),
      readInt64AsNum(values, base + 8)
    );
  }
}

const offset32 = ({values, offsets}, index) => values.subarray(offsets[index], offsets[index + 1]);
const offset64 = ({values, offsets}, index) => values.subarray(toNumber(offsets[index]), toNumber(offsets[index + 1]));

/**
 * A batch of binary blobs with variable offsets, returned as byte buffers of
 * unsigned 8-bit integers. The offsets are 32-bit ints.
 * @extends {ArrayBatch<Uint8Array>}
 */
export class BinaryBatch extends ArrayBatch {
  /**
   * @param {number} index
   * @returns {Uint8Array}
   */
  value(index) {
    return offset32(this, index);
  }
}

/**
 * A batch of binary blobs with variable offsets, returned as byte buffers of
 * unsigned 8-bit integers. The offsets are 64-bit ints. Value extraction will
 * fail if an offset exceeds `Number.MAX_SAFE_INTEGER`.
 * @extends {ArrayBatch<Uint8Array>}
 */
export class LargeBinaryBatch extends ArrayBatch {
  /**
   * @param {number} index
   * @returns {Uint8Array}
   */
  value(index) {
    return offset64(this, index);
  }
}

/**
 * A batch of UTF-8 strings with variable offsets. The offsets are 32-bit ints.
 * @extends {ArrayBatch<string>}
 */
export class Utf8Batch extends ArrayBatch {
  /**
   * @param {number} index
   */
  value(index) {
    return decodeUtf8(offset32(this, index));
  }
}

/**
 * A batch of UTF-8 strings with variable offsets. The offsets are 64-bit ints.
 * Value extraction will fail if an offset exceeds `Number.MAX_SAFE_INTEGER`.
 * @extends {ArrayBatch<string>}
 */
export class LargeUtf8Batch extends ArrayBatch {
  /**
   * @param {number} index
   */
  value(index) {
    return decodeUtf8(offset64(this, index));
  }
}

/**
 * A batch of list (array) values of variable length. The list offsets are
 * 32-bit ints.
 * @template V
 * @extends {ArrayBatch<import('./types.js').ValueArray<V>>}
 */
export class ListBatch extends ArrayBatch {
  /**
   * @param {number} index
   * @returns {import('./types.js').ValueArray<V>}
   */
  value(index) {
    const offsets = /** @type {Int32Array} */ (this.offsets);
    return this.children[0].slice(offsets[index], offsets[index + 1]);
  }
}

/**
 * A batch of list (array) values of variable length. The list offsets are
 * 64-bit ints. Value extraction will fail if an offset exceeds
 * `Number.MAX_SAFE_INTEGER`.
 * @template V
 * @extends {ArrayBatch<import('./types.js').ValueArray<V>>}
 */
export class LargeListBatch extends ArrayBatch {
  /**
   * @param {number} index
   * @returns {import('./types.js').ValueArray<V>}
   */
  value(index) {
    const offsets = /** @type {BigInt64Array} */ (this.offsets);
    return this.children[0].slice(toNumber(offsets[index]), toNumber(offsets[index + 1]));
  }
}

/**
 * A batch of binary blobs of fixed size, returned as byte buffers of unsigned
 * 8-bit integers.
 * @extends {ArrayBatch<Uint8Array>}
 */
export class FixedBatch extends ArrayBatch {
  /**
   * @param {number} index
   * @returns {Uint8Array}
   */
  value(index) {
    const { stride } = /** @type {import('./types.js').FixedSizeBinaryType} */ (this.type);
    const values = /** @type {Uint8Array} */ (this.values);
    return values.subarray(index * stride, (index + 1) * stride);
  }
}

/**
 * A batch of list (array) values of fixed length.
 * @template V
 * @extends {ArrayBatch<import('./types.js').ValueArray<V>>}
 */
export class FixedListBatch extends ArrayBatch {
  /**
   * @param {number} index
   * @returns {import('./types.js').ValueArray<V>}
   */
  value(index) {
    const { type, children } = this;
    const { stride } = /** @type {import('./types.js').FixedSizeListType} */ (type);
    return children[0].slice(index * stride, (index + 1) * stride);
  }
}

/**
 * Extract Map key-value pairs from parallel child batches.
 */
function pairs({ children, offsets }, index) {
  const [ keys, vals ] = children[0].children;
  const start = offsets[index];
  const end = offsets[index + 1];
  const entries = [];
  for (let i = start; i < end; ++i) {
    entries.push([keys.at(i), vals.at(i)]);
  }
  return entries;
}

/**
 * A batch of map (key, value) values. The map is represented as a list of
 * key-value structs.
 * @template K, V
 * @extends {ArrayBatch<[K, V][]>}
 */
export class MapEntryBatch extends ArrayBatch {
  /**
   * Return the value at the given index.
   * @param {number} index The value index.
   * @returns {[K, V][]} The map entries as an array of [key, value] arrays.
   */
  value(index) {
    return /** @type {[K, V][]} */ (pairs(this, index));
  }
}

/**
 * A batch of map (key, value) values. The map is represented as a list of
 * key-value structs.
 * @template K, V
 * @extends {ArrayBatch<Map<K, V>>}
 */
export class MapBatch extends ArrayBatch {
  /**
   * Return the value at the given index.
   * @param {number} index The value index.
   * @returns {Map<K, V>} The map value.
   */
  value(index) {
    return new Map(/** @type {[K, V][]} */ (pairs(this, index)));
  }
}

/**
 * A batch of union-type values with a sparse layout, enabling direct
 * lookup from the child value batches.
 * @template T
 * @extends {ArrayBatch<T>}
 */
export class SparseUnionBatch extends ArrayBatch {
  constructor(options) {
    super(options);
    const { typeIds } = options.type;
    /** @type {Record<number,number>} */
    this.map = typeIds.reduce((map, id, i) => ((map[id] = i), map), {});
  }

  /**
   * @param {number} index The value index.
   */
  value(index) {
    const { typeIds, children, map } = this;
    return children[map[typeIds[index]]].at(index);
  }
}

/**
 * A batch of union-type values with a dense layout, reqiring offset
 * lookups from the child value batches.
 * @template T
 * @extends {SparseUnionBatch<T>}
 */
export class DenseUnionBatch extends SparseUnionBatch {
  /**
   * @param {number} index The value index.
   */
  value(index) {
    return super.value(/** @type {number} */ (this.offsets[index]));
  }
}

/**
 * A batch of struct values, containing a set of named properties.
 * Struct property values are extracted and returned as JS objects.
 * @extends {ArrayBatch<Record<string, any>>}
 */
export class StructBatch extends ArrayBatch {
  constructor(options) {
    super(options);
    const type = /** @type {import('./types.js').StructType} */ (this.type);
    this.names = type.children.map(child => child.name);
  }

  /**
   * @param {number} index The value index.
   * @returns {Record<string, any>}
   */
  value(index) {
    const { children, names } = this;
    const n = names.length;
    const struct = {};
    for (let i = 0; i < n; ++i) {
      struct[names[i]] = children[i].at(index);
    }
    return struct;
  }
}

/**
 * A batch of dictionary-encoded values.
 * @template T
 * @extends {ArrayBatch<T>}
 */
export class DictionaryBatch extends ArrayBatch {
  /**
   * Create a new dictionary batch.
   * @param {object} options Batch options.
   * @param {import('./types.js').DataType} options.type The field data type
   * @param {number} options.length The length of the batch
   * @param {number} options.nullCount The null value count
   * @param {Uint8Array} [options.validity] Validity bitmap buffer
   * @param {import('./types.js').TypedArray} [options.values] Values buffer
   * @param {import('./column.js').Column<T>} options.dictionary
   *  The dictionary of column values.
   */
  constructor(options) {
    super(options);
    this.cache = options.dictionary.cache();
  }
  /**
   * @param {number} index The value index.
   */
  value(index) {
    return this.cache[this.key(index)];
  }
  /**
   * @param {number} index The value index.
   * @returns {number} The dictionary key
   */
  key(index) {
    return /** @type {number} */ (this.values[index]);
  }
}
