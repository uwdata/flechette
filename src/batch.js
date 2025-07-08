/**
 * @import { Column } from './column.js'
 * @import { DataType, DecimalType, IntegerArray, OffsetArray, TypedArray, TypedArrayConstructor, ValueArray } from './types.js'
 */
import { bisect, float64Array } from './util/arrays.js';
import { divide, fromDecimal128, fromDecimal256, fromDecimal64, toNumber } from './util/numbers.js';
import { decodeBit, readInt32, readInt64 } from './util/read.js';
import { decodeUtf8 } from './util/strings.js';
import { objectFactory, proxyFactory } from './util/struct.js';

/**
 * Check if the input is a batch that supports direct access to
 * binary data in the form of typed arrays.
 * @param {Batch<any>?} batch The data batch to check.
 * @returns {boolean} True if a direct batch, false otherwise.
 */
export function isDirectBatch(batch) {
  return batch instanceof DirectBatch;
}

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
   * @type {ArrayConstructor | TypedArrayConstructor | null}
   */
  static ArrayType = null;

  /**
   * Create a new column batch.
   * @param {object} options
   * @param {number} options.length The length of the batch
   * @param {number} options.nullCount The null value count
   * @param {DataType} options.type The data type.
   * @param {Uint8Array} [options.validity] Validity bitmap buffer
   * @param {TypedArray} [options.values] Values buffer
   * @param {OffsetArray} [options.offsets] Offsets buffer
   * @param {OffsetArray} [options.sizes] Sizes buffer
   * @param {Batch[]} [options.children] Children batches
   */
  constructor({
    length,
    nullCount,
    type,
    validity,
    values,
    offsets,
    sizes,
    children
  }) {
    this.length = length;
    this.nullCount = nullCount;
    this.type = type;
    this.validity = validity;
    this.values = values;
    this.offsets = offsets;
    this.sizes = sizes;
    this.children = children;

    // optimize access if this batch has no null values
    // some types (like union) may have null values in
    // child batches, but no top-level validity buffer
    if (!nullCount || !this.validity) {
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
   * Extract an array of values within the given index range. Unlike
   * Array.slice, all arguments are required and may not be negative indices.
   * @param {number} start The starting index, inclusive
   * @param {number} end The ending index, exclusive
   * @returns {ValueArray<T?>} The slice of values
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
    for (let i = 0; i < this.length; ++i) {
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
   * Create a new column batch with direct value array access.
   * @param {object} options
   * @param {number} options.length The length of the batch
   * @param {number} options.nullCount The null value count
   * @param {DataType} options.type The data type.
   * @param {Uint8Array} [options.validity] Validity bitmap buffer
   * @param {TypedArray} options.values Values buffer
   */
  constructor(options) {
    super(options);
    // underlying buffers may be padded, exceeding the logical batch length
    // we trim the values array so we can safely access it directly
    const { length, values } = this;
    this.values = values.subarray(0, length);
  }

  /**
   * Extract an array of values within the given index range. Unlike
   * Array.slice, all arguments are required and may not be negative indices.
   * When feasible, a zero-copy subarray of a typed array is returned.
   * @param {number} start The starting index, inclusive
   * @param {number} end The ending index, exclusive
   * @returns {ValueArray<T?>} The slice of values
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
  static ArrayType = float64Array;
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
 * @extends {NumberBatch}
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

/**
 * A batch of 32-bit decimal numbers, returned as converted 64-bit floating
 * point numbers. Number coercion may be lossy if the decimal precision can
 * not be represented in a 64-bit floating point format.
 * @extends {NumberBatch}
 */
export class Decimal32NumberBatch extends NumberBatch {
  constructor(options) {
    super(options);
    const { scale } = /** @type {DecimalType} */ (this.type);
    this.scale = 10 ** scale;
  }
  /**
   * @param {number} index The value index
   */
  value(index) {
    return /** @type {number} */(this.values[index]) / this.scale;
  }
}

/**
 * An abstract class for a batch of 64-, 128- or 256-bit decimal numbers,
 * accessed in strided BigUint64Arrays.
 * @template T
 * @extends {Batch<T>}
 */
export class DecimalBatch extends Batch {
  constructor(options) {
    super(options);
    const { bitWidth, scale } = /** @type {DecimalType} */ (this.type);
    this.decimal = bitWidth === 64 ? fromDecimal64
      : bitWidth === 128 ? fromDecimal128
      : fromDecimal256;
    this.scale = 10n ** BigInt(scale);
  }
}

/**
 * A batch of 64-, 128- or 256-bit decimal numbers, returned as converted
 * 64-bit floating point numbers. Number coercion may be lossy if the decimal
 * precision can not be represented in a 64-bit floating point format.
 * @extends {DecimalBatch<number>}
 */
export class DecimalNumberBatch extends DecimalBatch {
  static ArrayType = float64Array;
  /**
   * @param {number} index The value index
   */
  value(index) {
    return divide(
      this.decimal(/** @type {BigUint64Array} */ (this.values), index),
      this.scale
    );
  }
}

/**
 * A batch of 64-, 128- or 256-bit decimal numbers, returned as scaled
 * bigint values, such that all fractional digits have been shifted
 * to integer places by the decimal type scale factor.
 * @extends {DecimalBatch<bigint>}
 */
export class DecimalBigIntBatch extends DecimalBatch {
  static ArrayType = Array;
  /**
   * @param {number} index The value index
   */
  value(index) {
    return this.decimal(/** @type {BigUint64Array} */ (this.values), index);
  }
}

/**
 * A batch of date or timestamp values that are coerced to UNIX epoch timestamps
 * and returned as JS Date objects. This batch wraps a source batch that provides
 * timestamp values.
 * @extends {ArrayBatch<Date>}
 */
export class DateBatch extends ArrayBatch {
  /**
   * Create a new date batch.
   * @param {Batch<number>} batch A batch of timestamp values.
   */
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
 * A batch of month/day/nanosecond intervals, returned as three-element arrays.
 * @extends {ArrayBatch<Float64Array>}
 */
export class IntervalMonthDayNanoBatch extends ArrayBatch {
  /**
   * @param {number} index The value index
   */
  value(index) {
    const values = /** @type {Uint8Array} */ (this.values);
    const base = index << 4;
    return Float64Array.of(
      readInt32(values, base),
      readInt32(values, base + 4),
      readInt64(values, base + 8)
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
 * @extends {ArrayBatch<ValueArray<V>>}
 */
export class ListBatch extends ArrayBatch {
  /**
   * @param {number} index
   * @returns {ValueArray<V>}
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
 * @extends {ArrayBatch<ValueArray<V>>}
 */
export class LargeListBatch extends ArrayBatch {
  /**
   * @param {number} index
   * @returns {ValueArray<V>}
   */
  value(index) {
    const offsets = /** @type {BigInt64Array} */ (this.offsets);
    return this.children[0].slice(toNumber(offsets[index]), toNumber(offsets[index + 1]));
  }
}

/**
 * A batch of list (array) values of variable length. The list offsets and
 * sizes are 32-bit ints.
 * @template V
 * @extends {ArrayBatch<ValueArray<V>>}
 */
export class ListViewBatch extends ArrayBatch {
  /**
   * @param {number} index
   * @returns {ValueArray<V>}
   */
  value(index) {
    const a = /** @type {number} */ (this.offsets[index]);
    const b = a + /** @type {number} */ (this.sizes[index]);
    return this.children[0].slice(a, b);
  }
}

/**
 * A batch of list (array) values of variable length. The list offsets and
 * sizes are 64-bit ints. Value extraction will fail if an offset or size
 * exceeds `Number.MAX_SAFE_INTEGER`.
 * @template V
 * @extends {ArrayBatch<ValueArray<V>>}
 */
export class LargeListViewBatch extends ArrayBatch {
  /**
   * @param {number} index
   * @returns {ValueArray<V>}
   */
  value(index) {
    const a = /** @type {bigint} */ (this.offsets[index]);
    const b = a + /** @type {bigint} */ (this.sizes[index]);
    return this.children[0].slice(toNumber(a), toNumber(b));
  }
}

/**
 * A batch with a fixed stride.
 * @template T
 * @extends {ArrayBatch<T>}
 */
class FixedBatch extends ArrayBatch {
  constructor(options) {
    super(options);
    /** @type {number} */
    // @ts-ignore
    this.stride = this.type.stride;
  }
}

/**
 * A batch of binary blobs of fixed size, returned as byte buffers of unsigned
 * 8-bit integers.
 * @extends {FixedBatch<Uint8Array>}
 */
export class FixedBinaryBatch extends FixedBatch {
  /**
   * @param {number} index
   * @returns {Uint8Array}
   */
  value(index) {
    const { stride, values } = this;
    return /** @type {Uint8Array} */ (values)
      .subarray(index * stride, (index + 1) * stride);
  }
}

/**
 * A batch of list (array) values of fixed length.
 * @template V
 * @extends {FixedBatch<ValueArray<V>>}
 */
export class FixedListBatch extends FixedBatch {
  /**
   * @param {number} index
   * @returns {ValueArray<V>}
   */
  value(index) {
    const { children, stride } = this;
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
  /**
   * Create a new column batch.
   * @param {object} options
   * @param {number} options.length The length of the batch
   * @param {number} options.nullCount The null value count
   * @param {DataType} options.type The data type.
   * @param {Uint8Array} [options.validity] Validity bitmap buffer
   * @param {Int32Array} [options.offsets] Offsets buffer
   * @param {Batch[]} options.children Children batches
   * @param {Int8Array} options.typeIds Union type ids buffer
   * @param {Record<string, number>} options.map A typeId to children index map
   */
  constructor({ typeIds, ...options }) {
    super(options);
    /** @type {Int8Array} */
    this.typeIds = typeIds;
    /** @type {Record<string, number>} */
    // @ts-ignore
    this.typeMap = this.type.typeMap;
  }

  /**
   * @param {number} index The value index.
   */
  value(index, offset = index) {
    const { typeIds, children, typeMap } = this;
    return children[typeMap[typeIds[index]]].at(offset);
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
    return super.value(index, /** @type {number} */ (this.offsets[index]));
  }
}

/**
 * A batch of struct values, containing a set of named properties.
 * Struct property values are extracted and returned as JS objects.
 * @extends {ArrayBatch<Record<string, any>>}
 */
export class StructBatch extends ArrayBatch {
  constructor(options, factory = objectFactory) {
    super(options);
    /** @type {string[]} */
    // @ts-ignore
    this.names = this.type.children.map(child => child.name);
    this.factory = factory(this.names, this.children);
  }

  /**
   * @param {number} index The value index.
   * @returns {Record<string, any>}
   */
  value(index) {
    return this.factory(index);
  }
}

/**
 * A batch of struct values, containing a set of named properties.
 * Structs are returned as proxy objects that extract data directly
 * from underlying Arrow batches.
 * @extends {StructBatch}
 */
export class StructProxyBatch extends StructBatch {
  constructor(options) {
    super(options, proxyFactory);
  }
}

/**
 * A batch of run-end-encoded values.
 * @template T
 * @extends {ArrayBatch<T>}
 */
export class RunEndEncodedBatch extends ArrayBatch {
  /**
   * @param {number} index The value index.
   */
  value(index) {
    const [ { values: runs }, vals ] = this.children;
    return vals.at(
      bisect(/** @type {IntegerArray} */(runs), index)
    );
  }
}

/**
 * A batch of dictionary-encoded values.
 * @template T
 * @extends {ArrayBatch<T>}
 */
export class DictionaryBatch extends ArrayBatch {
  /**
   * Register the backing dictionary. Dictionaries are added
   * after batch creation as the complete dictionary may not
   * be finished across multiple record batches.
   * @param {Column<T>} dictionary
   * The dictionary of column values.
   */
  setDictionary(dictionary) {
    this.dictionary = dictionary;
    this.cache = dictionary.cache();
    return this;
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

/**
 * @template T
 * @extends {ArrayBatch<T>}
 */
class ViewBatch extends ArrayBatch {
  /**
   * Create a new view batch.
   * @param {object} options Batch options.
   * @param {number} options.length The length of the batch
   * @param {number} options.nullCount The null value count
   * @param {DataType} options.type The data type.
   * @param {Uint8Array} [options.validity] Validity bitmap buffer
   * @param {Uint8Array} options.values Values buffer
   * @param {Uint8Array[]} options.data View data buffers
   */
  constructor({ data, ...options }) {
    super(options);
    this.data = data;
  }

  /**
   * Get the binary data at the provided index.
   * @param {number} index The value index.
   * @returns {Uint8Array}
   */
  view(index) {
    const { values, data } = this;
    const offset = index << 4; // each entry is 16 bytes
    let start = offset + 4;
    let buf = /** @type {Uint8Array} */ (values);
    const length = readInt32(buf, offset);
    if (length > 12) {
      // longer strings are in a data buffer
      start = readInt32(buf, offset + 12);
      buf = data[readInt32(buf, offset + 8)];
    }
    return buf.subarray(start, start + length);
  }
}

/**
 * A batch of binary blobs from variable data buffers, returned as byte
 * buffers of unsigned 8-bit integers.
 * @extends {ViewBatch<Uint8Array>}
 */
export class BinaryViewBatch extends ViewBatch {
  /**
   * @param {number} index The value index.
   */
  value(index) {
    return this.view(index);
  }
}

/**
 * A batch of UTF-8 strings from variable data buffers.
 * @extends {ViewBatch<string>}
 */
export class Utf8ViewBatch extends ViewBatch {
  /**
   * @param {number} index The value index.
   */
  value(index) {
    return decodeUtf8(this.view(index));
  }
}
