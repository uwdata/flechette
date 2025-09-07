/**
 * @import { Batch } from './batch.js'
 * @import { DataType, ValueArray } from './types.js'
 */
import { bisect } from './util/arrays.js';
import { isDirectBatch } from './batch.js';

/**
 * Build up a column from batches.
 */
export function columnBuilder(type) {
  let data = [];
  return {
    add(batch) { data.push(batch); return this; },
    clear: () => data = [],
    done: () => new Column(data, type)
  };
}

/**
 * A data column. A column provides a view over one or more value batches,
 * each drawn from an Arrow record batch. This class supports random access
 * to column values by integer index; however, extracting arrays using
 * `toArray()` or iterating over values (`for (const value of column) {...}`)
 * provide more efficient ways for bulk access or scanning.
 * @template T
 */
export class Column {
  /**
   * Create a new column instance.
   * @param {Batch<T>[]} data The value batches.
   * @param {DataType} [type] The column data type.
   *  If not specified, the type is extracted from the batches.
   */
  constructor(data, type = data[0]?.type) {
    /**
     * The column data type.
     * @type {DataType}
     * @readonly
     */
    this.type = type;
    /**
     * The column length.
     * @type {number}
     * @readonly
     */
    this.length = data.reduce((m, c) => m + c.length, 0);
    /**
     * The count of null values in the column.
     * @type {number}
     * @readonly
     */
    this.nullCount = data.reduce((m, c) => m + c.nullCount, 0);
    /**
     * An array of column data batches.
     * @type {readonly Batch<T>[]}
     * @readonly
     */
    this.data = data;

    const n = data.length;
    const offsets = new Int32Array(n + 1);
    if (n === 1) {
      const [ batch ] = data;
      offsets[1] = batch.length;
      // optimize access to single batch
      this.at = index => batch.at(index);
    } else {
      for (let i = 0, s = 0; i < n; ++i) {
        offsets[i + 1] = (s += data[i].length);
      }
    }

    /**
     * Index offsets for data batches.
     * Used to map a column row index to a batch-specific index.
     * @type {Int32Array}
     * @readonly
     */
    this.offsets = offsets;
  }

  /**
   * Provide an informative object string tag.
   */
  get [Symbol.toStringTag]() {
    return 'Column';
  }

  /**
   * Return an iterator over the values in this column.
   * @returns {Iterator<T?>}
   */
  [Symbol.iterator]() {
    const data = this.data;
    return data.length === 1
      ? data[0][Symbol.iterator]()
      : batchedIterator(data);
  }

  /**
   * Return the column value at the given index. If a column has multiple
   * batches, this method performs binary search over the batch lengths to
   * determine the batch from which to retrieve the value. The search makes
   * lookup less efficient than a standard array access. If making a full
   * scan of a column, consider extracting arrays via `toArray()` or using an
   * iterator (`for (const value of column) {...}`).
   * @param {number} index The row index.
   * @returns {T | null} The value.
   */
  at(index) {
    // NOTE: if there is only one batch, this method is replaced with an
    // optimized version in the Column constructor.
    const { data, offsets } = this;
    const i = bisect(offsets, index) - 1;
    return data[i]?.at(index - offsets[i]); // undefined if out of range
  }

  /**
   * Return the column value at the given index. This method is the same as
   * `at()` and is provided for better compatibility with Apache Arrow JS.
   * @param {number} index The row index.
   * @returns {T | null} The value.
   */
  get(index) {
    return this.at(index);
  }

  /**
   * Extract column values into a single array instance. When possible,
   * a zero-copy subarray of the input Arrow data is returned.
   * @returns {ValueArray<T?>}
   */
  toArray() {
    const { length, nullCount, data } = this;
    const copy = !nullCount && isDirectBatch(data[0]);
    const n = data.length;

    if (copy && n === 1) {
      // use batch array directly
      // @ts-ignore
      return data[0].values;
    }

    // determine output array type
    const ArrayType = !n || nullCount > 0 ? Array
      // @ts-ignore
      : (data[0].constructor.ArrayType ?? data[0].values.constructor);

    const array = new ArrayType(length);
    return copy ? copyArray(array, data) : extractArray(array, data);
  }

  /**
   * Return an array of cached column values.
   * Used internally to accelerate dictionary types.
   */
  cache() {
    return this._cache ?? (this._cache = this.toArray());
  }
}

function *batchedIterator(data) {
  for (let i = 0; i < data.length; ++i) {
    const iter = data[i][Symbol.iterator]();
    for (let next = iter.next(); !next.done; next = iter.next()) {
      yield next.value;
    }
  }
}

function copyArray(array, data) {
  for (let i = 0, offset = 0; i < data.length; ++i) {
    const { values } = data[i];
    array.set(values, offset);
    offset += values.length;
  }
  return array;
}

function extractArray(array, data) {
  let index = -1;
  for (let i = 0; i < data.length; ++i) {
    const batch = data[i];
    for (let j = 0; j < batch.length; ++j) {
      array[++index] = batch.at(j);
    }
  }
  return array;
}
