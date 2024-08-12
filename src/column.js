import { DirectBatch } from './batch.js';

/**
 * Build up a column from batches.
 * @param {string} name The column name.
 * @param {import('./types.js').DataType} type The column data type.
 */
export function columnBuilder(name, type) {
  let data = [];
  return {
    type,
    add(batch) { data.push(batch); return this; },
    clear: () => data = [],
    done: () => new Column(name, type, data)
  };
}

/**
 * A data column. A column provides a view over one or more value batches,
 * each drawn from an Arrow record batch. While this class supports random
 * access to column values by integer index; however, extracting arrays using
 * `toArray()` or iterating over values (`for (const value of column) {...}`)
 * provide more efficient ways for bulk access or scanning.
 * @template T
 */
export class Column {
  /**
   * Create a new column instance.
   * @param {string} name The name of the column.
   * @param {import('./types.js').DataType} type The column data type.
   * @param {import('./batch.js').Batch<T>[]} data The value batches.
   */
  constructor(name, type, data) {
    /**
     * @type {string}
     * @readonly
     */
    this.name = name;
    /**
     * @type {import('./types.js').DataType}
     * @readonly
     */
    this.type = type;
    /**
     * @type {number}
     * @readonly
     */
    this.length = data.reduce((m, c) => m + c.length, 0);
    /**
     * @type {number}
     * @readonly
     */
    this.nullCount = data.reduce((m, c) => m + c.nullCount, 0);
    /**
     * @type {readonly import('./batch.js').Batch<T>[]}
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
   * @param {number} index The index
   * @returns {T | null} The value.
   */
  at(index) {
    // NOTE: if there is only one batch, this method is replaced with an
    // optimized version within the Column constructor.
    const { data, offsets } = this;

    // binary search for batch index
    let a = 0;
    let b = offsets.length;
    do {
      const mid = (a + b) >>> 1;
      if (offsets[mid] <= index) a = mid + 1;
      else b = mid;
    } while (a < b);

    // returns undefined if index is out of range
    return data[--a]?.at(index - offsets[a]);
  }

  /**
   * Extract column values into a single array instance. When possible,
   * a zero-copy subarray of the input Arrow data is returned.
   * @returns {import('./types.js').ValueArray<T?>}
   */
  toArray() {
    const { length, nullCount, data } = this;
    const copy = !nullCount && isDirect(data);

    if (copy && data.length === 1) {
      // use batch array directly
      // @ts-ignore
      return data[0].values;
    }

    // determine output array type
    const ArrayType = nullCount > 0 ? Array
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

function isDirect(data) {
  return data.length && data[0] instanceof DirectBatch;
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
