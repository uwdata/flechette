/**
 * @import { ColumnBuilderOptions, DataType, TypedArray, TypedArrayConstructor } from '../types.js'
 * @import { dictionaryContext } from './builders/dictionary.js'
 */
import { float32Array, float64Array, int16Array, int32Array, int64Array, int8Array, isInt64ArrayType, isTypedArray, uint16Array, uint32Array, uint64Array, uint8Array } from '../util/arrays.js';
import { DirectBatch, Int64Batch } from '../batch.js';
import { Column } from '../column.js';
import { float32, float64, int16, int32, int64, int8, uint16, uint32, uint64, uint8 } from '../data-types.js';
import { columnFromValues } from './column-from-values.js';

/**
 * Create a new column from a provided data array.
 * @template T
 * @param {Array | TypedArray} array The input data.
 * @param {DataType} [type] The data type.
 *  If not specified, type inference is attempted.
 * @param {ColumnBuilderOptions} [options]
 *  Builder options for the generated column.
 * @param {ReturnType<dictionaryContext>} [dicts]
 *  Builder context object, for internal use only.
 * @returns {Column<T>} The generated column.
 */
export function columnFromArray(array, type, options = {}, dicts) {
  return !type && isTypedArray(array)
    ? columnFromTypedArray(array, options)
    : columnFromValues(v => array.forEach(v), type, options, dicts);
}

/**
 * Create a new column from a typed array input.
 * @template T
 * @param {TypedArray} values The input data.
 * @param {ColumnBuilderOptions} options
 *  Builder options for the generated column.
 * @returns {Column<T>} The generated column.
 */
function columnFromTypedArray(values, { maxBatchRows, useBigInt }) {
  const arrayType = /** @type {TypedArrayConstructor} */ (
    values.constructor
  );
  const type = typeForTypedArray(arrayType);
  const length = values.length;
  const limit = Math.min(maxBatchRows || Infinity, length);
  const numBatches = Math.floor(length / limit);

  const batches = [];
  const batchType = isInt64ArrayType(arrayType) && !useBigInt ? Int64Batch : DirectBatch;
  const add = (start, end) => batches.push(new batchType({
    length: end - start,
    nullCount: 0,
    type,
    validity: new uint8Array(0),
    values: values.subarray(start, end)
  }));

  let idx = 0;
  for (let i = 0; i < numBatches; ++i) add(idx, idx += limit);
  if (idx < length) add(idx, length);

  return new Column(batches);
}

/**
 * Return an Arrow data type for a given typed array type.
 * @param {TypedArrayConstructor} arrayType The typed array type.
 * @returns {DataType} The data type.
 */
function typeForTypedArray(arrayType) {
  switch (arrayType) {
    case float32Array: return float32();
    case float64Array: return float64();
    case int8Array: return int8();
    case int16Array: return int16();
    case int32Array: return int32();
    case int64Array: return int64();
    case uint8Array: return uint8();
    case uint16Array: return uint16();
    case uint32Array: return uint32();
    case uint64Array: return uint64();
  }
}
