import { float32Array, float64Array, int16Array, int32Array, int64Array, int8Array, isInt64ArrayType, isTypedArray, uint16Array, uint32Array, uint64Array, uint8Array } from '../util/arrays.js';
import { DirectBatch, Int64Batch, NullBatch } from '../batch.js';
import { Column } from '../column.js';
import { float32, float64, int16, int32, int64, int8, uint16, uint32, uint64, uint8 } from '../data-types.js';
import { inferType } from './infer-type.js';
import { builder, builderContext } from './builder.js';
import { Type } from '../constants.js';

/**
 * Create a new column from a provided data array.
 * @template T
 * @param {Array | import('../types.js').TypedArray} data The input data.
 * @param {import('../types.js').DataType} [type] The data type.
 *  If not specified, type inference is attempted.
 * @param {import('../types.js').ColumnBuilderOptions} [options]
 *  Builder options for the generated column.
 * @param {ReturnType<import('./builder.js').builderContext>} [ctx]
 *  Builder context object, for internal use only.
 * @returns {Column<T>} The generated column.
 */
export function columnFromArray(data, type, options = {}, ctx) {
  if (!type) {
    if (isTypedArray(data)) {
      return columnFromTypedArray(data, options);
    } else {
      type = inferType(data);
    }
  }
  return columnFromValues(data, type, options, ctx);
}

/**
 * Create a new column from a typed array input.
 * @template T
 * @param {import('../types.js').TypedArray} values
 * @param {import('../types.js').ColumnBuilderOptions} options
 *  Builder options for the generated column.
 * @returns {Column<T>} The generated column.
 */
function columnFromTypedArray(values, { maxBatchRows, useBigInt }) {
  const arrayType = values.constructor;
  const type = typeForTypedArray(arrayType);
  const length = values.length;
  const limit = Math.min(maxBatchRows || Infinity, length);
  const numBatches = Math.floor(length / limit);

  const batches = [];
  const batchType = isInt64ArrayType(arrayType) && !useBigInt ? Int64Batch : DirectBatch;
  const add = (start, end) => batches.push(new batchType({
    length: limit,
    nullCount: 0,
    type,
    values: values.subarray(start, end)
  }));

  let idx = 0;
  for (let i = 0; i < numBatches; ++i) add(idx, idx += limit);
  if (idx < length) add(idx, length);

  return new Column(batches);
}

/**
 * Build a column by iterating over the provided values array.
 * @template T
 * @param {Array | import('../types.js').TypedArray} values The input data.
 * @param {import('../types.js').DataType} type The column data type.
 * @param {import('../types.js').ColumnBuilderOptions} [options]
 *  Builder options for the generated column.
 * @param {ReturnType<import('./builder.js').builderContext>} [ctx]
 *  Builder context object, for internal use only.
 * @returns {Column<T>} The generated column.
 */
function columnFromValues(values, type, options, ctx) {
  const { maxBatchRows, ...opt } = options;
  const length = values.length;
  const limit = Math.min(maxBatchRows || Infinity, length);

  // if null type, generate batches and exit early
  if (type.typeId === Type.Null) {
    return new Column(nullBatches(type, length, limit));
  }

  const data = [];
  ctx ??= builderContext(opt);
  const b = builder(type, ctx).init();
  const next = b => data.push(b.batch());
  const numBatches = Math.floor(length / limit);

  let idx = 0;
  let row = 0;
  for (let i = 0; i < numBatches; ++i) {
    for (row = 0; row < limit; ++row) {
      b.set(values[idx++], row);
    }
    next(b);
  }
  for (row = 0; idx < length; ++idx) {
    b.set(values[idx], row++);
  }
  if (row) next(b);

  // resolve dictionaries
  ctx.finish();

  return new Column(data);
}

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

function nullBatches(type, length, limit) {
  const data = [];
  const batch = length => new NullBatch({ length, nullCount: length, type });
  const numBatches = Math.floor(length / limit);
  for (let i = 0; i < numBatches; ++i) {
    data.push(batch(limit));
  }
  const rem = length % limit;
  if (rem) data.push(batch(rem));
  return data;
}