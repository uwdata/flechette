/**
 * @import { ColumnBuilderOptions, ColumnBuilderType, DataType, NullType } from '../types.js'
 * @import { dictionaryContext } from './builders/dictionary.js'
 */
import { NullBatch } from '../batch.js';
import { Column } from '../column.js';
import { inferType } from './infer-type.js';
import { builder, builderContext } from './builder.js';
import { Type } from '../constants.js';
import { float, int } from '../data-types.js';
import { isIterable } from '../util/objects.js';

/**
 * Create a new column by iterating over provided values.
 * @template T
 * @param {Iterable | ((callback: (value: any) => void) => void)} values
 *  Either an iterable object or a visitor function that applies a callback
 *  to successive data values (akin to Array.forEach).
 * @param {ColumnBuilderType} [type] The data type.
 * @param {ColumnBuilderOptions} [options]
 *  Builder options for the generated column.
 * @param {ReturnType<typeof dictionaryContext>} [dicts]
 *  Dictionary context object, for internal use only.
 * @returns {Column<T>} The generated column.
 */
export function columnFromValues(values, type, options = {}, dicts) {
  const visit = isIterable(values)
    ? callback => { for (const value of values) callback(value); }
    : values;

  const dataType = normalizeType(type ?? inferType(visit));
  const { maxBatchRows = Infinity, ...opt } = options;
  let data;

  if (dataType.typeId === Type.Null) {
    let length = 0;
    visit(() => ++length);
    data = nullBatches(dataType, length, maxBatchRows);
  } else {
    const ctx = builderContext(opt, dicts);
    const b = builder(dataType, ctx).init();
    const next = b => data.push(b.batch());
    data = [];

    let row = 0;
    visit(value => {
      b.set(value, row++);
      if (row >= maxBatchRows) {
        next(b);
        row = 0;
      }
    });
    if (row) next(b);

    // resolve dictionaries
    ctx.finish();
  }

  return new Column(data, dataType);
}

/**
 * Normalize Arrow JS numeric descriptors without mutating the input. Derive
 * the backing array from the logical type instead of trusting ArrayType.
 * @param {ColumnBuilderType} type
 * @returns {DataType}
 */
function normalizeType(type) {
  if (type.typeId === Type.Int && !('values' in type) && 'isSigned' in type) {
    return int(type.bitWidth, type.isSigned);
  }
  if (type.typeId === Type.Float && !('values' in type) && 'precision' in type) {
    return float(type.precision);
  }
  // The numeric input types have broad typeId declarations to accommodate
  // Arrow JS typings. All other inputs use Flechette's representation.
  return /** @type {DataType} */ (type);
}

/**
 * Create null batches with the given batch size limit.
 * @param {NullType} type The null data type.
 * @param {number} length The total column length.
 * @param {number} limit The maximum batch size.
 * @returns {NullBatch[]} The null batches.
 */
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
