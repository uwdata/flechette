import { NullBatch } from '../batch.js';
import { Column } from '../column.js';
import { inferType } from './infer-type.js';
import { builder, builderContext } from './builder.js';
import { Type } from '../constants.js';

/**
 * Create a new column by iterating over provided values.
 * @template T
 * @param {number} length The input data length.
 * @param {(visitor: (value: any) => void) => void} visit
 *  A function that applies a callback to successive data values.
 * @param {import('../types.js').DataType} type The data type.
 * @param {import('../types.js').ColumnBuilderOptions} [options]
 *  Builder options for the generated column.
 * @param {ReturnType<
 *    import('./builders/dictionary.js').dictionaryContext
 *  >} [dicts] Builder context object, for internal use only.
 * @returns {Column<T>} The generated column.
 */
export function columnFromValues(length, visit, type, options, dicts) {
  type ??= inferType(visit);
  const { maxBatchRows, ...opt } = options;
  const limit = Math.min(maxBatchRows || Infinity, length);

  // if null type, generate batches and exit early
  if (type.typeId === Type.Null) {
    return new Column(nullBatches(type, length, limit), type);
  }

  const ctx = builderContext(opt, dicts);
  const b = builder(type, ctx).init();
  const data = [];
  const next = b => data.push(b.batch());

  let row = 0;
  visit(value => {
    b.set(value, row++);
    if (row >= limit) {
      next(b);
      row = 0;
    }
  });
  if (row) next(b);

  // resolve dictionaries
  ctx.finish();

  return new Column(data, type);
}

/**
 * Create null batches with the given batch size limit.
 * @param {import('../types.js').NullType} type The null data type.
 * @param {number} length The total column length.
 * @param {number} limit The maximum batch size.
 * @returns {import('../batch.js').NullBatch[]} The null batches.
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
