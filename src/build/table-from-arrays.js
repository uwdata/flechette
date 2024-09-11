import { builderContext } from './builder.js';
import { columnFromArray } from './column-from-array.js';
import { tableFromColumns } from './table-from-columns.js';

/**
 * Create a new table from the provided arrays.
 * @param {[string, Array | import('../types.js').TypedArray][]
 *  | Record<string, Array | import('../types.js').TypedArray>} data
 *  The input data as a collection of named arrays.
 * @param {import('../types.js').TableBuilderOptions} options
 *  Table builder options, including an optional type map.
 * @returns {import('../table.js').Table} The new table.
 */
export function tableFromArrays(data, options = {}) {
  const { types = {}, ...opt } = options;
  const ctx = builderContext();
  const entries = Array.isArray(data) ? data : Object.entries(data);
  const columns = entries.map(([name, array]) =>
    /** @type {[string, import('../column.js').Column]} */ (
    [ name, columnFromArray(array, types[name], opt, ctx)]
  ));
  return tableFromColumns(columns);
}
