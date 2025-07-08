/**
 * @import { Column } from '../column.js'
 * @import { TableBuilderOptions, TypedArray } from '../types.js'
 * @import { Table } from '../table.js'
 */
import { dictionaryContext } from './builders/dictionary.js';
import { columnFromArray } from './column-from-array.js';
import { tableFromColumns } from './table-from-columns.js';

/**
 * Create a new table from the provided arrays.
 * @param {[string, Array | TypedArray][]
 *  | Record<string, Array | TypedArray>} data
 *  The input data as a collection of named arrays.
 * @param {TableBuilderOptions} options
 *  Table builder options, including an optional type map.
 * @returns {Table} The new table.
 */
export function tableFromArrays(data, options = {}) {
  const { types = {}, ...opt } = options;
  const dicts = dictionaryContext();
  const entries = Array.isArray(data) ? data : Object.entries(data);
  const columns = entries.map(([name, array]) =>
    /** @type {[string, Column]} */ (
    [ name, columnFromArray(array, types[name], opt, dicts)]
  ));
  return tableFromColumns(columns, options.useProxy);
}
