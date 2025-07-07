/**
 * @import { Column } from '../column.js'
 */
import { Endianness, Version } from '../constants.js';
import { field } from '../data-types.js';
import { Table } from '../table.js';

/**
 * Create a new table from a collection of columns. Columns are assumed
 * to have the same record batch sizes.
 * @param {[string, Column][] | Record<string, Column>} data The columns,
 *  as an object with name keys, or an array of [name, column] pairs.
 * @param {boolean} [useProxy] Flag indicating if row proxy
 *  objects should be used to represent table rows (default `false`).
 * @returns {Table} The new table.
 */
export function tableFromColumns(data, useProxy) {
  const fields = [];
  const entries = Array.isArray(data) ? data : Object.entries(data);
  const length = entries[0]?.[1].length;

  const columns = entries.map(([name, col]) => {
    if (col.length !== length) {
      throw new Error('All columns must have the same length.');
    }
    fields.push(field(name, col.type));
    return col;
  });

  const schema = {
    version: Version.V5,
    endianness: Endianness.Little,
    fields,
    metadata: null
  };

  return new Table(schema, columns, useProxy);
}
