import { Endianness, Type, Version } from '../constants.js';
import { field } from '../data-types.js';
import { Table } from '../table.js';

/**
 * Create a new table from a collection of columns. Columns are assumed
 * to have the same record batch sizes and consistent dictionary ids.
 * @param {[string, import('../column.js').Column][]
*  | Record<string, import('../column.js').Column>} data The columns,
*  as an object with name keys, or an array of [name, column] pairs.
* @returns {Table} The new table.
*/
export function tableFromColumns(data) {
  const fields = [];
  const dictionaryTypes = new Map;
  const entries = Array.isArray(data) ? data : Object.entries(data);
  const length = entries[0]?.[1].length;
  const columns = entries.map(([name, col]) => {
    if (col.length !== length) {
      throw new Error('All columns must have the same length.');
    }
    const type = col.type;
    if (type.typeId === Type.Dictionary) {
      const dict = dictionaryTypes.get(type.id);
      if (dict && dict !== type.dictionary) {
        throw new Error('Same id used across different dictionaries.');
      }
      dictionaryTypes.set(type.id, type.dictionary);
    }
    fields.push(field(name, col.type));
    return col;
  });

  const schema = {
    version: Version.V5,
    endianness: Endianness.Little,
    fields,
    metadata: null,
    dictionaryTypes
  };

  return new Table(schema, columns);
}
