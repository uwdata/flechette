import { DuckDB } from '@uwdata/mosaic-duckdb';

const db = new DuckDB();

export async function arrowQuery(sql, cleanup) {
  const ipc = await db.arrowBuffer(sql);
  if (cleanup) await db.exec(cleanup);
  return ipc;
}

export function arrowFromDuckDB(values, type) {
  const sql = values
    .map(stringify)
    .map(v => `SELECT ${v}${type ? `::${type}` : ''} AS value`).join(' UNION ALL ');
  return arrowQuery(sql);
}

function stringify(value) {
  switch (typeof value) {
    case 'string': return `'${value}'`;
    case 'object':
      if (value == null) {
        return 'NULL'
      } else if (Array.isArray(value)) {
        return `[${value.map(stringify).join(', ')}]`;
      } else if (value instanceof Date) {
        return value.toISOString();
      } else if (value instanceof Map) {
        return `MAP ${stringifyObject(Array.from(value.entries()))}`;
      } else {
        return stringifyObject(Object.entries(value));
      }
    default: return `${value}`;
  }
}

function stringifyObject(entries) {
  const props = entries.map(([key, value]) => `'${key}': ${stringify(value)}`);
  return `{ ${props.join(', ')} }`;
}
