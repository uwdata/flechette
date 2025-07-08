/**
 * @import { Column } from './column.js'
 * @import { Schema, StructFactory, TypeMap, ValueArray } from './types.js'
 */
import { bisect } from './util/arrays.js';
import { objectFactory, proxyFactory } from './util/struct.js';

/**
 * A table consists of a collection of named columns (or 'children').
 * To work with table data directly in JavaScript, use `toColumns()`
 * to extract an object that maps column names to extracted value arrays,
 * or `toArray()` to extract an array of row objects. For random access
 * by row index, use `getChild()` to access data for a specific column.
 * @template {TypeMap} [T=TypeMap]
 */
export class Table {
  /**
   * Create a new table with the given schema and columns (children).
   * @param {Schema} schema The table schema.
   * @param {Column[]} children The table columns.
   * @param {boolean} [useProxy=false] Flag indicating if row proxy
   *  objects should be used to represent table rows (default `false`).
   */
  constructor(schema, children, useProxy = false) {
    const names = schema.fields.map(f => f.name);

    /**
     * @type {Schema}
     * @readonly
     */
    this.schema = schema;
    /**
     * @type {(keyof T)[]}
     * @readonly
     */
    this.names = names;
    /**
     * @type {Column[]}
     * @readonly
     */
    this.children = children;
    /**
     * @type {StructFactory}
     * @readonly
     */
    this.factory = useProxy ? proxyFactory : objectFactory;

    // lazily created row object generators
    const gen = [];

    /**
     * Returns a row object generator for the given batch index.
     * @private
     * @readonly
     * @param {number} b The batch index.
     * @returns {(index: number) => { [P in keyof T]: T[P] }}
     */
    this.getFactory = b => gen[b]
      ?? (gen[b] = this.factory(names, children.map(c => c.data[b])));
  }

  /**
   * Provide an informative object string tag.
   */
  get [Symbol.toStringTag]() {
    return 'Table';
  }

  /**
   * The number of columns in this table.
   * @return {number} The number of columns.
   */
  get numCols() {
    return this.names.length;
  }

  /**
   * The number of rows in this table.
   * @return {number} The number of rows.
   */
  get numRows() {
    return this.children[0]?.length ?? 0;
  }

  /**
   * Return the child column at the given index position.
   * @template {T[keyof T]} R
   * @param {number} index The column index.
   * @returns {Column<R>}
   */
  getChildAt(index) {
    return this.children[index];
  }

  /**
   * Return the first child column with the given name.
   * @template {keyof T} P
   * @param {P} name The column name.
   * @returns {Column<T[P]>}
   */
  getChild(name) {
    const i = this.names.findIndex(x => x === name);
    return i > -1 ? this.children[i] : undefined;
  }

  /**
   * Construct a new table containing only columns at the specified indices.
   * The order of columns in the new table matches the order of input indices.
   * @template {T[keyof T]} V
   * @param {number[]} indices The indices of columns to keep.
   * @param {string[]} [as] Optional new names for selected columns.
   * @returns {Table<{ [key: string]: V }>} A new table with selected columns.
   */
  selectAt(indices, as = []) {
    const { children, factory, schema } = this;
    const { fields } = schema;
    return new Table(
      {
        ...schema,
        fields: indices.map((i, j) => renameField(fields[i], as[j]))
      },
      indices.map(i => children[i]),
      factory === proxyFactory
    );
  }

  /**
   * Construct a new table containing only columns with the specified names.
   * If columns have duplicate names, the first (with lowest index) is used.
   * The order of columns in the new table matches the order of input names.
   * @template {keyof T} K
   * @param {K[]} names Names of columns to keep.
   * @param {string[]} [as] Optional new names for selected columns.
   * @returns A new table with columns matching the specified names.
   */
  select(names, as) {
    const all = /** @type {K[]} */(this.names);
    const indices = names.map(name => all.indexOf(name));
    return this.selectAt(indices, as);
  }

  /**
   * Return an object mapping column names to extracted value arrays.
   * @returns {{ [P in keyof T]: ValueArray<T[P]> }}
   */
  toColumns() {
    const { children, names } = this;
    /** @type {{ [P in keyof T]: ValueArray<T[P]> }} */
    // @ts-expect-error assign to empty object
    const cols = {};
    names.forEach((name, i) => cols[name] = children[i]?.toArray() ?? [] );
    return cols;
  }

  /**
   * Return an array of objects representing the rows of this table.
   * @returns {{ [P in keyof T]: T[P] }[]}
   */
  toArray() {
    const { children, getFactory, numRows } = this;
    const data = children[0]?.data ?? [];
    const output = Array(numRows);
    for (let b = 0, row = -1; b < data.length; ++b) {
      const f = getFactory(b);
      for (let i = 0; i < data[b].length; ++i) {
        output[++row] = f(i);
      }
    }
    return output;
  }

  /**
   * Return an iterator over objects representing the rows of this table.
   * @returns {Generator<{ [P in keyof T]: T[P] }, any, any>}
   */
  *[Symbol.iterator]() {
    const { children, getFactory } = this;
    const data = children[0]?.data ?? [];
    for (let b = 0; b < data.length; ++b) {
      const f = getFactory(b);
      for (let i = 0; i < data[b].length; ++i) {
        yield f(i);
      }
    }
  }

  /**
   * Return a row object for the given index.
   * @param {number} index The row index.
   * @returns {{ [P in keyof T]: T[P] }} The row object.
   */
  at(index) {
    const { children, getFactory, numRows } = this;
    if (index < 0 || index >= numRows) return null;
    const [{ offsets }] = children;
    const b = bisect(offsets, index) - 1;
    return getFactory(b)(index - offsets[b]);
  }

  /**
   * Return a row object for the given index. This method is the same as
   * `at()` and is provided for better compatibility with Apache Arrow JS.
   * @param {number} index The row index.
   * @returns {{ [P in keyof T]: T[P] }} The row object.
   */
  get(index) {
    return this.at(index);
  }
}

function renameField(field, name) {
  return (name != null && name !== field.name)
    ? { ...field, name }
    : field;
}
