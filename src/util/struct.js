/**
 * @import { Batch } from '../batch.js';
 */

/**
 * Symbol for the row index value of a struct object proxy.
 */
export const RowIndex = Symbol('rowIndex');

/**
 * Returns a row proxy object factory. The resulting method takes a
 * batch-level row index as input and returns an object that proxies
 * access to underlying batches.
 * @param {string[]} names The column (property) names
 * @param {Batch[]} batches The value batches.
 * @returns {(index: number) => Record<string, any>}
 */
export function proxyFactory(names, batches) {
  class RowObject {
    /**
     * Create a new proxy row object representing a struct or table row.
     * @param {number} index The record batch row index.
     */
    constructor(index) {
      this[RowIndex] = index;
    }

    /**
     * Return a JSON-compatible object representation.
     */
    toJSON() {
      return structObject(names, batches, this[RowIndex]);
    }
  };

  // prototype for row proxy objects
  const proto = RowObject.prototype;

  for (let i = 0; i < names.length; ++i) {
    // skip duplicated column names
    if (Object.hasOwn(proto, names[i])) continue;

    // add a getter method for the current batch
    const batch = batches[i];
    Object.defineProperty(proto, names[i], {
      get() { return batch.at(this[RowIndex]); },
      enumerable: true
    });
  }

  return index => new RowObject(index);
}

/**
 * Returns a row object factory. The resulting method takes a
 * batch-level row index as input and returns an object whose property
 * values have been extracted from the batches.
 * @param {string[]} names The column (property) names
 * @param {Batch[]} batches The value batches.
 * @returns {(index: number) => Record<string, any>}
 */
export function objectFactory(names, batches) {
  return index => structObject(names, batches, index);
}

/**
 * Return a vanilla object representing a struct (row object) type.
 * @param {string[]} names The column (property) names
 * @param {Batch[]} batches The value batches.
 * @param {number} index The record batch row index.
 * @returns {Record<string, any>}
 */
export function structObject(names, batches, index) {
  const obj = {};
  for (let i = 0; i < names.length; ++i) {
    obj[names[i]] = batches[i].at(index);
  }
  return obj;
}
