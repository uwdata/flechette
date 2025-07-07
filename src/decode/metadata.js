/**
 * @import { Metadata } from '../types.js'
 */
import { readObject, readString, readVector } from '../util/read.js';

/**
 * Decode custom metadata consisting of key-value string pairs.
 * @param {Uint8Array} buf A byte buffer of binary Arrow IPC data
 * @param {number} index The starting index in the byte buffer
 * @returns {Metadata | null} The custom metadata map
 */
export function decodeMetadata(buf, index) {
  const entries = readVector(buf, index, 4, (buf, pos) => {
    const get = readObject(buf, pos);
    return /** @type {[string, string]} */ ([
      get(4, readString), // 4: key (string)
      get(6, readString)  // 6: key (string)
    ]);
  });
  return entries.length ? new Map(entries) : null;
}
