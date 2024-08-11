import { readString, readVector, table } from '../util.js';

/**
 * Decode custom metadata consisting of key-value string pairs.
 * @param {Uint8Array} buf A byte buffer of binary Arrow IPC data
 * @param {number} index The starting index in the byte buffer
 * @returns {import('../types.js').Metadata | null} The custom metadata map
 */
export function decodeMetadata(buf, index) {
  const { length, base } = readVector(buf, index);
  const metadata = length > 0 ? new Map : null;
  for (let i = 0; i < length; ++i) {
    //  4: key (string)
    //  6: key (string)
    const get = table(buf, base + i * 4);
    const key = get(4, readString);
    const val = get(6, readString);
    if (key || val) metadata.set(key, val);
  }
  return metadata?.size ? metadata : null;
}
