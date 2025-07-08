/**
 * @import { RecordBatch, Version_ } from '../types.js'
 */
import { Version } from '../constants.js';
import { readInt64, readObject, readOffset, readVector } from '../util/read.js';

/**
 * Decode a record batch.
 * @param {Uint8Array} buf A byte buffer of binary Arrow IPC data
 * @param {number} index The starting index in the byte buffer
 * @param {Version_} version Arrow version value
 * @returns {RecordBatch} The record batch
 */
export function decodeRecordBatch(buf, index, version) {
  //  4: length
  //  6: nodes
  //  8: buffers
  // 10: compression (not supported)
  // 12: variadicBuffers (buffer counts for view-typed fields)
  const get = readObject(buf, index);
  if (get(10, readOffset, 0)) {
    throw new Error('Record batch compression not implemented');
  }

  // If an Arrow buffer was written before version 4,
  // advance 8 bytes to skip the now-removed page_id field
  const offset = version < Version.V4 ? 8 : 0;

  return {
    length: get(4, readInt64, 0),
    nodes: readVector(buf, get(6, readOffset), 16, (buf, pos) => ({
      length: readInt64(buf, pos),
      nullCount: readInt64(buf, pos + 8)
    })),
    regions: readVector(buf, get(8, readOffset), 16 + offset, (buf, pos) => ({
      offset: readInt64(buf, pos + offset),
      length: readInt64(buf, pos + offset + 8)
    })),
    variadic: readVector(buf, get(12, readOffset), 8, readInt64)
  };
}
