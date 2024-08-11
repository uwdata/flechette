import { Version } from '../constants.js';
import { readInt64AsNum, readOffset, readVector, table } from '../util.js';

/**
 * Decode a record batch.
 * @param {Uint8Array} buf A byte buffer of binary Arrow IPC data
 * @param {number} index The starting index in the byte buffer
 * @param {import('../types.js').Version} version Arrow version value
 * @returns {import('../types.js').RecordBatch} The record batch
 */
export function decodeRecordBatch(buf, index, version) {
  //  4: length
  //  6: nodes
  //  8: buffers
  // 10: compression (not supported)
  // 12: variadicBuffers (for view types, not supported)
  const get = table(buf, index);
  if (get(10, readOffset, 0)) {
    throw new Error('Record batch compression not implemented');
  }

  // field nodes
  const nodes = [];
  const nodeVector = get(6, readVector);
  if (nodeVector) {
    const { length, base } = nodeVector;
    for (let i = 0; i < length; ++i) {
      const pos = base + i * 16;
      nodes.push({
        length: readInt64AsNum(buf, pos),
        nullCount: readInt64AsNum(buf, pos + 8)
      });
    }
  }

  // buffers
  const buffers = [];
  const bufferVector = get(8, readVector);
  if (bufferVector) {
    const { length, base } = bufferVector;
    const adjust = version < Version.V4;
    for (let i = 0; i < length; ++i) {
      // If this Arrow buffer was written before version 4,
      // advance 8 bytes to skip the now-removed page_id field
      const pos = base + i * 16 + (adjust ? (8 * (i + 1)) : 0);
      buffers.push({
        offset: readInt64AsNum(buf, pos),
        length: readInt64AsNum(buf, pos + 8)
      });
    }
  }

  return {
    length: get(4, readInt64AsNum, 0),
    nodes,
    buffers
  };
}
