import { readInt32, readInt64AsNum, readVector } from '../util.js';

/**
 * Decode a block that points to messages within an Arrow 'file' format.
 * @param {Uint8Array} buf A byte buffer of binary Arrow IPC data
 * @param {number} index The starting index in the byte buffer
 * @returns The message block.
 */
export function decodeBlock(buf, index) {
  //  0: offset
  //  8: metadataLength
  // 16: bodyLength
  return {
    offset: readInt64AsNum(buf, index),
    metadataLength: readInt32(buf, index + 8),
    bodyLength: readInt64AsNum(buf, index + 16)
  }
}

/**
 * Decode a vector of blocks.
 * @param {Uint8Array} buf
 * @param {number} index
 * @returns An array of message blocks.
 */
export function decodeBlocks(buf, index) {
  return readVector(buf, index, 24, decodeBlock);
}
