import { readInt32, readInt64, readVector } from '../util/read.js';

/**
 * Decode a block that points to messages within an Arrow 'file' format.
 * @param {Uint8Array} buf A byte buffer of binary Arrow IPC data
 * @param {number} index The starting index in the byte buffer
 * @returns The file block.
 */
export function decodeBlock(buf, index) {
  //  0: offset
  //  8: metadataLength
  // 16: bodyLength
  return {
    offset: readInt64(buf, index),
    metadataLength: readInt32(buf, index + 8),
    bodyLength: readInt64(buf, index + 16)
  }
}

/**
 * Decode a vector of blocks.
 * @param {Uint8Array} buf
 * @param {number} index
 * @returns An array of file blocks.
 */
export function decodeBlocks(buf, index) {
  return readVector(buf, index, 24, decodeBlock);
}
