/**
 * @import { BodyCompression, BodyCompressionMethod_, CompressionType_ } from '../types.js'
 */
import { BodyCompressionMethod, CompressionType } from '../constants.js';
import { readInt8, readObject } from '../util/read.js';

/**
 * Decode record batch body compression metadata.
 * @param {Uint8Array} buf A byte buffer of binary Arrow IPC data
 * @param {number} index The starting index in the byte buffer
 * @returns {BodyCompression | undefined} The body compression metadata
 */
export function decodeBodyCompression(buf, index) {
  //  4: codec
  //  6: method
  const get = readObject(buf, index);
  return {
    codec: /** @type {CompressionType_} */(
      get(4, readInt8, CompressionType.LZ4_FRAME)),
    method: /** @type {BodyCompressionMethod_} */(
      get(6, readInt8, BodyCompressionMethod.BUFFER))
  };
}
