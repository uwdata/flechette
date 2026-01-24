/** @import { Codec, CompressionType_ } from './types.js' */
import { readInt64 } from './util/read.js';

/** @type {Map<CompressionType_, Codec>} */
const codecs = new Map;

/**
 * Register a codec to use for compressing or decompressing Arrow buffers.
 * @param {CompressionType_} type The compression type.
 * @param {Codec} codec The codec implementation.
 */
export function setCompressionCodec(type, codec) {
  codecs.set(type, codec);
}

/**
 * Returns a compression codec for the provided type, or null if not found.
 * Compression codecs must first be registered using *setCompressionCodec*.
 * @param {CompressionType_} type The compression type.
 * @returns {Codec | null} The compression codec, or null if not registered.
 */
export function getCompressionCodec(type) {
  return codecs.get(type) ?? null;
}

const LENGTH_NO_COMPRESSED_DATA = -1;
const COMPRESS_LENGTH_PREFIX = 8;

/**
 * Decompress an Arrow buffer, return decompressed bytes and region metadata.
 * @param {Uint8Array} body The message body.
 * @param {{ offset: number, length: number }} region Buffer region metadata.
 * @param {Codec} codec A compression codec.
 * @returns {{ bytes: Uint8Array, offset: number, length: number }}
 */
export function decompressBuffer(body, { offset, length }, codec) {
  if (length === 0) {
    return { bytes: new Uint8Array(0), offset: 0, length: 0 };
  }
  const data = body.subarray(offset, offset + length);
  const len = readInt64(data, 0);
  const buf = data.subarray(COMPRESS_LENGTH_PREFIX);
  const bytes = (len === LENGTH_NO_COMPRESSED_DATA) ? buf : codec.decode(buf);
  return { bytes, offset: 0, length: bytes.length };
}
