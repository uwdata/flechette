/** @import { Codec, CompressionType_ } from './types.js' */
import { CompressionType } from './constants.js';
import { writeInt64 } from './encode/builder.js';
import { keyFor } from './util/objects.js';
import { readInt64 } from './util/read.js';

const LENGTH_NO_COMPRESSED_DATA = -1;
const COMPRESS_LENGTH_PREFIX = 8;

/**
 * Return an error message for a missing codec.
 * @param {CompressionType_} type The codec type.
 */
export function missingCodec(type) {
  return `Missing compression codec "${keyFor(CompressionType, type)}" (id ${type})`;
}

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
 * @param {CompressionType_ | null} [type] The compression type.
 * @returns {Codec | null} The compression codec, or null if not registered.
 */
export function getCompressionCodec(type) {
  return (type != null && codecs.get(type)) || null;
}

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
  const ulen = readInt64(body, offset); // uncompressed length
  const buf = body.subarray(offset + COMPRESS_LENGTH_PREFIX, offset + length);
  const bytes = (ulen === LENGTH_NO_COMPRESSED_DATA) ? buf : codec.decode(buf);
  return { bytes, offset: 0, length: bytes.length };
}

/**
 * Compress an Arrow buffer, return encoded bytes. If the compression does
 * not decrease the overall length, retains uncompressed bytes.
 * @param {Uint8Array} bytes The byte buffer to compress.
 * @param {Codec} codec A compression codec.
 */
export function compressBuffer(bytes, codec) {
  const compressed = codec.encode(bytes);
  const keep = compressed.length < bytes.length;
  const data = keep ? compressed : bytes;
  const buf = new Uint8Array(COMPRESS_LENGTH_PREFIX + data.length);
  writeInt64(buf, 0, keep ? bytes.length : LENGTH_NO_COMPRESSED_DATA);
  buf.set(data, COMPRESS_LENGTH_PREFIX);
  return buf;
}
