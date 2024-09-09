import { toNumber } from './numbers.js';
import { decodeUtf8 } from './strings.js';

/** The size in bytes of a 32-bit integer. */
export const SIZEOF_INT = 4;

/** The size in bytes of a 16-bit integer. */
export const SIZEOF_SHORT = 2;

/**
 * Return a boolean for a single bit in a bitmap.
 * @param {Uint8Array} bitmap The bitmap.
 * @param {number} index The bit index to read.
 * @returns {boolean} The boolean bitmap value.
 */
export function decodeBit(bitmap, index) {
  return (bitmap[index >> 3] & 1 << (index % 8)) !== 0;
}

/**
 * Lookup helper for flatbuffer object (table) entries.
 * @param {Uint8Array} buf The byte buffer.
 * @param {number} index The base index of the object.
 */
export function readObject(buf, index) {
  const pos = index + readInt32(buf, index);
  const vtable = pos - readInt32(buf, pos);
  const size = readInt16(buf, vtable);
  /**
   * Retrieve a value from a flatbuffer table layout.
   * @template T
   * @param {number} index The table entry index.
   * @param {(buf: Uint8Array, offset: number) => T} read Read function to invoke.
   * @param {T} [fallback=null] The default fallback value.
   * @returns {T}
   */
  return (index, read, fallback = null) => {
    if (index < size) {
      const off = readInt16(buf, vtable + index);
      if (off) return read(buf, pos + off);
    }
    return fallback;
  };
}

/**
 * Return a buffer offset value.
 * @param {Uint8Array} buf
 * @param {number} offset
 * @returns {number}
 */
export function readOffset(buf, offset) {
  return offset;
}

/**
 * Return a boolean value.
 * @param {Uint8Array} buf
 * @param {number} offset
 * @returns {boolean}
 */
export function readBoolean(buf, offset) {
  return !!readInt8(buf, offset);
}

/**
 * Return a signed 8-bit integer value.
 * @param {Uint8Array} buf
 * @param {number} offset
 * @returns {number}
 */
export function readInt8(buf, offset) {
  return readUint8(buf, offset) << 24 >> 24;
}

/**
 * Return an unsigned 8-bit integer value.
 * @param {Uint8Array} buf
 * @param {number} offset
 * @returns {number}
 */
export function readUint8(buf, offset) {
  return buf[offset];
}

/**
 * Return a signed 16-bit integer value.
 * @param {Uint8Array} buf
 * @param {number} offset
 * @returns {number}
 */
export function readInt16(buf, offset) {
  return readUint16(buf, offset) << 16 >> 16;
}

/**
 * Return an unsigned 16-bit integer value.
 * @param {Uint8Array} buf
 * @param {number} offset
 * @returns {number}
 */
export function readUint16(buf, offset) {
  return buf[offset] | buf[offset + 1] << 8;
}

/**
 * Return a signed 32-bit integer value.
 * @param {Uint8Array} buf
 * @param {number} offset
 * @returns {number}
 */
export function readInt32(buf, offset) {
  return buf[offset]
    | buf[offset + 1] << 8
    | buf[offset + 2] << 16
    | buf[offset + 3] << 24;
}

/**
 * Return an unsigned 32-bit integer value.
 * @param {Uint8Array} buf
 * @param {number} offset
 * @returns {number}
 */
export function readUint32(buf, offset) {
  return readInt32(buf, offset) >>> 0;
}

/**
 * Return a signed 64-bit integer value coerced to a JS number.
 * Throws an error if the value exceeds what a JS number can represent.
 * @param {Uint8Array} buf
 * @param {number} offset
 * @returns {number}
 */
export function readInt64(buf, offset) {
  return toNumber(BigInt.asIntN(
    64,
    BigInt(readUint32(buf, offset)) +
      (BigInt(readUint32(buf, offset + SIZEOF_INT)) << 32n)
  ));
}

/**
 * Create a JavaScript string from UTF-8 data stored inside the FlatBuffer.
 * This allocates a new string and converts to wide chars upon each access.
 * @param {Uint8Array} buf The byte buffer.
 * @param {number} index The index of the string entry.
 * @returns {string} The decoded string.
 */
export function readString(buf, index) {
  let offset = index + readInt32(buf, index); // get the string offset
  const length = readInt32(buf, offset);  // get the string length
  offset += SIZEOF_INT; // skip length value
  return decodeUtf8(buf.subarray(offset, offset + length));
}

/**
 * Extract a flatbuffer vector to an array.
 * @template T
 * @param {Uint8Array} buf The byte buffer.
 * @param {number} offset The offset location of the vector.
 * @param {number} stride The stride between vector entries.
 * @param {(buf: Uint8Array, pos: number) => T} extract Vector entry extraction function.
 * @returns {T[]} The extracted vector entries.
 */
export function readVector(buf, offset, stride, extract) {
  if (!offset) return [];

  // get base position by adding offset delta
  const base = offset + readInt32(buf, offset);

  // read vector size, extract entries
  return Array.from(
    { length: readInt32(buf, base) },
    (_, i) => extract(buf, base + SIZEOF_INT + i * stride)
  );
}
