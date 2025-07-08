/**
 * @import { TypedArray, TypedArrayConstructor } from '../types.js'
 */
import { align, grow, uint8Array } from '../util/arrays.js';

/**
 * Create a new resizable buffer instance.
 * @param {TypedArrayConstructor} [arrayType]
 *  The array type.
 * @returns {Buffer} The buffer.
 */
export function buffer(arrayType) {
  return new Buffer(arrayType);
}

/**
 * Resizable byte buffer.
 */
export class Buffer {
  /**
   * Create a new resizable buffer instance.
   * @param {TypedArrayConstructor} arrayType
   */
  constructor(arrayType = uint8Array) {
    this.buf = new arrayType(512);
  }
  /**
   * Return the underlying data as a 64-bit aligned array of minimum size.
   * @param {number} size The desired minimum array size.
   * @returns {TypedArray} The 64-bit aligned array.
   */
  array(size) {
    return align(this.buf, size);
  }
  /**
   * Prepare for writes to the given index, resizing as necessary.
   * @param {number} index The array index to prepare to write to.
   */
  prep(index) {
    if (index >= this.buf.length) {
      this.buf = grow(this.buf, index);
    }
  }
  /**
   * Return the value at the given index.
   * @param {number} index The array index.
   */
  get(index) {
    return this.buf[index];
  }
  /**
   * Set a value at the given index.
   * @param {number | bigint} value The value to set.
   * @param {number} index The index to write to.
   */
  set(value, index) {
    this.prep(index);
    this.buf[index] = value;
  }
  /**
   * Write a byte array at the given index. The method should be called
   * only when the underlying buffer is of type Uint8Array.
   * @param {Uint8Array} bytes The byte array.
   * @param {number} index The starting index to write to.
   */
  write(bytes, index) {
    this.prep(index + bytes.length);
    /** @type {Uint8Array} */ (this.buf).set(bytes, index);
  }
}

/**
 * Create a new resizable bitmap instance.
 * @returns {Bitmap} The bitmap buffer.
 */
export function bitmap() {
  return new Bitmap();
}

/**
 * Resizable bitmap buffer.
 */
export class Bitmap extends Buffer {
  /**
   * Set a bit to true at the given bitmap index.
   * @param {number} index The index to write to.
   */
  set(index) {
    const i = index >> 3;
    this.prep(i);
    /** @type {Uint8Array} */ (this.buf)[i] |= (1 << (index % 8));
  }
}
