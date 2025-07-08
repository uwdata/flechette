/**
 * @import { Sink } from './sink.js';
 */
import { grow } from '../util/arrays.js';
import { SIZEOF_INT, SIZEOF_SHORT, readInt16 } from '../util/read.js';
import { encodeUtf8 } from '../util/strings.js';

export function writeInt32(buf, index, value) {
  buf[index] = value;
  buf[index + 1] = value >> 8;
  buf[index + 2] = value >> 16;
  buf[index + 3] = value >> 24;
}

const INIT_SIZE = 1024;

/** Flatbuffer binary builder. */
export class Builder {
  /**
   * Create a new builder instance.
   * @param {Sink} sink The byte consumer.
   */
  constructor(sink) {
    /**
     * Sink that consumes built byte buffers;
     * @type {Sink}
     */
    this.sink = sink;
    /**
     * Minimum alignment encountered so far.
     * @type {number}
     */
    this.minalign = 1;
    /**
     * Current byte buffer.
     * @type {Uint8Array}
     */
    this.buf = new Uint8Array(INIT_SIZE);
    /**
     * Remaining space in the current buffer.
     * @type {number}
     */
    this.space = INIT_SIZE;
    /**
     * List of offsets of all vtables. Used to find and
     * reuse tables upon duplicated table field schemas.
     * @type {number[]}
     */
    this.vtables = [];
    /**
     * Total bytes written to sink thus far.
     */
    this.outputBytes = 0;
  }

  /**
   * Returns the flatbuffer offset, relative to the end of the current buffer.
   * @returns {number} Offset relative to the end of the buffer.
   */
  offset() {
    return this.buf.length - this.space;
  }

  /**
   * Write a flatbuffer int8 value at the current buffer position
   * and advance the internal cursor.
   * @param {number} value
   */
  writeInt8(value) {
    this.buf[this.space -= 1] = value;
  }

  /**
   * Write a flatbuffer int16 value at the current buffer position
   * and advance the internal cursor.
   * @param {number} value
   */
  writeInt16(value) {
    this.buf[this.space -= 2] = value;
    this.buf[this.space + 1] = value >> 8;
  }

  /**
   * Write a flatbuffer int32 value at the current buffer position
   * and advance the internal cursor.
   * @param {number} value
   */
  writeInt32(value) {
    writeInt32(this.buf, this.space -= 4, value);
  }

  /**
   * Write a flatbuffer int64 value at the current buffer position
   * and advance the internal cursor.
   * @param {number} value
   */
  writeInt64(value) {
    const v = BigInt(value);
    this.writeInt32(Number(BigInt.asIntN(32, v >> BigInt(32))));
    this.writeInt32(Number(BigInt.asIntN(32, v)));
  }

  /**
   * Add a flatbuffer int8 value, properly aligned,
   * @param value The int8 value to add the buffer.
   */
  addInt8(value) {
    prep(this, 1, 0);
    this.writeInt8(value);
  }

  /**
   * Add a flatbuffer int16 value, properly aligned,
   * @param value The int16 value to add the buffer.
   */
  addInt16(value) {
    prep(this, 2, 0);
    this.writeInt16(value);
  }

  /**
   * Add a flatbuffer int32 value, properly aligned,
   * @param value The int32 value to add the buffer.
   */
  addInt32(value) {
    prep(this, 4, 0);
    this.writeInt32(value);
  }

  /**
   * Add a flatbuffer int64 values, properly aligned.
   * @param value The int64 value to add the buffer.
   */
  addInt64(value) {
    prep(this, 8, 0);
    this.writeInt64(value);
  }

  /**
   * Add a flatbuffer offset, relative to where it will be written.
   * @param {number} offset The offset to add.
   */
  addOffset(offset) {
    prep(this, SIZEOF_INT, 0); // Ensure alignment is already done.
    this.writeInt32(this.offset() - offset + SIZEOF_INT);
  }

  /**
   * Add a flatbuffer object (vtable).
   * @param {number} numFields The maximum number of fields
   *  this object may include.
   * @param {(tableBuilder: ReturnType<objectBuilder>) => void} [addFields]
   *  A callback function that writes all fields using an object builder.
   * @returns {number} The object offset.
   */
  addObject(numFields, addFields) {
    const b = objectBuilder(this, numFields);
    addFields?.(b);
    return b.finish();
  }

  /**
   * Add a flatbuffer vector (list).
   * @template T
   * @param {T[]} items An array of items to write.
   * @param {number} itemSize The size in bytes of a serialized item.
   * @param {number} alignment The desired byte alignment value.
   * @param {(builder: this, item: T) => void} writeItem A callback
   *  function that writes a vector item to this builder.
   * @returns {number} The vector offset.
   */
  addVector(items, itemSize, alignment, writeItem) {
    const n = items?.length;
    if (!n) return 0;
    prep(this, SIZEOF_INT, itemSize * n);
    prep(this, alignment, itemSize * n); // Just in case alignment > int.
    for (let i = n; --i >= 0;) {
      writeItem(this, items[i]);
    }
    this.writeInt32(n);
    return this.offset();
  }

  /**
   * Convenience method for writing a vector of byte buffer offsets.
   * @param {number[]} offsets
   * @returns {number} The vector offset.
   */
  addOffsetVector(offsets) {
    return this.addVector(offsets, 4, 4, (b, off) => b.addOffset(off));
  }

  /**
   * Add a flatbuffer UTF-8 string.
   * @param {string} s The string to encode.
   * @return {number} The string offset.
   */
  addString(s) {
    if (s == null) return 0;
    const utf8 = encodeUtf8(s);
    const n = utf8.length;
    this.addInt8(0); // string null terminator
    prep(this, SIZEOF_INT, n);
    this.buf.set(utf8, this.space -= n);
    this.writeInt32(n);
    return this.offset();
  }

  /**
   * Finish the current flatbuffer by adding a root offset.
   * @param {number} rootOffset The root offset.
   */
  finish(rootOffset) {
    prep(this, this.minalign, SIZEOF_INT);
    this.addOffset(rootOffset);
  }

  /**
   * Flush the current flatbuffer byte buffer content to the sink,
   * and reset the flatbuffer builder state.
   */
  flush() {
    const { buf, sink } = this;
    const bytes = buf.subarray(this.space, buf.length);
    sink.write(bytes);
    this.outputBytes += bytes.byteLength;
    this.minalign = 1;
    this.vtables = [];
    this.buf = new Uint8Array(INIT_SIZE);
    this.space = INIT_SIZE;
  }

  /**
   * Add a byte buffer directly to the builder sink. This method bypasses
   * any unflushed flatbuffer state and leaves it unchanged, writing the
   * buffer to the sink *before* the flatbuffer.
   * The buffer will be padded for 64-bit (8-byte) alignment as needed.
   * @param {Uint8Array} buffer The buffer to add.
   * @returns {number} The total byte count of the buffer and padding.
   */
  addBuffer(buffer) {
    const size = buffer.byteLength;
    if (!size) return 0;
    this.sink.write(buffer);
    this.outputBytes += size;
    const pad = ((size + 7) & ~7) - size;
    this.addPadding(pad);
    return size + pad;
  }

  /**
   * Write padding bytes directly to the builder sink. This method bypasses
   * any unflushed flatbuffer state and leaves it unchanged, writing the
   * padding bytes to the sink *before* the flatbuffer.
   * @param {number} byteCount The number of padding bytes.
   */
  addPadding(byteCount) {
    if (byteCount > 0) {
      this.sink.write(new Uint8Array(byteCount));
      this.outputBytes += byteCount;
    }
  }
}

/**
 * Prepare to write an element of `size` after `additionalBytes` have been
 * written, e.g. if we write a string, we need to align such the int length
 * field is aligned to 4 bytes, and the string data follows it directly. If all
 * we need to do is alignment, `additionalBytes` will be 0.
 * @param {Builder} builder The builder to prep.
 * @param {number} size The size of the new element to write.
 * @param {number} additionalBytes Additional padding size.
 */
export function prep(builder, size, additionalBytes) {
  let { buf, space, minalign } = builder;

  // track the biggest thing we've ever aligned to
  if (size > minalign) {
    builder.minalign = size;
  }

  // find alignment needed so that `size` aligns after `additionalBytes`
  const bufSize = buf.length;
  const used = bufSize - space + additionalBytes;
  const alignSize = (~used + 1) & (size - 1);

  // reallocate the buffer if needed
  buf = grow(buf, used + alignSize + size - 1, true);
  space += buf.length - bufSize;

  // add padding
  for (let i = 0; i < alignSize; ++i) {
    buf[--space] = 0;
  }

  // update builder state
  builder.buf = buf;
  builder.space = space;
}

/**
 * Returns a builder object for flatbuffer objects (vtables).
 * @param {Builder} builder The underlying flatbuffer builder.
 * @param {number} numFields The expected number of fields, not
 *  including the standard size fields.
 */
function objectBuilder(builder, numFields) {
  /** @type {number[]} */
  const vtable = Array(numFields).fill(0);
  const startOffset = builder.offset();

  function slot(index) {
    vtable[index] = builder.offset();
  }

  return {
    /**
     * Add an int8-valued table field.
     * @param {number} index
     * @param {number} value
     * @param {number} defaultValue
     */
    addInt8(index, value, defaultValue) {
      if (value != defaultValue) {
        builder.addInt8(value);
        slot(index);
      }
    },

    /**
     * Add an int16-valued table field.
     * @param {number} index
     * @param {number} value
     * @param {number} defaultValue
     */
    addInt16(index, value, defaultValue) {
      if (value != defaultValue) {
        builder.addInt16(value);
        slot(index);
      }
    },

    /**
     * Add an int32-valued table field.
     * @param {number} index
     * @param {number} value
     * @param {number} defaultValue
     */
    addInt32(index, value, defaultValue) {
      if (value != defaultValue) {
        builder.addInt32(value);
        slot(index);
      }
    },

    /**
     * Add an int64-valued table field.
     * @param {number} index
     * @param {number} value
     * @param {number} defaultValue
     */
    addInt64(index, value, defaultValue) {
      if (value != defaultValue) {
        builder.addInt64(value);
        slot(index);
      }
    },

    /**
     * Add a buffer offset-valued table field.
     * @param {number} index
     * @param {number} value
     * @param {number} defaultValue
     */
    addOffset(index, value, defaultValue) {
      if (value != defaultValue) {
        builder.addOffset(value);
        slot(index);
      }
    },

    /**
     * Write the vtable to the buffer and return the table offset.
     * @returns {number} The buffer offset to the vtable.
     */
    finish() {
      // add offset entry, will overwrite later with actual offset
      builder.addInt32(0);
      const vtableOffset = builder.offset();

      // trim zero-valued fields (indicating default value)
      let i = numFields;
      while (--i >= 0 && vtable[i] === 0) {} // eslint-disable-line no-empty
      const size = i + 1;

      // Write out the current vtable.
      for (; i >= 0; --i) {
        // Offset relative to the start of the table.
        builder.addInt16(vtable[i] ? (vtableOffset - vtable[i]) : 0);
      }

      const standardFields = 2; // size fields
      builder.addInt16(vtableOffset - startOffset);
      const len = (size + standardFields) * SIZEOF_SHORT;
      builder.addInt16(len);

      // Search for an existing vtable that matches the current one.
      let existingTable = 0;
      const { buf, vtables, space: vt1 } = builder;
    outer_loop:
      for (i = 0; i < vtables.length; ++i) {
        const vt2 = buf.length - vtables[i];
        if (len == readInt16(buf, vt2)) {
          for (let j = SIZEOF_SHORT; j < len; j += SIZEOF_SHORT) {
            if (readInt16(buf, vt1 + j) != readInt16(buf, vt2 + j)) {
              continue outer_loop;
            }
          }
          existingTable = vtables[i];
          break;
        }
      }

      if (existingTable) {
        // Found a match: remove the current vtable.
        // Point table to existing vtable.
        builder.space = buf.length - vtableOffset;
        writeInt32(buf, builder.space, existingTable - vtableOffset);
      } else {
        // No match: add the location of the current vtable to the vtables list.
        // Point table to current vtable.
        const off = builder.offset();
        vtables.push(off);
        writeInt32(buf, buf.length - vtableOffset, off - vtableOffset);
      }

      return vtableOffset;
    }
  }
}
