export class Sink {
  /**
   * Write bytes to this sink.
   * @param {Uint8Array} bytes The byte buffer to write.
   */
  write(bytes) { // eslint-disable-line no-unused-vars
  }

  /**
   * Write padding bytes (zeroes) to this sink.
   * @param {number} byteCount The number of padding bytes.
   */
  pad(byteCount) {
    this.write(new Uint8Array(byteCount));
  }

  /**
   * @returns {Uint8Array | null}
   */
  finish() {
    return null;
  }
}

export class MemorySink extends Sink {
  /**
   * A sink that collects bytes in memory.
   */
  constructor() {
    super();
    this.buffers = [];
  }

  /**
   * Write bytes
   * @param {Uint8Array} bytes
   */
  write(bytes) {
    this.buffers.push(bytes);
  }

  /**
   * @returns {Uint8Array}
   */
  finish() {
    const bufs = this.buffers;
    const size = bufs.reduce((sum, b) => sum + b.byteLength, 0);
    const buf = new Uint8Array(size);
    for (let i = 0, off = 0; i < bufs.length; ++i) {
      buf.set(bufs[i], off);
      off += bufs[i].byteLength;
    }
    return buf;
  }
}
