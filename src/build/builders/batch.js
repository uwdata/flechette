/**
 * @import { Batch } from '../../batch.js'
 */

/**
 * Abstract class for building a column data batch.
 */
export class BatchBuilder {
  constructor(type, ctx) {
    this.type = type;
    this.ctx = ctx;
    this.batchClass = ctx.batchType(type);
  }

  /**
   * Initialize the builder state.
   * @returns {this} This builder.
   */
  init() {
    this.index = -1;
    return this;
  }

  /**
   * Write a value to the builder.
   * @param {*} value
   * @param {number} index
   * @returns {boolean | void}
   */
  set(value, index) {
    this.index = index;
    return false;
  }

  /**
   * Returns a batch constructor options object.
   * Used internally to marshal batch data.
   * @returns {Record<string, any>}
   */
  done() {
    return null;
  }

  /**
   * Returns a completed batch and reinitializes the builder state.
   * @returns {Batch}
   */
  batch() {
    const b = new this.batchClass(this.done());
    this.init();
    return b;
  }
}
