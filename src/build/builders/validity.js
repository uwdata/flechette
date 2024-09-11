import { uint8Array } from '../../util/arrays.js';
import { bitmap } from '../buffer.js';
import { BatchBuilder } from './batch.js';

/**
 * Builder for validity bitmaps within batches.
 */
export class ValidityBuilder extends BatchBuilder {
  constructor(type, ctx) {
    super(type, ctx);
  }

  init() {
    this.nullCount = 0;
    this.validity = bitmap();
    return super.init();
  }

  /**
   * @param {*} value
   * @param {number} index
   * @returns {boolean | void}
   */
  set(value, index) {
    this.index = index;
    const isValid = value != null;
    if (isValid) {
      this.validity.set(index);
    } else {
      this.nullCount++;
    }
    return isValid;
  }

  done() {
    const { index, nullCount, type, validity } = this;
    return {
      length: index + 1,
      nullCount,
      type,
      validity: nullCount
        ? validity.array((index >> 3) + 1)
        : new uint8Array(0)
    };
  }
}
