import { toBigInt } from '../../util/numbers.js';
import { buffer } from '../buffer.js';
import { ValidityBuilder } from './validity.js';

/**
 * Builder for data batches that can be accessed directly as typed arrays.
 */
export class DirectBuilder extends ValidityBuilder {
  constructor(type, ctx) {
    super(type, ctx);
    this.values = buffer(type.values);
  }

  init() {
    this.values = buffer(this.type.values);
    return super.init();
  }

  /**
   * @param {*} value
   * @param {number} index
   * @returns {boolean | void}
   */
  set(value, index) {
    if (super.set(value, index)) {
      this.values.set(value, index);
    }
  }

  done() {
    return {
      ...super.done(),
      values: this.values.array(this.index + 1)
    };
  }
}

/**
 * Builder for int64/uint64 data batches written as bigints.
 */
export class Int64Builder extends DirectBuilder {
  set(value, index) {
    super.set(value == null ? value : toBigInt(value), index);
  }
}

/**
 * Builder for data batches whose values must pass through a transform
 * function prior to be written to a backing buffer.
 */
export class TransformBuilder extends DirectBuilder {
  constructor(type, ctx, transform) {
    super(type, ctx);
    this.transform = transform;
  }
  set(value, index) {
    super.set(value == null ? value : this.transform(value), index);
  }
}
