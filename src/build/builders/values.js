import { toBigInt } from '../../util/numbers.js';
import { buffer } from '../buffer.js';
import { ValidityBuilder } from './validity.js';

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

export class Int64Builder extends DirectBuilder {
  set(value, index) {
    super.set(value == null ? value : toBigInt(value), index);
  }
}

export class TransformBuilder extends DirectBuilder {
  constructor(type, ctx, transform) {
    super(type, ctx);
    this.transform = transform;
  }
  set(value, index) {
    super.set(value == null ? value : this.transform(value), index);
  }
}
