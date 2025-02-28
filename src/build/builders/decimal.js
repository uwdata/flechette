import { toDecimal } from '../../util/numbers.js';
import { buffer } from '../buffer.js';
import { ValidityBuilder } from './validity.js';

/**
 * Builder for batches of decimal-typed data (64-bits or more).
 */
export class DecimalBuilder extends ValidityBuilder {
  constructor(type, ctx) {
    super(type, ctx);
    this.scale = 10 ** type.scale;
    this.stride = type.bitWidth >> 6;
  }

  init() {
    this.values = buffer(this.type.values);
    return super.init();
  }

  set(value, index) {
    const { scale, stride, values } = this;
    if (super.set(value, index)) {
      values.prep((index + 1) * stride);
      // @ts-ignore
      toDecimal(value, values.buf, index * stride, stride, scale);
    }
  }

  done() {
    const { index, stride, values } = this;
    return {
      ...super.done(),
      values: values.array((index + 1) * stride)
    };
  }
}
