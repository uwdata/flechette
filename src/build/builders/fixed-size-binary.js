import { buffer } from '../buffer.js';
import { ValidityBuilder } from './validity.js';

/**
 * Builder for fixed-size-binary-typed data batches.
 */
export class FixedSizeBinaryBuilder extends ValidityBuilder {
  constructor(type, ctx) {
    super(type, ctx);
    this.stride = type.stride;
  }

  init() {
    this.values = buffer();
    return super.init();
  }

  set(value, index) {
    if (super.set(value, index)) {
      this.values.write(value, index * this.stride);
    }
  }

  done() {
    const { stride, values } = this;
    return {
      ...super.done(),
      values: values.array(stride * (this.index + 1))
    };
  }
}
