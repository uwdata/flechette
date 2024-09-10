import { toOffset } from '../../util/numbers.js';
import { buffer } from '../buffer.js';
import { ValidityBuilder } from './validity.js';

/**
 * Builder for batches of binary-typed data.
 */
export class BinaryBuilder extends ValidityBuilder {
  constructor(type, ctx) {
    super(type, ctx);
    this.toOffset = toOffset(type.offsets);
  }

  init() {
    this.offsets = buffer(this.type.offsets);
    this.values = buffer();
    this.pos = 0;
    return super.init();
  }

  set(value, index) {
    const { offsets, values, toOffset } = this;
    if (super.set(value, index)) {
      values.write(value, this.pos);
      this.pos += value.length;
    }
    offsets.set(toOffset(this.pos), index + 1);
  }

  done() {
    return {
      ...super.done(),
      offsets: this.offsets.array(this.index + 2),
      values: this.values.array(this.pos + 1)
    };
  }
}
