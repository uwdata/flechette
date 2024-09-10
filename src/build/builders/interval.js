import { toMonthDayNanoBytes } from '../../util/numbers.js';
import { buffer } from '../buffer.js';
import { ValidityBuilder } from './validity.js';

/**
 * Builder for day/time interval-typed data batches.
 */
export class IntervalDayTimeBuilder extends ValidityBuilder {
  init() {
    this.values = buffer(this.type.values);
    return super.init();
  }

  set(value, index) {
    if (super.set(value, index)) {
      const i = index << 1;
      this.values.set(value[0], i);
      this.values.set(value[1], i + 1);
    }
  }

  done() {
    return {
      ...super.done(),
      values: this.values.array((this.index + 1) << 1)
    }
  }
}

/**
 * Builder for month/day/nano interval-typed data batches.
 */
export class IntervalMonthDayNanoBuilder extends ValidityBuilder {
  init() {
    this.values = buffer();
    return super.init();
  }

  set(value, index) {
    if (super.set(value, index)) {
      this.values.write(toMonthDayNanoBytes(value), index << 4);
    }
  }

  done() {
    return {
      ...super.done(),
      values: this.values.array((this.index + 1) << 4)
    }
  }
}
