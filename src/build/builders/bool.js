import { bitmap } from '../buffer.js';
import { ValidityBuilder } from './validity.js';

export class BoolBuilder extends ValidityBuilder {
  constructor(type, ctx) {
    super(type, ctx);
  }

  init() {
    this.values = bitmap();
    return super.init();
  }

  set(value, index) {
    super.set(value, index);
    if (value) this.values.set(index);
  }

  done() {
    return {
      ...super.done(),
      values: this.values.array((this.index >> 3) + 1)
    }
  }
}
