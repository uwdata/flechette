import { keyString } from '../../util/strings.js';
import { BatchBuilder } from './batch.js';

const NO_VALUE = {}; // empty object that fails strict equality

/**
 * Builder for run-end-encoded-typed data batches.
 */
export class RunEndEncodedBuilder extends BatchBuilder {
  constructor(type, ctx) {
    super(type, ctx);
    this.children = type.children.map(c => ctx.builder(c.type));
  }

  init() {
    this.pos = 0;
    this.key = null;
    this.value = NO_VALUE;
    this.children.forEach(c => c.init());
    return super.init();
  }

  next() {
    const [runs, vals] = this.children;
    runs.set(this.index + 1, this.pos);
    vals.set(this.value, this.pos++);
  }

  set(value, index) {
    // perform fast strict equality test
    if (value !== this.value) {
      // if no match, fallback to key string test
      const key = keyString(value);
      if (key !== this.key) {
        // if key doesn't match, write prior run and update
        if (this.key) this.next();
        this.key = key;
        this.value = value;
      }
    }
    this.index = index;
  }

  done() {
    this.next();
    const { children, index, type } = this;
    return {
      length: index + 1,
      nullCount: 0,
      type,
      children: children.map(c => c.batch())
    };
  }
}
