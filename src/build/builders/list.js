import { toOffset } from '../../util/numbers.js';
import { buffer } from '../buffer.js';
import { ValidityBuilder } from './validity.js';

/**
 * Abstract class for building list data batches.
 */
export class AbstractListBuilder extends ValidityBuilder {
  constructor(type, ctx, child) {
    super(type, ctx);
    this.child = child;
  }

  init() {
    this.child.init();
    const offsetType = this.type.offsets;
    this.offsets = buffer(offsetType);
    this.toOffset = toOffset(offsetType);
    this.pos = 0;
    return super.init();
  }

  done() {
    return {
      ...super.done(),
      offsets: this.offsets.array(this.index + 2),
      children: [ this.child.batch() ]
    };
  }
}

/**
 * Builder for list-typed data batches.
 */
export class ListBuilder extends AbstractListBuilder {
  constructor(type, ctx) {
    super(type, ctx, ctx.builder(type.children[0].type));
  }

  set(value, index) {
    const { child, offsets, toOffset } = this;
    if (super.set(value, index)) {
      value.forEach(v => child.set(v, this.pos++));
    }
    offsets.set(toOffset(this.pos), index + 1);
  }
}
