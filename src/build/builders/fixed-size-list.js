import { ValidityBuilder } from './validity.js';

/**
 * Builder for fixed-size-list-typed data batches.
 */
export class FixedSizeListBuilder extends ValidityBuilder {
  constructor(type, ctx) {
    super(type, ctx);
    this.child = ctx.builder(this.type.children[0].type);
    this.stride = type.stride;
  }

  init() {
    this.child.init();
    return super.init();
  }

  set(value, index) {
    const { child, stride } = this;
    const base = index * stride;
    if (super.set(value, index)) {
      for (let i = 0; i < stride; ++i) {
        child.set(value[i], base + i);
      }
    } else {
      child.index = base + stride;
    }
  }

  done() {
    const { child } = this;
    return {
      ...super.done(),
      children: [ child.batch() ]
    };
  }
}
