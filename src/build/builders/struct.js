import { ValidityBuilder } from './validity.js';

/**
 * Abstract class for building list-typed data batches.
 */
export class AbstractStructBuilder extends ValidityBuilder {
  constructor(type, ctx) {
    super(type, ctx);
    this.children = type.children.map(c => ctx.builder(c.type));
  }

  init() {
    this.children.forEach(c => c.init());
    return super.init();
  }

  done() {
    const { children } = this;
    children.forEach(c => c.index = this.index);
    return {
      ...super.done(),
      children: children.map(c => c.batch())
    };
  }
}

/**
 * Builder for struct-typed data batches.
 */
export class StructBuilder extends AbstractStructBuilder {
  constructor(type, ctx) {
    super(type, ctx);
    this.setters = this.children.map((child, i) => {
      const name = type.children[i].name;
      return (value, index) => child.set(value?.[name], index);
    });
  }

  set(value, index) {
    super.set(value, index);
    const setters = this.setters;
    for (let i = 0; i < setters.length; ++i) {
      setters[i](value, index);
    }
  }
}
