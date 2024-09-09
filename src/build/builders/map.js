import { AbstractListBuilder } from './list.js';
import { AbstractStructBuilder } from './struct.js';

export class MapBuilder extends AbstractListBuilder {
  constructor(type, ctx) {
    super(type, ctx, new MapStructBuilder(type.children[0].type, ctx));
  }

  set(value, index) {
    const { child, offsets, toOffset } = this;
    if (super.set(value, index)) {
      for (const keyValuePair of value) {
        child.set(keyValuePair, this.pos++);
      }
    }
    offsets.set(toOffset(this.pos), index + 1);
  }
}

class MapStructBuilder extends AbstractStructBuilder {
  set(value, index) {
    super.set(value, index);
    const [key, val] = this.children;
    key.set(value[0], index);
    val.set(value[1], index);
  }
}
