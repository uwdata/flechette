import { int8Array } from '../../util/arrays.js';
import { BatchBuilder } from './batch.js';
import { buffer } from '../buffer.js';

/**
 * Abstract class for building union-typed data batches.
 */
export class AbstractUnionBuilder extends BatchBuilder {
  constructor(type, ctx) {
    super(type, ctx);
    this.children = type.children.map(c => ctx.builder(c.type));
    this.typeMap = type.typeMap;
    this.lookup = type.typeIdForValue;
  }

  init() {
    this.nullCount = 0;
    this.typeIds = buffer(int8Array);
    this.children.forEach(c => c.init());
    return super.init();
  }

  set(value, index) {
    const { children, lookup, typeMap, typeIds } = this;
    this.index = index;
    const typeId = lookup(value, index);
    const child = children[typeMap[typeId]];
    typeIds.set(typeId, index);
    if (value == null) ++this.nullCount;
    // @ts-ignore
    this.update(value, index, child);
  }

  done() {
    const { children, nullCount, type, typeIds } = this;
    const length = this.index + 1;
    return {
      length,
      nullCount,
      type,
      typeIds: typeIds.array(length),
      children: children.map(c => c.batch())
    };
  }
}

/**
 * Builder for sparse union-typed data batches.
 */
export class SparseUnionBuilder extends AbstractUnionBuilder {
  update(value, index, child) {
    // update selected child with value
    // then set all other children to null
    child.set(value, index);
    this.children.forEach(c => { if (c !== child) c.set(null, index) });
  }
}

/**
 * Builder for dense union-typed data batches.
 */
export class DenseUnionBuilder extends AbstractUnionBuilder {
  init() {
    this.offsets = buffer(this.type.offsets);
    return super.init();
  }

  update(value, index, child) {
    const offset = child.index + 1;
    child.set(value, offset);
    this.offsets.set(offset, index);
  }

  done() {
    return {
      ...super.done(),
      offsets: this.offsets.array(this.index + 1)
    };
  }
}
