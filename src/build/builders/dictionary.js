import { Column } from '../../column.js';
import { keyString } from '../../util/strings.js';
import { batchType } from '../../batch-type.js';
import { buffer } from '../buffer.js';
import { builder } from '../builder.js';
import { ValidityBuilder } from './validity.js';

export function dictionaryValues(id, type, ctx) {
  const keys = Object.create(null);
  const values = builder(type.type, ctx);
  const batches = [];

  values.init();
  let index = -1;
  type.id = id;

  return {
    type,
    values,

    add(batch) {
      batches.push(batch);
      return batch;
    },

    key(value) {
      const v = keyString(value);
      let k = keys[v];
      if (k === undefined) {
        keys[v] = k = ++index;
        values.set(value, k);
      }
      return k;
    },

    finish(options) {
      const valueType = type.type;
      const batch = new (batchType(valueType, options))(values.done());
      const dictionary = new Column([batch]);
      batches.forEach(batch => batch.setDictionary(dictionary));
    }
  };
}

export class DictionaryBuilder extends ValidityBuilder {
  constructor(type, ctx) {
    super(type, ctx);
    this.dict = ctx.dictionary(type);
  }

  init() {
    this.values = buffer(this.type.keys.values);
    return super.init();
  }

  set(value, index) {
    if (super.set(value, index)) {
      this.values.set(this.dict.key(value), index);
    }
  }

  done() {
    return {
      ...super.done(),
      values: this.values.array(this.index + 1)
    };
  }

  batch() {
    // register batch with dictionary
    // batch will be updated when the dictionary is finished
    return this.dict.add(super.batch());
  }
}