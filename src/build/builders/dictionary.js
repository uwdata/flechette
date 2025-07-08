/**
 * @import { builderContext } from '../builder.js'
 * @import { DictionaryType, ExtractionOptions } from '../../types.js'
 */
import { Column } from '../../column.js';
import { keyString } from '../../util/strings.js';
import { batchType } from '../../batch-type.js';
import { buffer } from '../buffer.js';
import { ValidityBuilder } from './validity.js';

/**
 * Create a context object for managing dictionary builders.
 */
export function dictionaryContext() {
  const idMap = new Map;
  const dicts = new Set;
  return {
    /**
     * Get a dictionary values builder for the given dictionary type.
     * @param {DictionaryType} type
     *  The dictionary type.
     * @param {*} ctx The builder context.
     * @returns {ReturnType<dictionaryValues>}
     */
    get(type, ctx) {
      // if a dictionary has a non-negative id, assume it was set
      // intentionally and track it for potential reuse across columns
      // otherwise the dictionary is used for a single column only
      const id = type.id;
      if (id >= 0 && idMap.has(id)) {
        return idMap.get(id);
      } else {
        const dict = dictionaryValues(type, ctx);
        if (id >= 0) idMap.set(id, dict);
        dicts.add(dict);
        return dict;
      }
    },
    /**
     * Finish building dictionary values columns and assign them to
     * their corresponding dictionary batches.
     * @param {ExtractionOptions} options
     */
    finish(options) {
      dicts.forEach(dict => dict.finish(options));
    }
  };
}

/**
 * Builder helper for creating dictionary values.
 * @param {DictionaryType} type
 *  The dictionary data type.
 * @param {ReturnType<builderContext>} ctx
 *  The builder context.
 */
export function dictionaryValues(type, ctx) {
  const keys = Object.create(null);
  const values = ctx.builder(type.dictionary);
  const batches = [];

  values.init();
  let index = -1;

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
      const valueType = type.dictionary;
      const batch = new (batchType(valueType, options))(values.done());
      const dictionary = new Column([batch]);
      batches.forEach(batch => batch.setDictionary(dictionary));
    }
  };
}

/**
 * Builder for dictionary-typed data batches.
 */
export class DictionaryBuilder extends ValidityBuilder {
  constructor(type, ctx) {
    super(type, ctx);
    this.dict = ctx.dictionary(type);
  }

  init() {
    this.values = buffer(this.type.indices.values);
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
