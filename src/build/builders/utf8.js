import { encodeUtf8 } from '../../util/strings.js';
import { BinaryBuilder } from './binary.js';

/**
 * Builder for utf8-typed data batches.
 */
export class Utf8Builder extends BinaryBuilder {
  set(value, index) {
    super.set(value && encodeUtf8(value), index);
  }
}
