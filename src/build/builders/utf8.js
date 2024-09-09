import { encodeUtf8 } from '../../util/strings.js';
import { BinaryBuilder } from './binary.js';

export class Utf8Builder extends BinaryBuilder {
  set(value, index) {
    super.set(value && encodeUtf8(value), index);
  }
}
