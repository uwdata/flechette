import { readBoolean, readInt64AsNum, table } from '../util.js';
import { decodeRecordBatch } from './record-batch.js';

/**
 * Decode a dictionary batch.
 * @param {Uint8Array} buf A byte buffer of binary Arrow IPC data
 * @param {number} index The starting index in the byte buffer
 * @param {import('../types.js').Version_} version Arrow version value
 * @returns {import('../types.js').DictionaryBatch} The dictionary batch
 */
export function decodeDictionaryBatch(buf, index, version) {
  //  4: id
  //  6: data
  //  8: isDelta
  const get = table(buf, index);
  return {
    id: get(4, readInt64AsNum, 0),
    data: get(6, (buf, off) => decodeRecordBatch(buf, off, version)),
    /**
     * If isDelta is true the values in the dictionary are to be appended to a
     * dictionary with the indicated id. If isDelta is false this dictionary
     * should replace the existing dictionary.
     */
    isDelta: get(8, readBoolean, false)
  };
}
