import assert from 'node:assert';
import { tableFromIPC } from 'apache-arrow';
import { decodeIPC } from '../src/decode/decode-ipc.js';
import { encodeIPC } from '../src/encode/encode-ipc.js';
import { MAGIC } from '../src/constants.js';
import { decimalDataDecoded, decimalDataToEncode } from './util/decimal.js';

function arrowJSCheck(input, bytes) {
  // cross-check against arrow-js
  const arrowJS = tableFromIPC(bytes);
  assert.strictEqual(arrowJS.numRows, 3);
  assert.strictEqual(arrowJS.numCols, 1);
  const arrowCol = arrowJS.getChildAt(0);
  const arrowBuf = arrowCol.data[0].values;
  assert.strictEqual(arrowCol.type.typeId, 7);
  assert.deepStrictEqual(
    new Uint8Array(
      arrowBuf.buffer,
      arrowBuf.byteOffset,
      arrowBuf.length * arrowBuf.BYTES_PER_ELEMENT
    ),
    input.records[0].buffers[0]
  );
}

describe('encodeIPC', () => {
  it('encodes arrow file format', () => {
    const input = decimalDataToEncode();
    const expect = decimalDataDecoded();
    const bytes = encodeIPC(input, { format: 'file' }).finish();
    assert.deepStrictEqual(bytes.subarray(0, 6), MAGIC, 'start ARROW1 magic string');
    assert.deepStrictEqual(bytes.slice(-6), MAGIC, 'end ARROW1 magic string');
    assert.deepStrictEqual(decodeIPC(bytes), expect, 'Uint8Array');
    arrowJSCheck(input, bytes);
  });

  it('encodes arrow stream format', () => {
    const input = decimalDataToEncode();
    const expect = decimalDataDecoded();
    const bytes = encodeIPC(input, { format: 'stream' }).finish();
    assert.deepStrictEqual(decodeIPC(bytes), expect, 'Uint8Array');
    arrowJSCheck(input, bytes);
  });
});
