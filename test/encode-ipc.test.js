import { describe, it, expect } from "vitest";
import { tableFromIPC as arrowJSTableFromIPC } from 'apache-arrow';
import { decodeIPC } from '../src/decode/decode-ipc.js';
import { encodeIPC } from '../src/encode/encode-ipc.js';
import { MAGIC } from '../src/constants.js';
import { decimalDataDecoded, decimalDataToEncode } from './util/decimal.js';

function arrowJSCheck(input, bytes) {
  // cross-check against arrow-js
  const arrowJS = arrowJSTableFromIPC(bytes);
  expect(arrowJS.numRows).toBe(3);
  expect(arrowJS.numCols).toBe(1);
  const arrowCol = arrowJS.getChildAt(0);
  const arrowBuf = arrowCol.data[0].values;
  expect(arrowCol.type.typeId).toBe(7);
  expect(
    new Uint8Array(
      arrowBuf.buffer,
      arrowBuf.byteOffset,
      arrowBuf.length * arrowBuf.BYTES_PER_ELEMENT
    )
  ).toStrictEqual(input.records[0].buffers[0]);
}

describe('encodeIPC', () => {
  it('encodes arrow file format', () => {
    const input = decimalDataToEncode();
    const expected = decimalDataDecoded();
    const bytes = encodeIPC(input, { format: 'file' }).finish();
    expect(bytes.subarray(0, 6)).toStrictEqual(MAGIC); // start ARROW1 magic string
    expect(bytes.slice(-6)).toStrictEqual(MAGIC); // end ARROW1 magic string
    expect(decodeIPC(bytes)).toStrictEqual(expected); // Uint8Array
    arrowJSCheck(input, bytes);
  });

  it('encodes arrow stream format', () => {
    const input = decimalDataToEncode();
    const expected = decimalDataDecoded();
    const bytes = encodeIPC(input, { format: 'stream' }).finish();
    expect(decodeIPC(bytes)).toStrictEqual(expected); // Uint8Array
    arrowJSCheck(input, bytes);
  });
});
