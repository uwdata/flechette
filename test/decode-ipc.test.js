import { describe, it, expect } from "vitest";
import { readFile } from 'node:fs/promises';
import { decodeIPC } from '../src/decode/decode-ipc.js';
import { decimalDataDecoded } from './util/decimal.js';

describe('decodeIPC', () => {
  it('decodes arrow file format', async () => {
    const buffer = await readFile(`test/data/decimal_test.arrow`);
    const bytes = new Uint8Array(buffer);
    const expected = decimalDataDecoded();
    expect(decodeIPC(bytes)).toStrictEqual(expected);
    expect(decodeIPC(bytes.buffer)).toStrictEqual(expected);
  });

  it('decodes arrow stream format', async () => {
    const buffer = await readFile(`test/data/decimal_test.arrows`);
    const bytes = new Uint8Array(buffer);
    const expected = decimalDataDecoded();
    expect(decodeIPC(bytes)).toStrictEqual(expected);
    expect(decodeIPC([bytes])).toStrictEqual(expected);
    expect(decodeIPC(bytes.buffer)).toStrictEqual(expected);
  });

  it('decodes arrow stream format from multiple buffers', () => {
    // decimal.arrows, divided into separate messages
    const array = [
      Uint8Array.of(255,255,255,255,120,0,0,0,16,0,0,0,0,0,10,0,12,0,6,0,5,0,8,0,10,0,0,0,0,1,4,0,12,0,0,0,8,0,8,0,0,0,4,0,8,0,0,0,4,0,0,0,1,0,0,0,20,0,0,0,16,0,20,0,8,0,6,0,7,0,12,0,0,0,16,0,16,0,0,0,0,0,1,7,16,0,0,0,28,0,0,0,4,0,0,0,0,0,0,0,1,0,0,0,100,0,0,0,8,0,12,0,4,0,8,0,8,0,0,0,18,0,0,0,3,0,0,0),
      Uint8Array.of(255,255,255,255,136,0,0,0,20,0,0,0,0,0,0,0,12,0,22,0,6,0,5,0,8,0,12,0,12,0,0,0,0,3,4,0,24,0,0,0,48,0,0,0,0,0,0,0,0,0,10,0,24,0,12,0,4,0,8,0,10,0,0,0,60,0,0,0,16,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,48,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,232,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,224,46,0,0,0,0,0,0,0,0,0,0,0,0,0,0,208,132,0,0,0,0,0,0,0,0,0,0,0,0,0,0),
      Uint8Array.of(255,255,255,255,0,0,0,0)
    ];
    const expected = decimalDataDecoded();
    expect(decodeIPC(array)).toStrictEqual(expected);
  });

  it('throws on invalid inputs', () => {
    expect(() => decodeIPC('foo')).toThrow();
    expect(() => decodeIPC(['foo'])).toThrow();
  });
});
