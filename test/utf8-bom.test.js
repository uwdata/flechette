import { describe, it, expect } from 'vitest';
import { dictionary, largeUtf8, tableFromArrays, tableFromIPC, tableToIPC, utf8 } from '../src/index.js';
import { decodeUtf8, encodeUtf8 } from '../src/util/strings.js';

const BOM = '﻿';
const values = [BOM + 'a', 'b' + BOM, BOM, BOM + BOM, null];

describe('UTF-8 byte order mark', () => {
  it('preserves a leading U+FEFF when decoding', () => {
    for (const s of [BOM, BOM + 'x', BOM + BOM + 'x']) {
      expect(decodeUtf8(encodeUtf8(s))).toBe(s);
    }
  });

  it('round trips U+FEFF in values, field names, and metadata', () => {
    const name = BOM + 'u';
    const table = tableFromArrays(
      { [name]: values, l: values, d: values },
      { types: { [name]: utf8(), l: largeUtf8(), d: dictionary(utf8()) } }
    );
    table.schema.metadata = new Map([[BOM + 'k', BOM + 'v']]);
    table.schema.fields[1].metadata = new Map([[BOM + 'fk', BOM + 'fv']]);

    const result = tableFromIPC(tableToIPC(table));
    expect(result.schema.fields.map(f => f.name)).toStrictEqual([name, 'l', 'd']);
    expect(result.schema.metadata).toStrictEqual(new Map([[BOM + 'k', BOM + 'v']]));
    expect(result.schema.fields[1].metadata).toStrictEqual(new Map([[BOM + 'fk', BOM + 'fv']]));
    for (const f of result.schema.fields) {
      expect(result.getChild(f.name).toArray()).toStrictEqual(values);
    }
  });
});
