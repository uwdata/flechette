import { describe, it, expect } from "vitest";
import { geoJSONToWKB, geoJSONToWKT } from '@uwdata/wk-geojson';
import { Table, binary, columnFromArray, field, largeBinary, largeUtf8, struct, tableFromIPC, tableToIPC, utf8 } from '../src/index.js';

const geoms = [
  { type: 'Point', coordinates: [1, 2] },
  null,
  { type: 'LineString', coordinates: [[0, 0], [1, 1], [2, 0]] },
  { type: 'Polygon', coordinates: [[[30, 10], [40, 40], [20, 40], [10, 20], [30, 10]]] }
];
const wkb = geoms.map(g => g && geoJSONToWKB(g));
const wkt = geoms.map(g => g && geoJSONToWKT(g));

const extension = name => new Map([['ARROW:extension:name', name]]);

/**
 * Round-trip a single-column table through IPC bytes.
 */
function roundTrip(values, type, metadata, options) {
  const schema = { fields: [field('geom', type, true, metadata)] };
  const table = new Table(schema, [columnFromArray(values, type)]);
  const bytes = tableToIPC(table, { format: 'stream' });
  return tableFromIPC(bytes, options).getChild('geom');
}

describe('GeoArrow', () => {
  for (const [name, type] of [['binary', binary()], ['largeBinary', largeBinary()]]) {
    it(`decodes geoarrow.wkb ${name} data to GeoJSON`, () => {
      const col = roundTrip(wkb, type, extension('geoarrow.wkb'), { useGeoJSON: true });
      expect(col.toArray()).toStrictEqual(geoms);
      expect([...col]).toStrictEqual(geoms);
      expect(geoms.map((_, i) => col.at(i))).toStrictEqual(geoms);
    });
  }

  for (const [name, type] of [['utf8', utf8()], ['largeUtf8', largeUtf8()]]) {
    it(`decodes geoarrow.wkt ${name} data to GeoJSON`, () => {
      const col = roundTrip(wkt, type, extension('geoarrow.wkt'), { useGeoJSON: true });
      expect(col.toArray()).toStrictEqual(geoms);
      expect([...col]).toStrictEqual(geoms);
    });
  }

  it('returns raw WKB and WKT values by default', () => {
    const b = roundTrip(wkb, binary(), extension('geoarrow.wkb'));
    expect(b.toArray()).toStrictEqual(wkb);
    const t = roundTrip(wkt, utf8(), extension('geoarrow.wkt'));
    expect(t.toArray()).toStrictEqual(wkt);
  });

  it('ignores fields without geoarrow metadata', () => {
    expect(roundTrip(wkb, binary(), null, { useGeoJSON: true }).toArray()).toStrictEqual(wkb);
    expect(roundTrip(wkt, utf8(), extension('other'), { useGeoJSON: true }).toArray()).toStrictEqual(wkt);
    // mismatched storage type: WKB extension on a string column
    expect(roundTrip(wkt, utf8(), extension('geoarrow.wkb'), { useGeoJSON: true }).toArray()).toStrictEqual(wkt);
  });

  it('decodes geoarrow data nested in structs', () => {
    const type = struct([field('geom', binary(), true, extension('geoarrow.wkb'))]);
    const values = wkb.map(geom => ({ geom }));
    const col = roundTrip(values, type, null, { useGeoJSON: true });
    expect(col.toArray()).toStrictEqual(geoms.map(geom => ({ geom })));
  });
});
