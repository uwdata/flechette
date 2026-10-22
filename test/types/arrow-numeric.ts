// Run after `pnpm run build`:
// pnpm exec tsc --ignoreConfig --noEmit --strict --skipLibCheck --target es2022 --module node16 test/types/arrow-numeric.ts
import * as arrow from 'apache-arrow';
import { columnFromArray, columnFromValues, int32, tableFromArrays } from '../../dist/types/index-types.js';
import type { ColumnBuilderType, DataType } from '../../dist/types/index-types.js';

const types: ColumnBuilderType[] = [
  new arrow.Int8(), new arrow.Int16(), new arrow.Int32(), new arrow.Int64(),
  new arrow.Uint8(), new arrow.Uint16(), new arrow.Uint32(), new arrow.Uint64(),
  new arrow.Float16(), new arrow.Float32(), new arrow.Float64(), int32()
];
for (const type of types) {
  const column = columnFromArray([1, 2], type);
  const canonical: DataType = column.type;
  columnFromValues([1, 2].values(), type);
  tableFromArrays({ x: [1, 2] }, { types: { x: type } });
  columnFromArray([1, 2], canonical);
}

// Compatibility is deliberately limited to numeric descriptors.
// @ts-expect-error Arrow JS decimal buffers use a different representation.
columnFromArray([1], new arrow.Decimal(2, 10));
// @ts-expect-error Integer signedness must be specified.
columnFromArray([1], { typeId: 2, bitWidth: 32 });
