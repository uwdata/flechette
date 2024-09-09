import { DateUnit, IntervalUnit, Precision, TimeUnit, Type, UnionMode } from './constants.js';
import { intArrayType, float32Array, float64Array, int32Array, int64Array, uint16Array, uint64Array } from './util/arrays.js';
import { check, checkOneOf, keyFor } from './util/objects.js';

/**
 * @typedef {import('./types.js').Field | import('./types.js').DataType} FieldInput
 */

export const invalidDataType = (typeId) =>
  `Unsupported data type: "${keyFor(Type, typeId)}" (id ${typeId})`;

/**
 * Return a new Arrow field definition.
 * @param {string} name The field name.
 * @param {import('./types.js').DataType} type The field data type.
 * @param {boolean} [nullable=true] The nullable flag.
 * @param {Map<string,string>|null} [metadata=null] The field metadata.
 * @returns {import('./types.js').Field} The field instance.
 */
export const field = (name, type, nullable = true, metadata = null) => ({
  name,
  nullable,
  type,
  metadata
});

/**
 * Checks if a value is a field instance.
 * @param {any} value
 * @returns {value is import('./types.js').Field}
 */
function isField(value) {
  return Object.hasOwn(value, 'name') && isDataType(value.type)
}

/**
 * Checks if a value is a data type instance.
 * @param {any} value
 * @returns {value is import('./types.js').DataType}
 */
function isDataType(value) {
  return typeof value?.typeId === 'number';
}

/**
 * Return a field instance from a field or data type input.
 * @param {FieldInput} value
 *  The value to map to a field.
 * @param {string} [defaultName] The default field name.
 * @param {boolean} [defaultNullable=true] The default nullable value.
 * @returns {import('./types.js').Field} The field instance.
 */
function asField(value, defaultName = '', defaultNullable = true) {
  return isField(value)
    ? value
    : field(
        defaultName,
        check(value, isDataType, () => `Data type expected.`),
        defaultNullable
      );
}

/////

/**
 * Return a basic type with only a type id.
 * @param {typeof Type[keyof typeof Type]} typeId The type id.
 */
const basicType = (typeId) => ({ typeId });

/**
 * Return a Dictionary type instance.
 * @returns {import('./types.js').DictionaryType}
 */
export const dictionary = (type, keyType, ordered = false, id = -1) => ({
  typeId: Type.Dictionary,
  type,
  id,
  keys: keyType || int32(),
  ordered
});

/**
 * Return a Null type instance.
 * @returns {import('./types.js').NullType} The null data type.
 */
export const nullType = () => /** @type{import('./types.js').NullType} */
  (basicType(Type.Null));

/**
 * Return an Int data type instance.
 * @param {import('./types.js').IntBitWidth} [bitWidth=32] The integer bit width.
 * @param {boolean} [signed=true] Flag for signed or unsigned integers.
 * @returns {import('./types.js').IntType} The integer data type.
 */
export const int = (bitWidth = 32, signed = true) => ({
  typeId: Type.Int,
  bitWidth: checkOneOf(bitWidth, [8, 16, 32, 64]),
  signed,
  values: intArrayType(bitWidth, signed)
});
export const int8 = () => int(8);
export const int16 = () => int(16);
export const int32 = () => int(32);
export const int64 = () => int(64);
export const uint8 = () => int(8, false);
export const uint16 = () => int(16, false);
export const uint32 = () => int(32, false);
export const uint64 = () => int(64, false);

/**
 * Return a Float data type instance.
 * @param {import('./types.js').Precision_} [precision=2] The floating point precision.
 * @returns {import('./types.js').FloatType} The floating point data type.
 */
export const float = (precision = 2) => ({
  typeId: Type.Float,
  precision: checkOneOf(precision, Precision),
  values: [uint16Array, float32Array, float64Array][precision]
});
export const float16 = () => float(Precision.HALF);
export const float32 = () => float(Precision.SINGLE);
export const float64 = () => float(Precision.DOUBLE);

/**
 * Return a Binary type instance.
 * @returns {import('./types.js').BinaryType} The binary data type.
 */
export const binary = () => ({
  typeId: Type.Binary,
  offsets: int32Array
});

/**
 * Return a Utf8 type instance.
 * @returns {import('./types.js').Utf8Type} The utf8 data type.
 */
export const utf8 = () => ({
  typeId: Type.Utf8,
  offsets: int32Array
});

/**
 * Return a Bool type instance.
 * @returns {import('./types.js').BoolType} The bool data type.
 */
export const bool = () => /** @type{import('./types.js').BoolType} */ (basicType(Type.Bool));

/**
 * Return a Decimal type instance.
 * @param {number} precision The decimal precision.
 * @param {number} scale The number of digits beyond the decimal point.
 * @param {128 | 256} bitWidth The decimal bit width.
 * @returns {import('./types.js').DecimalType} The decimal data type.
 */
export const decimal = (precision, scale, bitWidth) => ({
  typeId: Type.Decimal,
  precision,
  scale,
  bitWidth: checkOneOf(bitWidth, [128, 256]),
  values: uint64Array
});

/**
 * Return a Date type instance.
 * @param {import('./types.js').DateUnit_} unit
 * @returns {import('./types.js').DateType} The date data type.
 */
export const date = (unit) => ({
  typeId: Type.Date,
  unit: checkOneOf(unit, DateUnit),
  values: unit === DateUnit.DAY ? int32Array : int64Array
});
export const dateDay = () => date(DateUnit.DAY);
export const dateMillisecond = () => date(DateUnit.MILLISECOND);

/**
 * Return a Time type instance.
 * @param {import('./types.js').TimeUnit_} unit
 * @param {32 | 64} bitWidth
 * @returns {import('./types.js').TimeType} The time data type.
 */
export const time = (unit = TimeUnit.MILLISECOND, bitWidth = 64) => ({
  typeId: Type.Time,
  unit: checkOneOf(unit, TimeUnit),
  bitWidth: checkOneOf(bitWidth, [32, 64]),
  values: bitWidth === 32 ? int32Array : int64Array
});
export const timeSecond = (bitWidth) => time(TimeUnit.SECOND, bitWidth);
export const timeMillisecond = (bitWidth) => time(TimeUnit.MILLISECOND, bitWidth);
export const timeMicrosecond = (bitWidth) => time(TimeUnit.MICROSECOND, bitWidth);
export const timeNanosecond = (bitWidth) => time(TimeUnit.NANOSECOND, bitWidth);

/**
 * Return a Timestamp type instance.
 * @param {import('./types.js').TimeUnit_} unit
 * @param {string|null} [timezone=null]
 * @returns {import('./types.js').TimestampType} The time data type.
 */
export const timestamp = (unit = TimeUnit.MILLISECOND, timezone = null) => ({
  typeId: Type.Timestamp,
  unit: checkOneOf(unit, TimeUnit),
  timezone,
  values: int64Array
});

/**
 * Return an Interval type instance.
 * @param {import('./types.js').IntervalUnit_} unit
 * @returns {import('./types.js').IntervalType} The interval data type.
 */
export const interval = (unit = IntervalUnit.MONTH_DAY_NANO) => ({
  typeId: Type.Interval,
  unit: checkOneOf(unit, IntervalUnit),
  values: unit === IntervalUnit.MONTH_DAY_NANO ? undefined : int32Array
});

/**
 * Return a List type instance.
 * @param {FieldInput} type The child (list item) field or data type.
 * @returns {import('./types.js').ListType} The fixed size binary data type.
 */
export const list = (type) => ({
  typeId: Type.List,
  children: [ asField(type) ],
  offsets: int32Array
});

/**
 * Return a Struct type instance.
 * @param {import('./types.js').Field[] | Record<string, import('./types.js').DataType>} children
 *  The property fields, or an object mapping property names to data types.
 *  If an object, fields are assumed to be nullable and have no metadata.
 * @returns {import('./types.js').StructType} The struct data type.
 */
export const struct = (children) => ({
  typeId: Type.Struct,
  children: Array.isArray(children) && isField(children[0])
    ? /** @type {import('./types.js').Field[]} */ (children)
    : Object.entries(children).map(([name, type]) => field(name, type))
});

/**
 * Return a Union type instance.
 * @param {import('./types.js').UnionMode_} mode The union mode.
 * @param {FieldInput[]} children The children fields or data types.
 *  Types are mapped to nullable fields with no metadata.
 * @param {number[]} [typeIds] Children type ids. If not provided,
 *  the children indices are used directly.
 * @param {(value: any, index: number) => number} [typeIdForValue]
 *  A function that takes an arbitrary value and a row index and
 *  returns a correponding union type id.
 * @returns {import('./types.js').UnionType} The union data type.
 */
export const union = (mode, children, typeIds, typeIdForValue) => {
  typeIds ??= children.map((v, i) => i);
  return {
    typeId: Type.Union,
    mode: checkOneOf(mode, UnionMode),
    typeIds,
    typeMap: typeIds.reduce((m, id, i) => ((m[id] = i), m), {}),
    children: children.map((v, i) => asField(v, `_${i}`)),
    typeIdForValue,
    offsets: int32Array,
  };
};

/**
 * Return a FixedSizeBinary type instance.
 * @param {number} stride The fixed size in bytes.
 * @returns {import('./types.js').FixedSizeBinaryType} The fixed size binary data type.
 */
export const fixedSizeBinary = (stride) => ({
  typeId: Type.FixedSizeBinary,
  stride
});

/**
 * Return a FixedSizeList type instance.
 * @param {FieldInput} child The list item data type.
 * @param {number} stride The fixed list size.
 * @returns {import('./types.js').FixedSizeListType} The fixed size binary data type.
 */
export const fixedSizeList = (child, stride) => ({
  typeId: Type.FixedSizeList,
  stride,
  children: [ asField(child) ]
});

/**
 * Internal method to create a Map type instance.
 * @param {boolean} keysSorted Flag indicating if the map keys are sorted.
 * @param {import('./types.js').Field} child The child fields.
 * @returns {import('./types.js').MapType} The map data type.
 */
export const mapType = (keysSorted, child) => ({
  typeId: Type.Map,
  keysSorted,
  children: [child],
  offsets: int32Array
});

/**
 * Return a Map type instance.
 * @param {FieldInput} keyField The map key field or data type.
 * @param {FieldInput} valueField The map value field or data type.
 * @param {boolean} [keysSorted=false] Flag indicating if the map keys are sorted.
 * @returns {import('./types.js').MapType} The map data type.
 */
export const map = (keyField, valueField, keysSorted = false) => mapType(
  keysSorted,
  field(
    'entries',
    struct([ asField(keyField, 'key', false), asField(valueField, 'value') ]),
    false
  )
);

/**
 * Return a Duration type instance.
 * @param {import('./types.js').TimeUnit_} unit
 * @returns {import('./types.js').DurationType} The duration data type.
 */
export const duration = (unit = TimeUnit.MILLISECOND) => ({
  typeId: Type.Duration,
  unit: checkOneOf(unit, TimeUnit),
  values: int64Array
});

/**
 * Return a LargeBinary type instance.
 * @returns {import('./types.js').LargeBinaryType} The large binary data type.
 */
export const largeBinary = () => ({
  typeId: Type.LargeBinary,
  offsets: int64Array
});

/**
 * Return a LargeUtf8 type instance.
 * @returns {import('./types.js').LargeUtf8Type} The large utf8 data type.
 */
export const largeUtf8 = () => ({
  typeId: Type.LargeUtf8,
  offsets: int64Array
});

/**
 * Return a LargeList type instance.
 * @param {FieldInput} child The child (list item) field or data type.
 * @returns {import('./types.js').LargeListType} The large list data type.
 */
export const largeList = (child) => ({
  typeId: Type.LargeList,
  children: [ asField(child) ],
  offsets: int64Array
});

/**
 * Return a RunEndEncoded type instance.
 * @param {FieldInput} runsField The run-ends field or data type.
 * @param {FieldInput} valuesField The value field or data type.
 * @returns {import('./types.js').RunEndEncodedType} The large list data type.
 */
export const runEndEncoded = (runsField, valuesField) => ({
  typeId: Type.RunEndEncoded,
  children: [
    check(
      asField(runsField, 'run_ends'),
      (field) => field.type.typeId === Type.Int,
      () => 'Run-ends must have an integer type.'
    ),
    asField(valuesField, 'values')
  ]
});

/**
 * Return a BinaryView type instance.
 * @returns {import('./types.js').BinaryViewType} The binary view data type.
 */
export const binaryView = () => /** @type{import('./types.js').BinaryViewType} */
  (basicType(Type.BinaryView));

/**
 * Return a Utf8View type instance.
 * @returns {import('./types.js').Utf8ViewType} The binary view data type.
 */
export const utf8View = () => /** @type{import('./types.js').Utf8ViewType} */
  (basicType(Type.Utf8View));

/**
 * Return a ListView type instance.
 * @param {FieldInput} child The child (list item) field or data type.
 * @returns {import('./types.js').ListViewType} The list view data type.
 */
export const listView = (child) => ({
  typeId: Type.ListView,
  children: [ asField(child, 'value') ],
  offsets: int32Array
});

/**
 * Return a LargeListView type instance.
 * @param {FieldInput} child The child (list item) field or data type.
 * @returns {import('./types.js').LargeListViewType} The large list view data type.
 */
export const largeListView = (child) => ({
  typeId: Type.LargeListView,
  children: [ asField(child, 'value') ],
  offsets: int64Array
});
