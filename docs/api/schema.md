---
title: Schema | API Reference
---
# Flechette API Reference <a href="https://idl.uw.edu/flechette"><img align="right" src="../assets/logo.svg" height="38"/></a>

[Top-Level](/flechette/api) | [Data Types](data-types) | [**Schema**](schema) | [Table](table) | [Column](column)

## Schema Object

A schema describes the contents of a table, including all column names and types as well as any associated metadata. A schema is represented as a standard JavaScript object with the following properties:

* *version* (`number`): A number indicating the Arrow version of the data, corresponding to the properties of the top-level `Version` object. When creating new tables, Flechette uses `Version.V5`.
* *endianness* (`number`): The binary [endianness](https://en.wikipedia.org/wiki/Endianness) (byte order) of the Arrow data. Be default, Flechette assumes the value `Endianness.Little`.
* *fields* (`Field[]`): An array of field specifications. See the [field documentation](#field-object) for more details.
* *metadata* (`Map<string, string>`): Custom schema-level metadata annotations.

## Field Object

A field describes the name, data type, and metadata for a collection of data values. A field may correspond to either a top-level column or nested data such as the content of a list, struct, or union type. A field is represented as a standard JavaScript object with the following properties:

* *name* (`string`): The field name.
* *type* (`DataType`): The field data type. See the [Data Types documentation](./data-types) for more.
* *nullable* (`boolean`): Metadata flag indicating if the field values may be set to `null`.
* *metadata* (`Map<string, string>`): Custom field-level metadata annotations.

<hr/><a id="field" href="#field">#</a>
<b>field</b>(<i>name</i>, <i>type</i>[, <i>nullable</i>, <i>metadata</i>])

Create a new field instance for use in a schema or type definition. A field represents a field name, data type, and additional metadata. Fields are used to represent child types within nested types like [List](#list), [Struct](#struct), and [Union](#union).

* *name* (`string`): The field name.
* *type* (`DataType`): The field [data type](./data-types).
* *nullable* (`boolean`): Flag indicating if the field is nullable (default `true`).
* *metadata* (`Map<string,string>`): Custom field metadata annotations (default `null`).
