---
title: Flechette API Reference
---
# Flechette API Reference

[Top-Level](/flechette/api) | [Data Types](data-types) | [**Table**](table) | [Column](column)

## Table Class

A table consists of named data [columns](#column) (or 'children'). To extract table data directly to JavaScript values, use [`toColumns()`](#toColumns) to produce an object that maps column names to extracted value arrays, or [`toArray()`](#toArray) to extract an array of row objects. Tables are [iterable](#iterator), iterating over row objects. While `toArray()` and [table iterators](#iterator) enable convenient use by tools that expect row objects, column-oriented processing is more efficient and thus recommended. Use [`getChild`](#getChild) or [`getChildAt`](#getChildAt) to access a specific [`Column`](column).

* [constructor](#constructor)
* [numCols](#numCols)
* [numRows](#numRows)
* [getChildAt](#getChildAt)
* [getChild](#getChild)
* [selectAt](#selectAt)
* [select](#select)
* [at](#at)
* [get](#get)
* [toColumns](#toColumns)
* [toArray](#toArray)
* [Symbol.iterator](#iterator)

<hr/><a id="constructor" href="#constructor">#</a>
Table.<b>constructor</b>(<i>schema</i>, <i>children</i>)

Create a new table with the given *schema* and *children* columns. The column types and order *must* be consistent with the given *schema*.

* *schema* (`Schema`): The table schema.
* *children* (`Column[]`): The table columns.

<hr/><a id="numCols" href="#numCols">#</a>
Table.<b>numCols</b>

The number of columns in the table.

<hr/><a id="numRows" href="#numRows">#</a>
Table.<b>numRows</b>

The number of rows in the table.

<hr/><a id="getChildAt" href="#getChildAt">#</a>
Table.<b>getChildAt</b>(<i>index</i>)

Return the child [column](column) at the given *index* position.

* *index* (`number`): The column index.

<hr/><a id="getChild" href="#getChild">#</a>
Table.<b>getChild</b>(<i>name</i>)

Return the first child [column](column) with the given *name*.

* *name* (`string`): The column name.

<hr/><a id="selectAt" href="#selectAt">#</a>
Table.<b>selectAt</b>(<i>indices</i>[, <i>as</i>])

Construct a new table containing only columns at the specified *indices*. The order of columns in the new table matches the order of input *indices*.

* *indices* (`number[]`): The indices of columns to keep.
* *as* (`string[]`): Optional new names for the selected columns.

<hr/><a id="select" href="#select">#</a>
Table.<b>select</b>(<i>names</i>[, <i>as</i>])

Construct a new table containing only columns with the specified *names*. If columns have duplicate names, the first (with lowest index) is used. The order of columns in the new table matches the order of input *names*.

* *names* (`string[]`): The names of columns to keep.
* *as* (`string[]`): Optional new names for selected columns.

<hr/><a id="at" href="#at">#</a>
Table.<b>at</b>(<i>index</i>)

Return a row object for the given *index*.

* *index* (`number`): The row index.

<hr/><a id="get" href="#get">#</a>
Table.<b>get</b>(<i>index</i>)

Return a row object for the given *index*. This method is the same as [`at`](#at) and is provided for better compatibility with Apache Arrow JS.

<hr/><a id="toColumns" href="#toColumns">#</a>
Table.<b>toColumns</b>()

Return an object that maps column names to extracted value arrays.

<hr/><a id="toArray" href="#toArray">#</a>
Table.<b>toArray</b>()

Return an array of objects representing the rows of this table.

<hr/><a id="iterator" href="#iterator">#</a>
Table.<b>[Symbol.iterator]</b>()

Return an iterator over row objects representing the rows of this table.
