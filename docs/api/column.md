---
title: Flechette API Reference
---
# Flechette API Reference

[Top-Level](/flechette/api) | [Data Types](data-types) | [Table](table) | [**Column**](column)

## Column Class

A data column. A column provides a view over one or more value batches, each corresponding to part of an Arrow record batch. The Column class supports random access to column values by integer index using the [`at`](#at) method; however, extracting arrays using [`toArray`](#toArray) may provide more efficient means of bulk access and scanning.

* [constructor](#constructor)
* [type](#type)
* [length](#length)
* [nullCount](#nullCount)
* [data](#data)
* [at](#at)
* [get](#get)
* [toArray](#toArray)
* [Symbol.iterator](#iterator)

<hr/><a id="constructor" href="#constructor">#</a>
Column.<b>constructor</b>(<i>data</i>)

Create a new column with the given data batches.

* *data* (`Batch[]`): The column data batches.

<hr/><a id="type" href="#type">#</a>
Column.<b>type</b>

The column [data type](data-types).

<hr/><a id="length" href="#length">#</a>
Column.<b>length</b>

The column length (number of rows).

<hr/><a id="nullCount" href="#nullCount">#</a>
Column.<b>nullCount</b>

The count of null values in the column.

<hr/><a id="data" href="#data">#</a>
Column.<b>data</b>

An array of column data batches.

<hr/><a id="at" href="#at">#</a>
Column.<b>at</b>(<i>index</i>)

Return the column value at the given *index*. If a column has multiple batches, this method performs binary search over the batch lengths to determine the batch from which to retrieve the value. The search makes lookup less efficient than a standard array access. If making multiple full scans of a column, consider extracting an array via `toArray()`.

* *index* (`number`): The row index.

<hr/><a id="get" href="#get">#</a>
Column.<b>get</b>(<i>index</i>)

Return the column value at the given *index*. This method is the same as [`at`](#at) and is provided for better compatibility with Apache Arrow JS.

<hr/><a id="toArray" href="#toArray">#</a>
Column.<b>toArray</b>()

Extract column values into a single array instance. When possible, a zero-copy subarray of the input Arrow data is returned. A typed array is used if possible. If a column contains `null` values, a standard `Array` is created and populated.

<hr/><a id="iterator" href="#iterator">#</a>
Column.<b>[Symbol.iterator]</b>()

Return an iterator over the values in this column.