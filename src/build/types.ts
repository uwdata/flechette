import type { Batch } from "../batch.js";
import type { DataType, DictionaryType, ExtractionOptions } from "../types.ts";
import type { BatchBuilder } from "./builders/batch.js";

export interface DictionaryContext {
  get(type: DictionaryType, ctx: BuilderContext): DictionaryValues;
  finish(options: ExtractionOptions): void;
}

export interface DictionaryValues {
  type: DictionaryType;
  values: BatchBuilder;
  add<T extends Batch<unknown>>(batch: T): T;
  key(value: unknown): number;
  finish(options: ExtractionOptions): void;
}

export interface BuilderContext {
  batchType: (type: DataType) => BatchBuilder;
  builder(type: DataType): BatchBuilder;
  dictionary(type: DictionaryType): DictionaryValues;
  finish: () => void;
}
