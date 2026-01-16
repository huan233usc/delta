/*
 * Copyright (2025) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.spark.internal.v2.read.deletionvector;

import java.io.Serializable;
import org.apache.spark.sql.delta.DeltaParquetFileFormat;
import org.apache.spark.sql.types.StructType;

/**
 * Schema context for deletion vector processing in the V2 connector.
 *
 * <p>Encapsulates schema augmentation and pre-computed indices needed for DV filtering.
 */
public class DvSchemaContext implements Serializable {

  private static final long serialVersionUID = 1L;

  private final StructType schemaWithDvColumn;
  private final int dvColumnIndex;
  private final int inputColumnCount;
  private final StructType outputSchema;

  /**
   * Create a DV schema context.
   *
   * @param readDataSchema original data schema without DV column
   * @param partitionSchema partition columns schema
   */
  public DvSchemaContext(StructType readDataSchema, StructType partitionSchema) {
    this.schemaWithDvColumn =
        readDataSchema.add(DeltaParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD());
    this.dvColumnIndex =
        schemaWithDvColumn.fieldIndex(DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME());
    this.inputColumnCount = schemaWithDvColumn.fields().length + partitionSchema.fields().length;
    this.outputSchema = readDataSchema.merge(partitionSchema, /* handleDuplicateColumns= */ false);
  }

  /** Returns schema with the __delta_internal_is_row_deleted column added. */
  public StructType getSchemaWithDvColumn() {
    return schemaWithDvColumn;
  }

  public int getDvColumnIndex() {
    return dvColumnIndex;
  }

  public int getInputColumnCount() {
    return inputColumnCount;
  }

  public StructType getOutputSchema() {
    return outputSchema;
  }
}
