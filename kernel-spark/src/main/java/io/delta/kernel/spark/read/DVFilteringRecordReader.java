/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.spark.read;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import scala.collection.Iterator;
import scala.collection.JavaConverters;

/**
 * A RecordReader wrapper that filters out deleted rows based on deletion vectors and projects out
 * internal columns (__delta_internal_is_row_deleted and _tmp_metadata_row_index).
 *
 * <p>This reader wraps the output of DeltaParquetFileFormatV2, which includes the DV column. It
 * filters rows where the DV column is non-zero and removes the DV column from the output.
 */
public class DVFilteringRecordReader extends RecordReader<Void, Object> {
  private final Iterator<InternalRow> baseIterator;
  private final int dvColumnIndex;
  private final Set<Integer> columnsToRemoveIndices;
  private final int numOutputFields;
  private Object currentValue;
  private boolean finished;

  public DVFilteringRecordReader(
      Iterator<InternalRow> baseIterator,
      int dvColumnIndex,
      int[] columnsToRemoveIndices,
      int numOutputFields) {
    this.baseIterator = baseIterator;
    this.dvColumnIndex = dvColumnIndex;
    this.columnsToRemoveIndices = new HashSet<>();
    for (int idx : columnsToRemoveIndices) {
      this.columnsToRemoveIndices.add(idx);
    }
    this.numOutputFields = numOutputFields;
    this.currentValue = null;
    this.finished = false;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
    // No initialization needed - iterator is already created
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    if (finished) {
      return false;
    }

    // Keep reading until we find a non-deleted row/batch
    while (baseIterator.hasNext()) {
      Object item = baseIterator.next();

      if (item instanceof ColumnarBatch) {
        ColumnarBatch batch = (ColumnarBatch) item;
        ColumnarBatch filtered = filterColumnarBatch(batch);
        if (filtered.numRows() > 0) {
          currentValue = filtered;
          return true;
        }
        // All rows deleted, continue to next batch
      } else if (item instanceof InternalRow) {
        InternalRow row = (InternalRow) item;
        byte isDeleted = row.getByte(dvColumnIndex);
        if (isDeleted == 0) {
          currentValue = projectRow(row);
          return true;
        }
        // Row deleted, continue
      } else {
        // Unknown type, pass through
        currentValue = item;
        return true;
      }
    }

    finished = true;
    return false;
  }

  @Override
  public Void getCurrentKey() {
    return null;
  }

  @Override
  public Object getCurrentValue() {
    return currentValue;
  }

  @Override
  public float getProgress() {
    return finished ? 1.0f : 0.0f;
  }

  @Override
  public void close() throws IOException {
    // Base iterator will be closed by Spark
  }

  private ColumnarBatch filterColumnarBatch(ColumnarBatch batch) {
    int batchSize = batch.numRows();
    int numCols = batch.numCols();

    // Safety check: ensure DV column index is valid
    if (dvColumnIndex >= numCols) {
      // DV column not present in batch, return as-is (shouldn't happen, but defensive)
      return batch;
    }

    ColumnVector dvColumn = batch.column(dvColumnIndex);

    // Build row ID mapping
    int[] rowIdMapping = new int[batchSize];
    int numLiveRows = 0;

    for (int rowId = 0; rowId < batchSize; rowId++) {
      byte isDeleted = dvColumn.getByte(rowId);
      if (isDeleted == 0) {
        rowIdMapping[numLiveRows++] = rowId;
      }
    }

    if (numLiveRows == 0) {
      return new ColumnarBatch(new ColumnVector[0], 0);
    }

    int[] trimmedMapping = new int[numLiveRows];
    System.arraycopy(rowIdMapping, 0, trimmedMapping, 0, numLiveRows);

    // Create filtered column vectors - only keep columns that exist in batch and aren't removed
    ColumnVector[] filteredVectors = new ColumnVector[numOutputFields];
    int outputIdx = 0;

    for (int i = 0; i < numCols; i++) {
      if (!columnsToRemoveIndices.contains(i)) {
        filteredVectors[outputIdx++] = new ColumnVectorWithFilter(batch.column(i), trimmedMapping);
      }
    }

    ColumnarBatch result = new ColumnarBatch(filteredVectors);
    result.setNumRows(numLiveRows);
    return result;
  }

  private InternalRow projectRow(InternalRow row) {
    Object[] values = new Object[numOutputFields];
    int outputIndex = 0;

    for (int i = 0; i < row.numFields(); i++) {
      if (!columnsToRemoveIndices.contains(i)) {
        values[outputIndex++] = row.get(i, null);
      }
    }

    return InternalRow.fromSeq(
        JavaConverters.asScalaIterator(Arrays.asList(values).iterator()).toSeq());
  }
}
