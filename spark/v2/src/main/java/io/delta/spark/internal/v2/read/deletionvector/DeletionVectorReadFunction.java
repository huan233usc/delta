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
import java.util.Arrays;
import java.util.stream.IntStream;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.ProjectingInternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnVector;
import scala.Function1;
import scala.collection.Iterator;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.AbstractFunction1;

/**
 * Wraps a base reader function to apply deletion vector filtering.
 *
 * <p>Supports both row-based and vectorized reading:
 *
 * <ul>
 *   <li>Row-based: Uses Scala Iterator's flatMap to filter deleted rows and project out DV column
 *   <li>Vectorized: Uses ColumnVectorWithFilter to remap row indices without copying data
 * </ul>
 */
public class DeletionVectorReadFunction
    extends AbstractFunction1<PartitionedFile, Iterator<InternalRow>> implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc;
  private final DvSchemaContext dvContext;

  private DeletionVectorReadFunction(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc, DvSchemaContext dvContext) {
    this.baseReadFunc = baseReadFunc;
    this.dvContext = dvContext;
  }

  @Override
  public Iterator<InternalRow> apply(PartitionedFile file) {
    int dvColumnIndex = dvContext.getDvColumnIndex();
    int outputColumnCount = dvContext.getInputColumnCount() - 1;
    ProjectingInternalRow projection =
        buildProjection(
            dvContext.getOutputSchema(), dvColumnIndex, dvContext.getInputColumnCount());

    // Use flatMap to handle both InternalRow and ColumnarBatch
    return baseReadFunc
        .apply(file)
        .flatMap(
            new AbstractFunction1<InternalRow, Iterator<InternalRow>>() {
              @Override
              public Iterator<InternalRow> apply(InternalRow item) {
                if (item instanceof ColumnarBatch) {
                  ColumnarBatch filtered =
                      filterBatch((ColumnarBatch) item, dvColumnIndex, outputColumnCount);
                  if (filtered != null) {
                    return Iterator.single((InternalRow) (Object) filtered);
                  }
                  return Iterator.empty();
                } else {
                  // Row-based: filter deleted rows, project out DV column
                  if (item.getByte(dvColumnIndex) == 0) {
                    projection.project(item);
                    return Iterator.single(projection);
                  }
                  return Iterator.empty();
                }
              }
            });
  }

  /** Filter a ColumnarBatch by building row ID mapping for live rows. */
  private static ColumnarBatch filterBatch(
      ColumnarBatch batch, int dvColumnIndex, int outputColumnCount) {
    int[] liveRows = findLiveRows(batch, dvColumnIndex);
    if (liveRows.length == 0) {
      return null;
    }

    // Build filtered column vectors (excluding DV column)
    ColumnVector[] filteredVectors = new ColumnVector[outputColumnCount];
    int outIdx = 0;
    for (int i = 0; i < batch.numCols(); i++) {
      if (i != dvColumnIndex) {
        filteredVectors[outIdx++] = new ColumnVectorWithFilter(batch.column(i), liveRows);
      }
    }
    return new ColumnarBatch(filteredVectors, liveRows.length);
  }

  /** Find indices of rows where DV column is 0 (not deleted). */
  private static int[] findLiveRows(ColumnarBatch batch, int dvColumnIndex) {
    ColumnVector dvColumn = batch.column(dvColumnIndex);
    int[] temp = new int[batch.numRows()];
    int count = 0;
    for (int i = 0; i < batch.numRows(); i++) {
      if (dvColumn.getByte(i) == 0) {
        temp[count++] = i;
      }
    }
    return Arrays.copyOf(temp, count);
  }

  /** Build ProjectingInternalRow that skips the DV column. */
  private static ProjectingInternalRow buildProjection(
      StructType outputSchema, int excludeIndex, int inputColumnCount) {
    // Build ordinals: [0, 1, ..., excludeIndex-1, excludeIndex+1, ..., inputColumnCount-1]
    int[] ordinals = IntStream.range(0, inputColumnCount).filter(i -> i != excludeIndex).toArray();
    return ProjectingInternalRow.apply(
        outputSchema,
        CollectionConverters.asScala(
                Arrays.stream(ordinals).boxed().map(i -> (Object) i).iterator())
            .toSeq());
  }

  /** Factory method to wrap a reader function with DV filtering. */
  public static DeletionVectorReadFunction wrap(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc, DvSchemaContext dvContext) {
    return new DeletionVectorReadFunction(baseReadFunc, dvContext);
  }
}
