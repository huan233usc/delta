/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import static io.delta.spark.internal.v2.InternalRowTestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;

public class DeletionVectorReadFunctionTest {

  private static final StructType DATA_SCHEMA =
      new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType);
  private static final StructType PARTITION_SCHEMA = new StructType();

  // ===== Row-based tests =====

  @Test
  public void testRowFilterDeletedRowsAndProjectRemovesDvColumn() {
    List<InternalRow> inputRows =
        List.of(
            row(1, "alice", (byte) 0), // Not deleted.
            row(2, "bob", (byte) 1), // Deleted.
            row(3, "charlie", (byte) 0)); // Not deleted.

    DeletionVectorSchemaContext context =
        new DeletionVectorSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);
    DeletionVectorReadFunction readFunc =
        DeletionVectorReadFunction.wrap(mockReader(inputRows), context);

    List<InternalRow> result = collectRows(readFunc.apply(null));

    assertRowsEquals(result, List.of(row(1, "alice"), row(3, "charlie")));
  }

  @Test
  public void testRowAllRowsDeleted() {
    List<InternalRow> inputRows =
        List.of(row(1, "alice", (byte) 1), row(2, "bob", (byte) 1)); // All deleted.

    DeletionVectorSchemaContext context =
        new DeletionVectorSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);
    DeletionVectorReadFunction readFunc =
        DeletionVectorReadFunction.wrap(mockReader(inputRows), context);

    List<InternalRow> result = collectRows(readFunc.apply(null));

    assertRowsEquals(result, List.of());
  }

  @Test
  public void testRowNoRowsDeleted() {
    List<InternalRow> inputRows =
        List.of(row(1, "alice", (byte) 0), row(2, "bob", (byte) 0), row(3, "charlie", (byte) 0));

    DeletionVectorSchemaContext context =
        new DeletionVectorSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);
    DeletionVectorReadFunction readFunc =
        DeletionVectorReadFunction.wrap(mockReader(inputRows), context);

    List<InternalRow> result = collectRows(readFunc.apply(null));

    assertRowsEquals(result, List.of(row(1, "alice"), row(2, "bob"), row(3, "charlie")));
  }

  // ===== ColumnarBatch (vectorized) tests =====

  @Test
  public void testBatchFilterDeletedRowsAndProjectRemovesDvColumn() {
    // 3 rows: row 1 deleted, rows 0 and 2 kept.
    ColumnarBatch inputBatch = createBatch(new int[] {1, 2, 3}, new byte[] {0, 1, 0});

    DeletionVectorSchemaContext context =
        new DeletionVectorSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);
    DeletionVectorReadFunction readFunc =
        DeletionVectorReadFunction.wrap(mockBatchReader(List.of(inputBatch)), context);

    List<ColumnarBatch> result = collectBatches(readFunc.apply(null));

    assertEquals(1, result.size());
    ColumnarBatch batch = result.get(0);
    assertEquals(2, batch.numRows(), "Should have 2 live rows");
    assertEquals(2, batch.numCols(), "DV column should be removed");
    // Filtered: row 0 -> original 0 (id=1), row 1 -> original 2 (id=3)
    assertEquals(1, batch.column(0).getInt(0));
    assertEquals(3, batch.column(0).getInt(1));
  }

  @Test
  public void testBatchAllRowsDeleted() {
    ColumnarBatch inputBatch = createBatch(new int[] {1, 2}, new byte[] {1, 1});

    DeletionVectorSchemaContext context =
        new DeletionVectorSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);
    DeletionVectorReadFunction readFunc =
        DeletionVectorReadFunction.wrap(mockBatchReader(List.of(inputBatch)), context);

    List<ColumnarBatch> result = collectBatches(readFunc.apply(null));

    assertEquals(1, result.size());
    assertEquals(0, result.get(0).numRows(), "All rows deleted, batch should be empty");
  }

  @Test
  public void testBatchNoRowsDeleted() {
    ColumnarBatch inputBatch = createBatch(new int[] {1, 2, 3}, new byte[] {0, 0, 0});

    DeletionVectorSchemaContext context =
        new DeletionVectorSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);
    DeletionVectorReadFunction readFunc =
        DeletionVectorReadFunction.wrap(mockBatchReader(List.of(inputBatch)), context);

    List<ColumnarBatch> result = collectBatches(readFunc.apply(null));

    assertEquals(1, result.size());
    ColumnarBatch batch = result.get(0);
    assertEquals(3, batch.numRows());
    assertEquals(2, batch.numCols());
    assertEquals(1, batch.column(0).getInt(0));
    assertEquals(2, batch.column(0).getInt(1));
    assertEquals(3, batch.column(0).getInt(2));
  }

  /**
   * Creates a ColumnarBatch with columns [id (int), name (string), is_row_deleted (byte)].
   *
   * <p>Name values are auto-generated as "name_0", "name_1", etc.
   */
  private static ColumnarBatch createBatch(int[] ids, byte[] dvFlags) {
    int numRows = ids.length;
    WritableColumnVector idCol = new OnHeapColumnVector(numRows, DataTypes.IntegerType);
    WritableColumnVector nameCol = new OnHeapColumnVector(numRows, DataTypes.StringType);
    WritableColumnVector dvCol = new OnHeapColumnVector(numRows, DataTypes.ByteType);

    for (int i = 0; i < numRows; i++) {
      idCol.putInt(i, ids[i]);
      nameCol.putByteArray(i, ("name_" + i).getBytes());
      dvCol.putByte(i, dvFlags[i]);
    }

    return new ColumnarBatch(new ColumnVector[] {idCol, nameCol, dvCol}, numRows);
  }
}
