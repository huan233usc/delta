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
package io.delta.spark.dsv2.scan.batch;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.spark.dsv2.KernelSparkDsv2TestBase;
import io.delta.spark.dsv2.utils.SerializableKernelRowWrapper;
import java.util.HashMap;
import org.junit.jupiter.api.Test;

public class KernelSparkInputPartitionTest extends KernelSparkDsv2TestBase {

  @Test
  public void testConstructorWithNullFileRow() {
    Row mockScanState = new GenericRow(AddFile.SCHEMA_WITHOUT_STATS, new HashMap<>());
    SerializableKernelRowWrapper scanStateWrapper = new SerializableKernelRowWrapper(mockScanState);

    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () -> {
              new KernelSparkInputPartition(scanStateWrapper, null);
            });

    assertEquals("serializedScanFileRow", exception.getMessage());
  }

  //////////////////////
  // Private helpers //
  /////////////////////
  private void createTestTable(String path, String tableName) {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, value DOUBLE) USING delta LOCATION '%s'",
            tableName, path));
    spark.sql(
        String.format("INSERT INTO %s VALUES (1, 'Alice', 10.5), (2, 'Bob', 20.5)", tableName));
  }
}
