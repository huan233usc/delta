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

package io.delta.kernel.spark.write;

import io.delta.kernel.spark.utils.SerializableKernelRowWrapper;
import java.io.Serializable;
import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;

/**
 * Factory for creating {@link SparkDVDataWriter} instances on executors.
 *
 * <p>This factory is serialized and sent to executors, where it creates writers that collect row
 * indices for DV-based DELETE/UPDATE operations.
 */
public class SparkDVDataWriterFactory implements DataWriterFactory, Serializable {
  private static final long serialVersionUID = 1L;

  private final List<SerializableKernelRowWrapper> scannedAddFiles;
  private final org.apache.spark.sql.types.StructType inputSchema;

  public SparkDVDataWriterFactory(
      List<SerializableKernelRowWrapper> scannedAddFiles,
      org.apache.spark.sql.types.StructType inputSchema) {
    this.scannedAddFiles = scannedAddFiles;
    this.inputSchema = inputSchema;
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    return new SparkDVDataWriter(partitionId, taskId, 0L, scannedAddFiles, inputSchema);
  }

  public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
    return new SparkDVDataWriter(partitionId, taskId, epochId, scannedAddFiles, inputSchema);
  }
}
