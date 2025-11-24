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

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.spark.utils.SerializableKernelRowWrapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataWriter for DV-based DELETE/UPDATE operations.
 *
 * <p>This writer does NOT write new data files. Instead, it collects the row indices of rows that
 * should be deleted/updated in each existing file. These row indices are sent back to the driver as
 * part of the commit message, where they will be converted into deletion vectors.
 *
 * <p>This writer expects the input data to contain metadata columns: - `_file`: The path of the
 * file containing this row - `_pos`: The row index within that file
 */
public class SparkDVDataWriter implements DataWriter<InternalRow> {
  private static final Logger LOG = LoggerFactory.getLogger(SparkDVDataWriter.class);

  private final int partitionId;
  private final long taskId;
  private final long epochId;
  private final List<SerializableKernelRowWrapper> scannedAddFiles;
  private final org.apache.spark.sql.types.StructType inputSchema;

  /**
   * Map from file path to the set of row indices to delete in that file.
   *
   * <p>As we process rows, we extract the file path and row index from metadata columns and add
   * them to this map.
   */
  private final Map<String, RoaringBitmapArray> deletedRowsByFilePath = new HashMap<>();

  /**
   * Map from file path to the AddFile row for that file.
   *
   * <p>We need this to generate the new AddFile with DV.
   */
  private final Map<String, Row> addFilesByPath = new HashMap<>();

  private long recordCount = 0;

  // Column indices for metadata columns
  private final int filePathColumnIndex;
  private final int fileRowIndexColumnIndex;

  public SparkDVDataWriter(
      int partitionId,
      long taskId,
      long epochId,
      List<SerializableKernelRowWrapper> scannedAddFiles,
      org.apache.spark.sql.types.StructType inputSchema) {
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.epochId = epochId;
    this.scannedAddFiles = scannedAddFiles;
    this.inputSchema = inputSchema;

    // Build a map from file path to AddFile for quick lookup
    // Unwrap the serializable wrappers to get the actual Row objects
    for (SerializableKernelRowWrapper wrapper : scannedAddFiles) {
      Row addFileRow = wrapper.getRow();
      AddFile addFile = new AddFile(addFileRow);
      addFilesByPath.put(addFile.getPath(), addFileRow);
    }

    // Find the indices of metadata columns (_file and _pos)
    this.filePathColumnIndex = inputSchema.fieldIndex("_file");
    this.fileRowIndexColumnIndex = inputSchema.fieldIndex("_pos");

    LOG.info(
        "SparkDVDataWriter created for partition={}, task={}, epoch={}, scannedFiles={}, "
            + "filePathCol={}, fileRowIndexCol={}",
        partitionId,
        taskId,
        epochId,
        scannedAddFiles.size(),
        filePathColumnIndex,
        fileRowIndexColumnIndex);
  }

  @Override
  public void write(InternalRow record) throws IOException {
    // Extract metadata columns: _file and _pos
    // These columns are added by Spark when reading with metadata columns enabled

    // Extract file path (relative to table root)
    String filePath = record.getUTF8String(filePathColumnIndex).toString();

    // Extract row index within the file
    long rowIndex = record.getLong(fileRowIndexColumnIndex);

    // Add this row index to the deletion bitmap for this file
    RoaringBitmapArray bitmap =
        deletedRowsByFilePath.computeIfAbsent(filePath, k -> new RoaringBitmapArray());
    bitmap.add(rowIndex);

    recordCount++;

    if (recordCount % 10000 == 0) {
      LOG.info(
          "SparkDVDataWriter progress: partition={}, recordCount={}, affectedFiles={}",
          partitionId,
          recordCount,
          deletedRowsByFilePath.size());
    }
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    LOG.info(
        "SparkDVDataWriter committing: partition={}, task={}, epoch={}, "
            + "recordCount={}, affectedFiles={}",
        partitionId,
        taskId,
        epochId,
        recordCount,
        deletedRowsByFilePath.size());

    return new SparkDVCommitMessage(deletedRowsByFilePath, addFilesByPath);
  }

  @Override
  public void abort() throws IOException {
    LOG.warn(
        "SparkDVDataWriter aborted: partition={}, task={}, epoch={}", partitionId, taskId, epochId);
    // No cleanup needed since we didn't write any files
  }

  @Override
  public void close() throws IOException {
    // No resources to close
  }
}
