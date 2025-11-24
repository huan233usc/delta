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
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.spark.utils.SerializableKernelRowWrapper;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/**
 * Commit message for DV-based DELETE/UPDATE operations.
 *
 * <p>This message carries information about which rows were deleted/updated in each file, allowing
 * the driver to generate deletion vectors.
 *
 * <p>Each executor task processes a partition of data and identifies which rows in which files
 * should be deleted. The row indices are collected per file and sent back to the driver as part of
 * this commit message.
 */
public class SparkDVCommitMessage implements WriterCommitMessage, Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * Map from file path to the set of deleted row indices in that file.
   *
   * <p>The key is the file path (relative to table root), and the value is a RoaringBitmapArray
   * containing the row indices to delete.
   */
  private final Map<String, RoaringBitmapArray> deletedRowsByFilePath;

  /**
   * Map from file path to the AddFile row for that file.
   *
   * <p>This is needed to generate the new AddFile with DV and the RemoveFile for the old version.
   */
  private final Map<String, SerializableKernelRowWrapper> addFilesByPath;

  public SparkDVCommitMessage(
      Map<String, RoaringBitmapArray> deletedRowsByFilePath, Map<String, Row> addFilesByPath) {
    this.deletedRowsByFilePath = deletedRowsByFilePath;
    this.addFilesByPath = new HashMap<>();
    for (Map.Entry<String, Row> entry : addFilesByPath.entrySet()) {
      this.addFilesByPath.put(entry.getKey(), new SerializableKernelRowWrapper(entry.getValue()));
    }
  }

  /**
   * Get the map of deleted rows by file path.
   *
   * @return Map from file path to deleted row indices
   */
  public Map<String, RoaringBitmapArray> getDeletedRowsByFilePath() {
    return deletedRowsByFilePath;
  }

  /**
   * Get the map of AddFile rows by file path.
   *
   * @return Map from file path to AddFile rows
   */
  public Map<String, Row> getAddFilesByPath() {
    Map<String, Row> result = new HashMap<>();
    for (Map.Entry<String, SerializableKernelRowWrapper> entry : addFilesByPath.entrySet()) {
      result.put(entry.getKey(), entry.getValue().getRow());
    }
    return result;
  }
}
