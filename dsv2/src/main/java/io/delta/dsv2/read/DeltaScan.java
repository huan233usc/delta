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
package io.delta.dsv2.read;

import io.delta.kernel.ResolvedTable;
import io.delta.kernel.Scan;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;

public class DeltaScan implements org.apache.spark.sql.connector.read.Scan, Batch {
  private final ResolvedTable table;
  private final Scan kernelScan;
  private final Engine tableEngine;
  private final StructType sparkReadSchema;
  private final String serializedScanState;
  private final String accessKey;
  private final String secretKey;
  private final String sessionToken;
  private InputPartition[] cachedPartitions;

  public DeltaScan(
      ResolvedTable table,
      Scan kernelScan,
      Engine tableEngine,
      StructType sparkReadSchema,
      String accessKey,
      String secretKey,
      String sessionToken) {
    this.table = table;
    this.kernelScan = kernelScan;
    this.tableEngine = tableEngine;
    this.sparkReadSchema = sparkReadSchema;
    this.serializedScanState = JsonUtils.rowToJson(kernelScan.getScanState(tableEngine));
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.sessionToken = sessionToken;
  }

  @Override
  public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
    return new DeltaMicroBatchStream(
        tableEngine, table, checkpointLocation, accessKey, secretKey, sessionToken);
  }

  /**
   * Get the Kernel ScanFiles ColumnarBatchIter and convert to {@link DeltaInputPartition} array.
   */
  private synchronized InputPartition[] planPartitions() {
    if (cachedPartitions != null) {
      return cachedPartitions;
    }

    List<DeltaInputPartition> scanFileAsInputPartitions = new ArrayList<>();

    Iterator<FilteredColumnarBatch> columnarBatchIterator = kernelScan.getScanFiles(tableEngine);
    while (columnarBatchIterator.hasNext()) {
      FilteredColumnarBatch columnarBatch = columnarBatchIterator.next();
      Iterator<Row> rowIterator = columnarBatch.getRows();

      while (rowIterator.hasNext()) {
        Row row = rowIterator.next();
        String serializedScanFileRow = JsonUtils.rowToJson(row);

        DeltaInputPartition inputPartition =
            new DeltaInputPartition(
                serializedScanFileRow, serializedScanState, accessKey, secretKey, sessionToken);
        scanFileAsInputPartitions.add(inputPartition);
      }
    }

    cachedPartitions = scanFileAsInputPartitions.toArray(new InputPartition[0]);
    return cachedPartitions;
  }

  /////////////////////////
  // SparkScan Overrides //
  /////////////////////////

  @Override
  public StructType readSchema() {
    return sparkReadSchema;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  /////////////////////
  // Batch Overrides //
  /////////////////////

  /**
   * Returns a list of input partitions. Each InputPartition represents a data split that can be
   * processed by one Spark task. The number of input partitions returned here is the same as the
   * number of RDD partitions this scan outputs.
   *
   * <p>If the Scan supports filter pushdown, this Batch is likely configured with a filter and is
   * responsible for creating splits for that filter, which is not a full scan.
   *
   * <p>This method will be called only once during a data source scan, to launch one Spark job.
   */
  @Override
  public InputPartition[] planInputPartitions() {
    return planPartitions();
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new DeltaReaderFactory();
  }
}
