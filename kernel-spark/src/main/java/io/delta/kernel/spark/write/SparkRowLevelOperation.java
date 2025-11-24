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

import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.spark.read.SparkScan;
import io.delta.kernel.spark.read.SparkScanBuilder;
import io.delta.kernel.spark.table.SparkTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Row-level operation implementation for Delta tables using Kernel.
 *
 * <p>This implements Copy-on-Write (COW) semantics:
 *
 * <ul>
 *   <li>Read affected data files via {@link #newScanBuilder}
 *   <li>Spark applies UPDATE/DELETE/MERGE transformations
 *   <li>Write back modified data via {@link #newWriteBuilder}
 *   <li>Transaction commits RemoveFiles (old) + AddFiles (new)
 * </ul>
 *
 * <p>Note: Following Iceberg's pattern, we store non-serializable objects like SparkTable,
 * Snapshot, and Engine. Spark does NOT serialize RowLevelOperation itself - it only serializes
 * DataWriterFactory.
 */
public class SparkRowLevelOperation implements RowLevelOperation {

  private final SparkTable table;
  private final Snapshot snapshot;
  private final Engine engine;
  private final Configuration hadoopConf;
  private final RowLevelOperationInfo info;

  // Store the scan for COW operations - needed to generate RemoveFile actions
  private SparkScan configuredScan;

  public SparkRowLevelOperation(
      SparkTable table,
      Snapshot snapshot,
      Engine engine,
      Configuration hadoopConf,
      RowLevelOperationInfo info) {
    this.table = table;
    this.snapshot = snapshot;
    this.engine = engine;
    this.hadoopConf = hadoopConf;
    this.info = info;
  }

  /** Get the configured scan (needed for COW to generate RemoveFile actions). */
  public SparkScan getConfiguredScan() {
    return configuredScan;
  }

  @Override
  public Command command() {
    return info.command();
  }

  @Override
  public String description() {
    return "kernel-spark row-level " + command() + " operation";
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    // Use kernel's ScanBuilder to read affected files
    // For COW, we need to read entire files that contain matching rows
    return new SparkScanBuilder(
        table.name(), // tableName
        snapshot, // initialSnapshot
        table.snapshotManager(), // snapshotManager
        table.dataSchema(), // dataSchema
        table.partitionSchema(), // partitionSchema
        options) {
      @Override
      public org.apache.spark.sql.connector.read.Scan build() {
        // Build the scan and save it for later use in generating RemoveFile actions
        SparkRowLevelOperation.this.configuredScan = (SparkScan) super.build();
        return SparkRowLevelOperation.this.configuredScan;
      }
    };
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    // Kernel doesn't have specific UPDATE/DELETE/MERGE operations yet
    // Use MANUAL_UPDATE for all row-level operations
    // The actual command is tracked in the row-level operation info
    io.delta.kernel.transaction.UpdateTableTransactionBuilder txnBuilder =
        snapshot.buildUpdateTableTransaction("kernel-spark", Operation.MANUAL_UPDATE);

    // Generate a query ID for this write operation
    String queryId = java.util.UUID.randomUUID().toString();

    // Pass scan information for COW operations (to generate RemoveFile actions)
    // Pass hadoopConf instead of engine for serialization
    return new SparkWriteBuilder(
        txnBuilder, hadoopConf, info.schema(), queryId, configuredScan, engine, snapshot);
  }

  @Override
  public NamedReference[] requiredMetadataAttributes() {
    // Check if deletion vectors are enabled for this table
    io.delta.kernel.internal.SnapshotImpl snapshotImpl =
        (io.delta.kernel.internal.SnapshotImpl) snapshot;
    java.util.Map<String, String> tableConfig = snapshotImpl.getMetadata().getConfiguration();
    String dvEnabled = tableConfig.get("delta.enableDeletionVectors");
    boolean useDeletionVectors = "true".equalsIgnoreCase(dvEnabled);

    if (useDeletionVectors) {
      // For DV-based operations, we need file path and row position metadata columns
      // Following Iceberg's pattern: use simple top-level names (_file, _pos)
      return new NamedReference[] {
        org.apache.spark.sql.connector.expressions.FieldReference.apply("_file"),
        org.apache.spark.sql.connector.expressions.FieldReference.apply("_pos")
      };
    } else {
      // For COW operations, we don't need metadata columns
      // (file grouping is handled by Spark's partitioning)
      return new NamedReference[0];
    }
  }
}
