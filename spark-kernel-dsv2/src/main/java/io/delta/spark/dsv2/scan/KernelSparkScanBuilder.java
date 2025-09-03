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
package io.delta.spark.dsv2.scan;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.ScanBuilder;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.spark.dsv2.utils.SchemaUtils;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * A Spark ScanBuilder implementation that wraps Delta Kernel's ScanBuilder. This allows Spark to
 * use Delta Kernel for reading Delta tables.
 */
public class KernelSparkScanBuilder
    implements org.apache.spark.sql.connector.read.ScanBuilder, SupportsPushDownRequiredColumns {

  private final ScanBuilder kernelScanBuilder;
  private final Configuration hadoopConf;
  private final SchemaPartitioner schemaPartitioner;

  private StructType dataSchema;
  private StructType partitionSchema;
  private StructType readSchema;

  public KernelSparkScanBuilder(SnapshotImpl snapshot, Configuration hadoopConf) {
    requireNonNull(snapshot, "snapshot is null");
    this.kernelScanBuilder = snapshot.getScanBuilder();
    this.hadoopConf = hadoopConf;

    // Get partition column names from snapshot and create a partitioner
    List<String> partitionColumnNames = snapshot.getPartitionColumnNames();
    Set<String> partitionColumnSet = new HashSet<>(partitionColumnNames);
    this.schemaPartitioner = new SchemaPartitioner(partitionColumnSet);

    // Get full table schema and partition it
    StructType fullSchema = SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema());
    SchemaPartitionResult result = schemaPartitioner.partition(fullSchema);
    this.dataSchema = result.getDataSchema();
    this.partitionSchema = result.getPartitionSchema();
    this.readSchema = result.getReadSchema();
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    SchemaPartitionResult result = schemaPartitioner.partition(requiredSchema);
    this.dataSchema = result.getDataSchema();
    this.partitionSchema = result.getPartitionSchema();
    this.readSchema = result.getReadSchema();
  }

  @Override
  public Scan build() {
    return new KernelSparkScan(
        kernelScanBuilder.build(), readSchema, dataSchema, partitionSchema, hadoopConf);
  }

  /** Holds the result of partitioning a {@link StructType} into data and partition schemas. */
  private static class SchemaPartitionResult {
    private final StructType dataSchema;
    private final StructType partitionSchema;
    private final StructType readSchema;

    SchemaPartitionResult(StructType dataSchema, StructType partitionSchema) {
      this.dataSchema = dataSchema;
      this.partitionSchema = partitionSchema;

      // Read schema = dataSchema.fields ++ partitionSchema.fields
      List<StructField> readFields = new ArrayList<>();
      readFields.addAll(Arrays.asList(dataSchema.fields()));
      readFields.addAll(Arrays.asList(partitionSchema.fields()));
      this.readSchema = new StructType(readFields.toArray(new StructField[0]));
    }

    StructType getDataSchema() {
      return dataSchema;
    }

    StructType getPartitionSchema() {
      return partitionSchema;
    }

    StructType getReadSchema() {
      return readSchema;
    }
  }

  /**
   * Helper class to partition a {@link StructType} into data and partition schemas based on a set
   * of partition column names.
   */
  private static class SchemaPartitioner {
    private final Set<String> partitionColumns;

    SchemaPartitioner(Set<String> partitionColumns) {
      this.partitionColumns = partitionColumns;
    }

    SchemaPartitionResult partition(StructType schema) {
      List<StructField> partitionFields = new ArrayList<>();
      List<StructField> dataFields = new ArrayList<>();

      Arrays.stream(schema.fields())
          .forEach(
              field -> {
                if (partitionColumns.contains(field.name())) {
                  partitionFields.add(field);
                } else {
                  dataFields.add(field);
                }
              });

      return new SchemaPartitionResult(
          new StructType(dataFields.toArray(new StructField[0])),
          new StructType(partitionFields.toArray(new StructField[0])));
    }
  }
}
