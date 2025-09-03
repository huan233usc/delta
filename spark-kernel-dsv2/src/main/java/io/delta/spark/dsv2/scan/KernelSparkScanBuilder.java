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
  private StructType dataSchema;
  private StructType partitionSchema;
  private StructType readSchema;
  private final Configuration hadoopConf;
  private final Set<String> partitionColumnSet;

  public KernelSparkScanBuilder(SnapshotImpl snapshot, Configuration hadoopConf) {
    requireNonNull(snapshot, "snapshot is null");
    this.kernelScanBuilder = snapshot.getScanBuilder();
    this.hadoopConf = hadoopConf;

    // Get full table schema
    StructType fullSchema = SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema());

    // Get partition column names from snapshot
    List<String> partitionColumnNames = snapshot.getPartitionColumnNames();
    partitionColumnSet = new HashSet<>(partitionColumnNames);

    // Separate partition fields from data fields - similar to Scala version
    List<StructField> partitionFields = new ArrayList<>();
    List<StructField> dataFields = new ArrayList<>();

    for (StructField field : fullSchema.fields()) {
      if (partitionColumnSet.contains(field.name())) {
        partitionFields.add(field);
      } else {
        dataFields.add(field);
      }
    }

    // Create schemas
    this.dataSchema = new StructType(dataFields.toArray(new StructField[0]));
    this.partitionSchema = new StructType(partitionFields.toArray(new StructField[0]));

    // Read schema = dataSchema.fields ++ partitionSchema.fields
    List<StructField> readFields = new ArrayList<>();
    readFields.addAll(Arrays.asList(dataSchema.fields()));
    readFields.addAll(Arrays.asList(partitionSchema.fields()));
    this.readSchema = new StructType(readFields.toArray(new StructField[0]));
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    // Separate partition fields from data fields - similar to Scala version
    List<StructField> partitionFields = new ArrayList<>();
    List<StructField> dataFields = new ArrayList<>();

    for (StructField field : requiredSchema.fields()) {
      if (partitionColumnSet.contains(field.name())) {
        partitionFields.add(field);
      } else {
        dataFields.add(field);
      }
    }

    // Create schemas
    this.dataSchema = new StructType(dataFields.toArray(new StructField[0]));
    this.partitionSchema = new StructType(partitionFields.toArray(new StructField[0]));

    // Read schema = dataSchema.fields ++ partitionSchema.fields
    List<StructField> readFields = new ArrayList<>();
    readFields.addAll(Arrays.asList(dataSchema.fields()));
    readFields.addAll(Arrays.asList(partitionSchema.fields()));
    this.readSchema = new StructType(readFields.toArray(new StructField[0]));
  }

  @Override
  public Scan build() {
    return new KernelSparkScan(
        kernelScanBuilder.build(), readSchema, dataSchema, partitionSchema, hadoopConf);
  }
}
