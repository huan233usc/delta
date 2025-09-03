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

import io.delta.kernel.Scan;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.dsv2.scan.batch.PreparableReadFunction;
import io.delta.spark.dsv2.utils.ScalaUtils;
import io.delta.spark.dsv2.utils.SchemaUtils;
import io.delta.spark.dsv2.utils.SerializableKernelRowWrapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.PartitioningUtils;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * Shared context for Kernel-based Spark scans that manages scan state and partition planning.
 *
 * <p>This class is created on the Driver and handles the conversion of Delta Kernel scan files into
 * Spark InputPartitions. It caches scan file batches to support multiple calls to planPartitions()
 * and ensures consistent partition planning.
 */
public class KernelSparkScanContext {

  private static final Logger LOG = LoggerFactory.getLogger(KernelSparkScanContext.class);

  private final Scan kernelScan;
  private final Engine engine;
  private final Configuration hadoopConf;
  private final Row scanState;
  private final StructType dataSchema;
  private final StructType tablePartitionSchema;
  private final ParquetFileFormat parquetFileFormat;

  /**
   * Cached scan file batches from kernel scan to avoid re-reading log files for replay on multiple
   * planPartitions calls
   */
  private final AtomicReference<Optional<List<FilteredColumnarBatch>>> cachedScanFileBatches;

  public KernelSparkScanContext(
      Scan kernelScan,
      StructType dataSchema,
      StructType tablePartitionSchema,
      Configuration hadoopConf) {
    this.hadoopConf = requireNonNull(hadoopConf, "hadoopConf is null");
    this.engine = DefaultEngine.create(this.hadoopConf);
    this.kernelScan = requireNonNull(kernelScan, "kernelScan is null");
    this.scanState = kernelScan.getScanState(engine);
    this.dataSchema = requireNonNull(dataSchema, "dataSchema is null");
    this.tablePartitionSchema =
        requireNonNull(tablePartitionSchema, "tablePartitionSchema is null");
    this.cachedScanFileBatches = new AtomicReference<>(Optional.empty());
    this.parquetFileFormat = new ParquetFileFormat();
  }

  /**
   * Plans and creates input partitions for Spark execution.
   *
   * <p>This method converts Delta Kernel scan files into Spark InputPartitions. Each file row from
   * the kernel scan creates one input partition containing serialized scan state and file metadata.
   * The InputPartitions are serialized and sent from Driver to Executors.
   *
   * @return array of InputPartitions for Spark to process
   * @throws UncheckedIOException if kernel scan fails or partition creation fails
   */
  public InputPartition[] planPartitions() {
    final List<FilteredColumnarBatch> batches = loadScanFileBatches();
    return convertBatchesToInputPartitions(batches);
  }

  private List<FilteredColumnarBatch> loadScanFileBatches() {
    Optional<List<FilteredColumnarBatch>> batches = cachedScanFileBatches.get();
    if (!batches.isPresent()) {
      synchronized (this) {
        batches = cachedScanFileBatches.get();
        if (!batches.isPresent()) {
          List<FilteredColumnarBatch> batchList = new ArrayList<>();
          try (CloseableIterator<FilteredColumnarBatch> scanFiles =
              kernelScan.getScanFiles(engine)) {
            while (scanFiles.hasNext()) {
              FilteredColumnarBatch batch = scanFiles.next();
              batchList.add(batch);
            }
          } catch (IOException e) {
            LOG.warn("Failed to read from kernel scan", e);
            throw new UncheckedIOException("Failed to read added files from kernel scan", e);
          }
          batches = Optional.of(batchList);
          cachedScanFileBatches.set(batches);
        }
      }
    }
    return batches.get();
  }

  private InputPartition[] convertBatchesToInputPartitions(List<FilteredColumnarBatch> batches) {
    List<InputPartition> partitions = new ArrayList<>();
    for (FilteredColumnarBatch batch : batches) {
      try (CloseableIterator<Row> rows = batch.getRows()) {
        while (rows.hasNext()) {
          Row row = rows.next();
          PartitionedFile partitionedFile = partitionedFileFromKernelRow(row);
          partitions.add(
              new FilePartition(partitions.size(), new PartitionedFile[] {partitionedFile}));
        }
      } catch (IOException e) {
        LOG.warn("Failed to construct input partition", e);
        throw new UncheckedIOException("Failed to construct input partition", e);
      }
    }
    return partitions.toArray(new InputPartition[0]);
  }

  /**
   * Converts a Kernel Row representing a scan file to a Spark PartitionedFile. This method extracts
   * file information and partition values from the row.
   *
   * @param row The Kernel Row containing scan file information
   * @return A PartitionedFile that can be used by Spark's ParquetFileFormat
   */
  private PartitionedFile partitionedFileFromKernelRow(Row row) {
    // Extract file status information
    io.delta.kernel.utils.FileStatus fileStatus =
        io.delta.kernel.internal.InternalScanFileUtils.getAddFileStatus(row);
    org.apache.spark.paths.SparkPath filePath =
        org.apache.spark.paths.SparkPath.fromPathString(fileStatus.getPath());
    long length = fileStatus.getSize();
    long modificationTime = fileStatus.getModificationTime();

    // Extract partition values
    Map<String, String> partitionValueMap =
        io.delta.kernel.internal.InternalScanFileUtils.getPartitionValues(row);

    // Initialize with empty row as default
    org.apache.spark.sql.catalyst.InternalRow partitionValues =
        org.apache.spark.sql.catalyst.InternalRow.empty();

    // Only process if we have partition schema and values
    if (tablePartitionSchema != null
        && tablePartitionSchema.fields().length > 0
        && !partitionValueMap.isEmpty()) {

      // Create array of values matching the partition schema fields
      Object[] values = new Object[tablePartitionSchema.fields().length];
      // For each field in the partition schema
      for (int i = 0; i < tablePartitionSchema.fields().length; i++) {
        org.apache.spark.sql.types.StructField field = tablePartitionSchema.fields()[i];
        String colName = field.name();
        String stringValue = partitionValueMap.get(colName);

        if (stringValue != null) {
          // Cast partition value based on field type in the schema
          Object value =
              PartitioningUtils.castPartValueToDesiredType(
                  field.dataType(),
                  stringValue,
                  DateTimeUtils.getZoneId(SQLConf.get().sessionLocalTimeZone()));

          // Ensure numeric values are not null to avoid NullPointerException in vectorized reading
          if (value == null) {
            // Provide default values for numeric types to avoid NPE in vectorized read
            if (field.dataType() instanceof org.apache.spark.sql.types.ByteType) {
              value = (byte) 0;
            } else if (field.dataType() instanceof org.apache.spark.sql.types.ShortType) {
              value = (short) 0;
            } else if (field.dataType() instanceof org.apache.spark.sql.types.IntegerType) {
              value = 0;
            } else if (field.dataType() instanceof org.apache.spark.sql.types.LongType) {
              value = 0L;
            } else if (field.dataType() instanceof org.apache.spark.sql.types.FloatType) {
              value = 0.0f;
            } else if (field.dataType() instanceof org.apache.spark.sql.types.DoubleType) {
              value = 0.0;
            }
          }

          values[i] = value;
          System.out.println(
              "Partition value for "
                  + field.name()
                  + ": "
                  + values[i]
                  + " (type: "
                  + (values[i] != null ? values[i].getClass().getName() : "null")
                  + ")");
        }
      }
      // Create InternalRow from values - try a simpler approach
      partitionValues =
          org.apache.spark.sql.catalyst.InternalRow.fromSeq(
              scala.collection.JavaConverters.asScalaBuffer(java.util.Arrays.asList(values))
                  .toSeq());
    }

    // File reading parameters
    long start = 0;
    String[] hosts = new String[0];

    // Debug information
    System.out.println("Creating PartitionedFile:");
    System.out.println("  File path: " + filePath);
    System.out.println("  File length: " + length);
    System.out.println("  Partition values: " + partitionValues);
    System.out.println("  Partition schema: " + tablePartitionSchema);

    // Create and return PartitionedFile
    return new PartitionedFile(
        partitionValues, // Partition values
        filePath, // File path
        start, // Start offset
        length, // File length
        hosts, // Host locations
        modificationTime, // Last modified time
        length, // File size
        ScalaUtils.toScalaMap(new HashMap<>())); // Metadata
  }

  /**
   * Creates a reader function that can read data from partitioned files. This method handles the
   * creation of ParquetFileFormat and configures it with the appropriate schemas and configuration.
   *
   * @return A PreparableReadFunction that can read data from files
   */
  public PreparableReadFunction createReaderFunction() {
    SparkSession sparkSession = SparkSession.active();
    Filter[] filtersArray = new Filter[0];
    Seq<Filter> filters =
        JavaConverters.asScalaBufferConverter(java.util.Arrays.asList(filtersArray))
            .asScala()
            .toSeq();
    Map<String, String> options = new HashMap<>();
    options.put(FileFormat.OPTION_RETURNING_BATCH(), String.valueOf(supportColumnar()));
    Function1<PartitionedFile, Iterator<InternalRow>> scalaReadFunc =
        parquetFileFormat.buildReaderWithPartitionValues(
            sparkSession,
            SchemaUtils.convertKernelSchemaToSparkSchema(
                ScanStateRow.getLogicalSchema(scanState)), // Data columns only
            tablePartitionSchema, // Partition columns only
            dataSchema,
            filters,
            ScalaUtils.toScalaMap(options),
            hadoopConf);
    return (PartitionedFile file) -> scalaReadFunc.apply(file);
  }

  public boolean supportColumnar() {
    return parquetFileFormat.supportBatch(
        SparkSession.active(),
        SchemaUtils.convertKernelSchemaToSparkSchema(ScanStateRow.getLogicalSchema(scanState)));
  }
}
