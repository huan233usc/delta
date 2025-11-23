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

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataWriter implementation that writes data to Delta table using Kernel APIs.
 *
 * <p>This runs on Spark executors and:
 *
 * <ol>
 *   <li>Buffers InternalRow data
 *   <li>Writes data to Parquet files using Spark's writer
 *   <li>Generates Delta AddFile actions via Kernel
 *   <li>Returns actions to driver for commit
 * </ol>
 */
public class SparkDataWriter implements DataWriter<InternalRow> {
  private static final Logger LOG = LoggerFactory.getLogger(SparkDataWriter.class);

  private final Row transactionState;
  private final Engine engine;
  private final StructType sparkSchema;
  private final String queryId;
  private final int partitionId;
  private final long taskId;

  private final List<String> partitionColumns;
  private final DataWriteContext writeContext;

  private final List<InternalRow> buffer;
  private final List<DataFileStatus> writtenFiles;

  private final Configuration hadoopConf;
  private final OutputWriterFactory writerFactory;
  private final String targetDirectory;
  private OutputWriter currentWriter;
  private String currentFilePath;

  private long recordCount = 0;

  public SparkDataWriter(
      Row transactionState,
      Engine engine,
      Configuration hadoopConf,
      OutputWriterFactory writerFactory,
      StructType sparkSchema,
      String queryId,
      int partitionId,
      long taskId) {
    this.transactionState = transactionState;
    this.engine = engine;
    this.hadoopConf = hadoopConf;
    this.writerFactory = writerFactory;
    this.sparkSchema = sparkSchema;
    this.queryId = queryId;
    this.partitionId = partitionId;
    this.taskId = taskId;

    // Get partition columns from transaction state
    this.partitionColumns = TransactionStateRow.getPartitionColumnsList(transactionState);

    // For now, assume unpartitioned table (empty partition values)
    // TODO: Support partitioned tables by extracting partition values from data
    Map<String, Literal> partitionValues = Collections.emptyMap();
    this.writeContext = Transaction.getWriteContext(engine, transactionState, partitionValues);

    this.buffer = new ArrayList<>();
    this.writtenFiles = new ArrayList<>();

    // Initialize target directory
    this.targetDirectory = writeContext.getTargetDirectory();
    this.currentWriter = null;
    this.currentFilePath = null;

    LOG.info(
        "Created SparkDataWriter for partition={}, task={}, query={}, targetDir={}",
        partitionId,
        taskId,
        queryId,
        targetDirectory);
  }

  @Override
  public void write(InternalRow record) throws IOException {
    // Lazily initialize the writer on first write
    if (currentWriter == null) {
      initializeWriter();
    }

    // Write the row to the current Parquet file
    currentWriter.write(record);
    recordCount++;
  }

  /** Initialize the Parquet writer for writing data. */
  private void initializeWriter() throws IOException {
    try {
      // Set output path
      Path outputPath = new Path(targetDirectory);

      // Generate unique file name
      String fileName =
          String.format(
              "part-%05d-%s-%05d-%s.snappy.parquet",
              partitionId, UUID.randomUUID().toString(), taskId, queryId);

      currentFilePath = new Path(outputPath, fileName).toString();

      // Create TaskAttemptContext
      TaskAttemptID attemptId =
          new TaskAttemptID(
              String.format("task_%s", queryId),
              0, // job id
              org.apache.hadoop.mapreduce.TaskType.REDUCE,
              partitionId,
              (int) taskId);
      TaskAttemptContext taskContext = new TaskAttemptContextImpl(hadoopConf, attemptId);

      // Create the actual OutputWriter using the pre-configured writerFactory
      // This writerFactory was prepared on the driver and serialized to the executor
      currentWriter = writerFactory.newInstance(currentFilePath, sparkSchema, taskContext);

      LOG.info("Initialized Parquet writer for file: {}", currentFilePath);

    } catch (Exception e) {
      LOG.error("Failed to initialize Parquet writer", e);
      throw new IOException("Failed to initialize Parquet writer", e);
    }
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    LOG.info(
        "Committing writer for partition={}, task={}, records={}",
        partitionId,
        taskId,
        recordCount);

    try {
      // Close the current writer if it's open
      if (currentWriter != null) {
        currentWriter.close();

        // Collect file status for the written file
        DataFileStatus fileStatus = collectFileStatus(currentFilePath);
        writtenFiles.add(fileStatus);

        currentWriter = null;
        currentFilePath = null;
      }

      // Generate AddFile actions for all written files using Kernel API
      CloseableIterator<DataFileStatus> fileStatusIter =
          toCloseableIterator(writtenFiles.iterator());

      CloseableIterator<Row> actionsIter =
          Transaction.generateAppendActions(engine, transactionState, fileStatusIter, writeContext);

      // Collect all actions and wrap them for serialization
      List<io.delta.kernel.spark.utils.SerializableKernelRowWrapper> actionWrappers =
          new ArrayList<>();
      while (actionsIter.hasNext()) {
        Row action = actionsIter.next();
        actionWrappers.add(new io.delta.kernel.spark.utils.SerializableKernelRowWrapper(action));
      }

      LOG.info(
          "Generated {} AddFile actions for partition={}, task={}",
          actionWrappers.size(),
          partitionId,
          taskId);

      return new SparkWriterCommitMessage(
          actionWrappers.toArray(new io.delta.kernel.spark.utils.SerializableKernelRowWrapper[0]));

    } catch (Exception e) {
      LOG.error("Failed to commit writer for partition={}, task={}", partitionId, taskId, e);
      throw new IOException("Failed to commit writer", e);
    }
  }

  /** Collect file status for a written Parquet file. */
  private DataFileStatus collectFileStatus(String filePath) throws IOException {
    try {
      Path path = new Path(filePath);
      FileSystem fs = path.getFileSystem(hadoopConf);
      FileStatus fileStatus = fs.getFileStatus(path);

      // Create DataFileStatistics with record count
      // For now, we only provide numRecords; full stats (min/max/null counts) would require
      // reading Parquet footer or computing during write
      io.delta.kernel.statistics.DataFileStatistics stats =
          new io.delta.kernel.statistics.DataFileStatistics(
              recordCount, // numRecords
              Collections.emptyMap(), // minValues - would need Parquet footer
              Collections.emptyMap(), // maxValues - would need Parquet footer
              Collections.emptyMap(), // nullCount - would need Parquet footer
              java.util.Optional.empty() // tightBounds
              );

      // Create DataFileStatus with statistics
      DataFileStatus dataFileStatus =
          new DataFileStatus(
              fileStatus.getPath().toString(),
              fileStatus.getLen(),
              fileStatus.getModificationTime(),
              java.util.Optional.of(stats));

      LOG.info(
          "Collected file status for {}: size={}, records={}",
          filePath,
          fileStatus.getLen(),
          recordCount);

      return dataFileStatus;

    } catch (Exception e) {
      LOG.error("Failed to collect file status for {}", filePath, e);
      throw new IOException("Failed to collect file status", e);
    }
  }

  @Override
  public void abort() throws IOException {
    LOG.warn("Aborting writer for partition={}, task={}", partitionId, taskId);

    // Close and delete current file if any
    if (currentWriter != null) {
      try {
        currentWriter.close();
      } catch (Exception e) {
        LOG.warn("Failed to close writer during abort", e);
      }

      // Try to delete the partial file
      if (currentFilePath != null) {
        try {
          Path path = new Path(currentFilePath);
          FileSystem fs = path.getFileSystem(hadoopConf);
          fs.delete(path, false);
          LOG.info("Deleted partial file: {}", currentFilePath);
        } catch (Exception e) {
          LOG.warn("Failed to delete partial file: {}", currentFilePath, e);
        }
      }

      currentWriter = null;
      currentFilePath = null;
    }

    buffer.clear();
    writtenFiles.clear();
  }

  @Override
  public void close() throws IOException {
    // Cleanup resources
    if (currentWriter != null) {
      try {
        currentWriter.close();
      } catch (Exception e) {
        LOG.warn("Failed to close writer", e);
      }
      currentWriter = null;
    }

    buffer.clear();
    writtenFiles.clear();
  }
}
