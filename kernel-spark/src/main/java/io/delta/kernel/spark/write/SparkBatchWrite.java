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

import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.spark.read.SparkScan;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of Spark's {@link BatchWrite} for Delta tables using Kernel.
 *
 * <p>This class orchestrates the batch write process:
 *
 * <ol>
 *   <li>Provides DataWriterFactory for executors to write data files
 *   <li>Collects AddFile actions from all executor tasks
 *   <li>For COW operations (UPDATE/MERGE): generates RemoveFile actions for replaced files
 *   <li>Commits the transaction with all generated actions (RemoveFiles + AddFiles)
 * </ol>
 */
public class SparkBatchWrite implements BatchWrite {
  private static final Logger LOG = LoggerFactory.getLogger(SparkBatchWrite.class);

  private final Transaction transaction;
  private final Engine engine;
  private final SerializableConfiguration hadoopConf;
  private final StructType sparkSchema;
  private final String queryId;
  private final SparkScan scanForCOW; // null for regular writes, non-null for COW operations

  public SparkBatchWrite(
      Transaction transaction,
      Engine engine,
      SerializableConfiguration hadoopConf,
      StructType sparkSchema,
      String queryId) {
    this(transaction, engine, hadoopConf, sparkSchema, queryId, null);
  }

  public SparkBatchWrite(
      Transaction transaction,
      Engine engine,
      SerializableConfiguration hadoopConf,
      StructType sparkSchema,
      String queryId,
      SparkScan scanForCOW) {
    this.transaction = transaction;
    this.engine = engine;
    this.hadoopConf = hadoopConf;
    this.sparkSchema = sparkSchema;
    this.queryId = queryId;
    this.scanForCOW = scanForCOW;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    LOG.info("Creating DataWriterFactory for query: {}", queryId);

    // Extract and wrap transactionState for serialization
    io.delta.kernel.data.Row transactionState = transaction.getTransactionState(engine);
    io.delta.kernel.spark.utils.SerializableKernelRowWrapper transactionStateWrapper =
        new io.delta.kernel.spark.utils.SerializableKernelRowWrapper(transactionState);

    // Prepare OutputWriterFactory on driver (requires SparkSession)
    // This is similar to how SparkBatch.createReaderFactory() works
    org.apache.hadoop.mapreduce.Job job;
    try {
      job = org.apache.hadoop.mapreduce.Job.getInstance(hadoopConf.value());
      job.setOutputKeyClass(Void.class);
      job.setOutputValueClass(org.apache.spark.sql.catalyst.InternalRow.class);
    } catch (java.io.IOException e) {
      throw new RuntimeException("Failed to create Job instance", e);
    }

    // Use empty options map (Scala immutable Map)
    scala.collection.immutable.Map<String, String> options =
        scala.collection.immutable.Map$.MODULE$.empty();

    // Prepare the OutputWriterFactory on driver
    org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat parquetFormat =
        new org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat();
    org.apache.spark.sql.execution.datasources.OutputWriterFactory writerFactory =
        parquetFormat.prepareWrite(
            org.apache.spark.sql.SparkSession.active(), job, options, sparkSchema);

    // Wrap the job configuration (which includes Parquet settings) for serialization
    SerializableConfiguration jobConf = new SerializableConfiguration(job.getConfiguration());

    return new SparkDataWriterFactory(
        transactionStateWrapper, jobConf, writerFactory, sparkSchema, queryId);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    LOG.info(
        "Committing Delta transaction for query: {} with {} messages", queryId, messages.length);

    try {
      // Collect all AddFile actions from executor tasks
      List<Row> addFileActions = new ArrayList<>();
      for (WriterCommitMessage message : messages) {
        SparkWriterCommitMessage msg = (SparkWriterCommitMessage) message;
        // Each message may contain multiple actions (multiple files written per task)
        addFileActions.addAll(Arrays.asList(msg.getActions()));
      }

      LOG.info("Generated {} AddFile actions", addFileActions.size());

      // For COW operations (UPDATE/MERGE): generate RemoveFile actions for replaced files
      List<Row> allActions = new ArrayList<>();
      if (scanForCOW != null) {
        LOG.info("COW operation detected - generating RemoveFile actions for replaced files");

        // Get the saved AddFiles from SparkScan (already scanned during read phase)
        // IMPORTANT: Do NOT call kernelScan.getScanFiles() again - it can only be called once!
        List<Row> addFilesToDelete = scanForCOW.getScannedAddFiles();

        LOG.info("Found {} AddFiles to delete (from saved scan)", addFilesToDelete.size());

        // Generate RemoveFile actions
        CloseableIterator<Row> removeFileActions =
            Transaction.generateDeleteActions(
                engine, toCloseableIterator(addFilesToDelete.iterator()));

        // Collect RemoveFile actions
        int removeFileCount = 0;
        while (removeFileActions.hasNext()) {
          allActions.add(removeFileActions.next());
          removeFileCount++;
        }

        LOG.info("Generated {} RemoveFile actions", removeFileCount);
      }

      // Add the new AddFile actions
      allActions.addAll(addFileActions);

      LOG.info(
          "Committing {} total actions ({} remove, {} add)",
          allActions.size(),
          allActions.size() - addFileActions.size(),
          addFileActions.size());

      // Create a CloseableIterable from the actions list
      CloseableIterable<Row> dataActions =
          CloseableIterable.inMemoryIterable(toCloseableIterator(allActions.iterator()));

      // Commit the transaction
      transaction.commit(engine, dataActions);

      LOG.info("Successfully committed Delta transaction for query: {}", queryId);
    } catch (Exception e) {
      LOG.error("Failed to commit Delta transaction for query: {}", queryId, e);
      throw new RuntimeException("Failed to commit Delta transaction", e);
    }
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    LOG.warn("Aborting Delta transaction for query: {}", queryId);
    // Kernel doesn't expose an explicit abort API
    // The transaction will simply not be committed
    // TODO: Consider cleaning up any data files that were written
  }
}
