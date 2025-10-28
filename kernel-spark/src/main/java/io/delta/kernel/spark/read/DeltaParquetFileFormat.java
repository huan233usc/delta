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
package io.delta.kernel.spark.read;

import io.delta.kernel.internal.deletionvectors.DeletionVectorUtils;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.internal.util.Tuple2;
import java.io.Serializable;
import java.util.*;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.collection.Iterator;

/**
 * A custom Parquet file format for Delta tables that applies Deletion Vectors to filter out deleted
 * rows.
 *
 * <p>This class extends Spark's {@link ParquetFileFormat} to add Delta-specific functionality for
 * handling Deletion Vectors. When a file has an associated deletion vector, this reader filters out
 * the deleted rows during the scan.
 *
 * <p>Design: - Wraps Spark's standard {@link ParquetFileFormat} for Parquet reading - Uses Delta
 * Kernel APIs to load and process Deletion Vectors - Supports both vectorized ({@link
 * org.apache.spark.sql.vectorized.ColumnarBatch}) and row-based ({@link InternalRow}) reading
 *
 * @see <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors">Delta
 *     Protocol - Deletion Vectors</a>
 */
public class DeltaParquetFileFormat extends ParquetFileFormat implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String tablePath;

  /**
   * Constructs a DeltaParquetFileFormat for a specific Delta table.
   *
   * @param tablePath the absolute path to the Delta table
   */
  public DeltaParquetFileFormat(String tablePath) {
    this.tablePath = tablePath;
  }

  @Override
  public Function1<PartitionedFile, Iterator<InternalRow>> buildReaderWithPartitionValues(
      org.apache.spark.sql.SparkSession sparkSession,
      StructType dataSchema,
      StructType partitionSchema,
      StructType requiredSchema,
      scala.collection.immutable.Seq<Filter> filters,
      scala.collection.immutable.Map<String, String> options,
      Configuration hadoopConf) {

    // Build the standard Parquet reader
    Function1<PartitionedFile, Iterator<InternalRow>> baseReader =
        super.buildReaderWithPartitionValues(
            sparkSession,
            dataSchema,
            partitionSchema,
            requiredSchema,
            filters,
            options,
            hadoopConf);

    // Wrap with deletion vector filtering if needed
    return (PartitionedFile file) -> {
      Iterator<InternalRow> dataIterator = baseReader.apply(file);
      return applyDeletionVectorIfNeeded(file, dataIterator, hadoopConf);
    };
  }

  /**
   * Applies deletion vector filtering if the file has an associated DV.
   *
   * @param file the Parquet file to read
   * @param dataIterator the iterator of rows from the Parquet reader
   * @param hadoopConf Hadoop configuration
   * @return an iterator that filters out deleted rows, or the original iterator if no DV
   */
  private <T> Iterator<InternalRow> applyDeletionVectorIfNeeded(
      PartitionedFile file, Iterator<T> dataIterator, Configuration hadoopConf) {

    // Check if this file has a deletion vector
    Optional<io.delta.kernel.internal.actions.DeletionVectorDescriptor> dvDescriptorOpt =
        extractDeletionVectorDescriptor(file);
    if (!dvDescriptorOpt.isPresent()) {
      // No deletion vector, return original iterator
      return (Iterator<InternalRow>) (Iterator<?>) dataIterator;
    }

    // Load deletion vector using Kernel API
    RoaringBitmapArray deletionVector = loadDeletionVector(dvDescriptorOpt.get(), hadoopConf);

    // Filter out deleted rows - handle both vectorized and row-based data
    // Parquet may return ColumnarBatch or InternalRow
    return new DeletionVectorFilterIterator<>(dataIterator, deletionVector);
  }

  /** Key used to pass deletion vector descriptor in PartitionedFile metadata. */
  public static final String DV_DESCRIPTOR_KEY = "deletionVectorDescriptor";

  /** Extract deletion vector descriptor from PartitionedFile metadata. */
  private Optional<io.delta.kernel.internal.actions.DeletionVectorDescriptor>
      extractDeletionVectorDescriptor(PartitionedFile file) {
    scala.collection.immutable.Map<String, Object> metadata =
        file.otherConstantMetadataColumnValues();

    scala.Option<Object> dvOption = metadata.get(DV_DESCRIPTOR_KEY);
    if (dvOption.isDefined()) {
      Object dvObj = dvOption.get();
      if (dvObj instanceof io.delta.kernel.internal.actions.DeletionVectorDescriptor) {
        return Optional.of((io.delta.kernel.internal.actions.DeletionVectorDescriptor) dvObj);
      }
    }
    return Optional.empty();
  }

  /**
   * Load deletion vector from storage using Delta Kernel APIs.
   *
   * @param dvDescriptor the deletion vector descriptor
   * @param hadoopConf Hadoop configuration for file system access
   * @return the loaded deletion vector bitmap
   */
  private RoaringBitmapArray loadDeletionVector(
      io.delta.kernel.internal.actions.DeletionVectorDescriptor dvDescriptor,
      Configuration hadoopConf) {
    try {
      // Create a new engine for this task
      io.delta.kernel.engine.Engine engine =
          io.delta.kernel.defaults.engine.DefaultEngine.create(hadoopConf);

      // Use Kernel API to load the deletion vector
      Tuple2<io.delta.kernel.internal.actions.DeletionVectorDescriptor, RoaringBitmapArray> result =
          DeletionVectorUtils.loadNewDvAndBitmap(engine, tablePath, dvDescriptor);

      return result._2;
    } catch (Exception e) {
      throw new RuntimeException("Failed to load deletion vector", e);
    }
  }

  /**
   * Iterator that filters out rows marked as deleted in the deletion vector. Supports both
   * vectorized (ColumnarBatch) and non-vectorized (InternalRow) data.
   *
   * @param <T> The type of elements in the underlying iterator (typically ColumnarBatch or
   *     InternalRow)
   */
  private static class DeletionVectorFilterIterator<T>
      extends scala.collection.AbstractIterator<InternalRow> {
    private final Iterator<T> underlying;
    private final RoaringBitmapArray deletionVector;
    private long currentRowIndex = 0;

    // For handling ColumnarBatch - use Scala Iterator
    private scala.collection.Iterator<InternalRow> currentBatchIterator = null;

    // Type handlers map for processing different data formats
    private final Map<Class<?>, Function<Object, InternalRow>> typeHandlers;

    DeletionVectorFilterIterator(Iterator<T> underlying, RoaringBitmapArray deletionVector) {
      this.underlying = underlying;
      this.deletionVector = deletionVector;

      // Initialize type handlers
      this.typeHandlers = new HashMap<>();
      typeHandlers.put(
          org.apache.spark.sql.vectorized.ColumnarBatch.class, this::handleColumnarBatch);
      typeHandlers.put(InternalRow.class, this::handleInternalRow);
    }

    @Override
    public boolean hasNext() {
      // First check if we have rows from current batch
      if (currentBatchIterator != null && currentBatchIterator.hasNext()) {
        return true;
      }

      // Try to get next batch or row
      return underlying.hasNext();
    }

    @Override
    public InternalRow next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      // If we have rows from current batch, return next one
      if (currentBatchIterator != null && currentBatchIterator.hasNext()) {
        return currentBatchIterator.next();
      }

      // Get next item from underlying iterator
      Object next = underlying.next();

      // Use type handlers map to process different data formats
      Function<Object, InternalRow> handler = typeHandlers.get(next.getClass());
      if (handler != null) {
        return handler.apply(next);
      } else {
        throw new RuntimeException(
            "Unexpected row type from Parquet reader: " + next.getClass().getName());
      }
    }

    /** Handle vectorized ColumnarBatch data */
    private InternalRow handleColumnarBatch(Object obj) {
      org.apache.spark.sql.vectorized.ColumnarBatch batch =
          (org.apache.spark.sql.vectorized.ColumnarBatch) obj;
      List<InternalRow> filteredRows = filterColumnarBatch(batch);
      // Convert Java Iterator to Scala Iterator
      currentBatchIterator =
          scala.collection.JavaConverters.asScalaIterator(filteredRows.iterator());
      return currentBatchIterator.next();
    }

    /** Handle non-vectorized InternalRow data */
    private InternalRow handleInternalRow(Object obj) {
      InternalRow row = (InternalRow) obj;
      // Filter out deleted rows
      while (deletionVector.contains(currentRowIndex)) {
        currentRowIndex++;
        if (!underlying.hasNext()) {
          throw new NoSuchElementException();
        }
        row = (InternalRow) underlying.next();
      }
      currentRowIndex++;
      return row;
    }

    /** Filter ColumnarBatch by deletion vector. Returns list of non-deleted rows. */
    private List<InternalRow> filterColumnarBatch(
        org.apache.spark.sql.vectorized.ColumnarBatch batch) {
      List<InternalRow> result = new ArrayList<>();
      int numRows = batch.numRows();

      for (int i = 0; i < numRows; i++) {
        if (!deletionVector.contains(currentRowIndex + i)) {
          result.add(batch.getRow(i).copy());
        }
      }

      currentRowIndex += numRows;
      return result;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DeltaParquetFileFormat that = (DeltaParquetFileFormat) o;
    return Objects.equals(tablePath, that.tablePath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tablePath);
  }
}
