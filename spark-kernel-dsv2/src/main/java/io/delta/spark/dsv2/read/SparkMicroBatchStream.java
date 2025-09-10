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
package io.delta.spark.dsv2.read;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.utils.CloseableIterator;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.*;
import org.apache.spark.sql.delta.sources.CompositeLimit;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.delta.sources.ReadMaxBytes;
import org.apache.spark.sql.delta.util.JsonUtils;
import org.apache.spark.sql.execution.datasources.FileFormat$;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.collection.JavaConverters;

/**
 * Delta Kernel implementation of Spark's MicroBatchStream using Kernel API.
 *
 * <p>This implementation uses the Kernel API for incremental scanning and integrates with Spark's
 * DeltaSourceOffset for robust offset tracking.
 */
public class SparkMicroBatchStream
    implements MicroBatchStream, SupportsAdmissionControl, SupportsTriggerAvailableNow {

  private static final Logger LOG = LoggerFactory.getLogger(SparkMicroBatchStream.class);
  private final SQLConf sqlConf = SQLConf.get();
  private final StructType readDataSchema;
  private final StructType dataSchema;
  private final StructType partitionSchema;;
  private final Configuration hadoopConf;
  private final String tableId;
  private final StreamingHelper tableHelper;
  private final SnapshotImpl snapshotAtSourceInit;
  private final Filter[] dataFilters;

  public SparkMicroBatchStream(
      StructType readDataSchema,
      StructType dataSchema,
      StructType partitionSchema,
      Engine engine,
      SnapshotImpl snapshotAtSourceInit,
      Filter[] dataFilters,
      Configuration hadoopConf) {
    this.hadoopConf = requireNonNull(hadoopConf, "hadoopConf is null");
    this.tableId = snapshotAtSourceInit.getMetadata().getId();

    // Create table helper for snapshot management and incremental scanning
    this.tableHelper =
        new StreamingHelper(
            snapshotAtSourceInit.getPath(),
            engine,
            requireNonNull(snapshotAtSourceInit, "snapshotAtSourceInit is null"));
    this.snapshotAtSourceInit = snapshotAtSourceInit;
    this.readDataSchema = readDataSchema;
    this.partitionSchema = partitionSchema;
    this.dataSchema = dataSchema;
    this.dataFilters = dataFilters;
  }

  @Override
  public Offset initialOffset() {
    LOG.info("Getting initial offset for Delta streaming");
    // Use the starting version if specified, otherwise use the current snapshot version
    long version = tableHelper.getCurrentSnapshot().getVersion();
    return DeltaSourceOffset.apply(tableId, 0, DeltaSourceOffset.BASE_INDEX(), true);
  }

  /**
   * This method should NOT be called when SupportsAdmissionControl is implemented. Spark will call
   * latestOffset(Offset, ReadLimit) instead.
   */
  @Override
  public Offset latestOffset() {
    LOG.warn(
        "latestOffset() called when SupportsAdmissionControl is implemented. "
            + "This should not happen in normal operation.");
    throw new UnsupportedOperationException(
        "latestOffset() should not be called when SupportsAdmissionControl is implemented. "
            + "Use latestOffset(Offset, ReadLimit) instead.");
  }

  @Override
  public Offset latestOffset(Offset startOffset, ReadLimit limit) {
    LOG.info(
        "Getting latest offset with admission control from {} with limit {}", startOffset, limit);
    try {
      DeltaSourceOffset start = null;
      if (startOffset != null) {
        start = DeltaSourceOffset.apply(tableId, startOffset);
      }

      AdmissionLimits limits = AdmissionLimits.from(limit);
      Optional<DeltaSourceOffset> endOffset =
          start == null
              ? getStartingOffset(limits)
              : getNextOffsetFromPreviousOffset(start, limits);
      System.out.println(endOffset);

      return endOffset.orElse(null);

    } catch (Exception e) {
      LOG.error("Failed to get latest offset", e);
      throw new RuntimeException("Failed to get latest offset", e);
    }
  }

  @Override
  public InputPartition[] planInputPartitions(Offset start, Offset end) {
    LOG.info("Planning input partitions from {} to {}", start, end);

    DeltaSourceOffset startOffset = DeltaSourceOffset.apply(tableId, start);
    DeltaSourceOffset endOffset = DeltaSourceOffset.apply(tableId, end);

    if (endOffset.reservoirVersion() < startOffset.reservoirVersion()
        || (endOffset.reservoirVersion() == startOffset.reservoirVersion()
            && endOffset.index() <= startOffset.index())) {
      LOG.info("No new data available");
      return new InputPartition[0];
    }

    // Get file changes using the table helper
    try (CloseableIterator<IndexedFile> fileChanges =
        getFileChanges(
            startOffset.reservoirVersion(),
            startOffset.index(),
            startOffset.isInitialSnapshot(),
            Optional.of(endOffset))) {

      java.util.List<IndexedFile> addFiles = new java.util.ArrayList<>();

      while (fileChanges.hasNext()) {
        IndexedFile indexedFile = fileChanges.next();
        // Only include ADD files for actual data reading
        if ((indexedFile.version > startOffset.reservoirVersion()
                || (indexedFile.version == startOffset.reservoirVersion()
                    && indexedFile.index > startOffset.index()))
            && indexedFile.add != null
            && indexedFile.add.getDataChange()) {
          addFiles.add(indexedFile);
        }
      }

      LOG.info("Found {} ADD files to read", addFiles.size());

      if (addFiles.isEmpty()) {
        return new InputPartition[0];
      }

      // Convert AddFiles to PartitionedFiles and create FilePartitions
      java.util.List<InputPartition> partitions = new java.util.ArrayList<>();

      for (int i = 0; i < addFiles.size(); i++) {
        IndexedFile indexedFile = addFiles.get(i);
        io.delta.kernel.internal.actions.AddFile addFile = indexedFile.add;

        // Create PartitionedFile from AddFile
        org.apache.spark.sql.execution.datasources.PartitionedFile partitionedFile =
            createPartitionedFileFromAddFile(addFile);

        // Create FilePartition with single file
        partitions.add(
            new org.apache.spark.sql.execution.datasources.FilePartition(
                i,
                new org.apache.spark.sql.execution.datasources.PartitionedFile[] {
                  partitionedFile
                }));
      }

      return partitions.toArray(new InputPartition[0]);

    } catch (Exception e) {
      LOG.error("Failed to plan input partitions", e);
      throw new RuntimeException("Failed to plan input partitions", e);
    }
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    LOG.info("Creating reader factory for Delta streaming");

    boolean enableVectorizedReader =
        ParquetUtils.isBatchReadSupportedForSchema(sqlConf, readDataSchema);

    scala.collection.immutable.Map<String, String> options =
        scala.collection.immutable.Map$.MODULE$
            .<String, String>empty()
            .updated(
                FileFormat$.MODULE$.OPTION_RETURNING_BATCH(),
                String.valueOf(enableVectorizedReader));
    Function1<PartitionedFile, scala.collection.Iterator<InternalRow>> readFunc =
        new ParquetFileFormat()
            .buildReaderWithPartitionValues(
                SparkSession.active(),
                dataSchema,
                partitionSchema,
                readDataSchema,
                JavaConverters.asScalaBuffer(Arrays.asList(dataFilters)).toSeq(),
                options,
                hadoopConf);

    return new SparkReaderFactory(readFunc, enableVectorizedReader);
  }

  @Override
  public void commit(Offset end) {
    LOG.info("Committing offset: {}", end);
    // No special commit logic needed for now
  }

  @Override
  public void stop() {
    LOG.info("Stopping Delta streaming source");
    // No cleanup needed for now
  }

  @Override
  public Offset deserializeOffset(String json) {
    LOG.debug("Deserializing offset from JSON: {}", json);
    try {
      // Use Jackson to deserialize the JSON directly

      DeltaSourceOffset offset = JsonUtils.mapper().readValue(json, DeltaSourceOffset.class);

      // Validate that the table ID matches
      if (!tableId.equals(offset.reservoirId())) {
        throw new IllegalArgumentException(
            "Deserialized offset has different table ID. Expected: "
                + tableId
                + ", Got: "
                + offset.reservoirId());
      }

      return offset;

    } catch (Exception e) {
      LOG.error("Failed to deserialize offset from JSON: {}", json, e);
      throw new RuntimeException("Failed to deserialize offset", e);
    }
  }

  // SupportsAdmissionControl implementation
  private Optional<DeltaSourceOffset> getStartingOffset(AdmissionLimits limits) {
    long startVersion = snapshotAtSourceInit.getVersion();
    boolean isInitialSnapshot = true;

    if (startVersion < 0) {
      return Optional.empty();
    }
    return computeEndOffsetFromVersion(startVersion, isInitialSnapshot, limits);
  }

  private Optional<DeltaSourceOffset> getNextOffsetFromPreviousOffset(
      DeltaSourceOffset previous, AdmissionLimits limits) {
    try (CloseableIterator<IndexedFile> changes =
        getFileChangesWithRateLimit(
            previous.reservoirVersion(),
            previous.index(),
            tableHelper.getLatestVersion(),
            previous.isInitialSnapshot(),
            limits)) {

      IndexedFile lastFile = null;
      while (changes.hasNext()) {
        lastFile = changes.next();
      }

      if (lastFile == null) {
        // No new changes
        return Optional.of(previous);
      } else {
        return buildOffsetFromIndexedFile(
            lastFile, previous.reservoirVersion(), previous.isInitialSnapshot());
      }

    } catch (Exception e) {
      LOG.error("Failed to get next offset from previous offset: {}", previous, e);
      throw new RuntimeException("Failed to get incremental changes", e);
    }
  }

  private Optional<DeltaSourceOffset> computeEndOffsetFromVersion(
      long fromVersion, boolean isInitialSnapshot, AdmissionLimits limits) {
    try (CloseableIterator<IndexedFile> changes =
        getFileChangesWithRateLimit(
            fromVersion,
            DeltaSourceOffset.BASE_INDEX(),
            tableHelper.getLatestVersion(),
            isInitialSnapshot,
            limits)) {

      IndexedFile lastFile = null;
      while (changes.hasNext()) {
        lastFile = changes.next();
      }

      if (lastFile == null) {
        // No changes in this version range
        return Optional.empty();
      } else {
        return buildOffsetFromIndexedFile(lastFile, fromVersion, isInitialSnapshot);
      }

    } catch (Exception e) {
      LOG.error("Failed to compute end offset from version: {}", fromVersion, e);
      throw new RuntimeException("Failed to compute end offset", e);
    }
  }

  // SupportsTriggerAvailableNow implementation
  private Optional<DeltaSourceOffset> lastOffsetForAvailableNow = Optional.empty();
  private boolean availableNowInited = false;

  @Override
  public void prepareForTriggerAvailableNow() {
    availableNowInited = true;
    LOG.info("Prepared for Trigger.AvailableNow mode");
  }

  // Helper class for parsing ReadLimit
  private static class AdmissionLimits {
    private final Optional<Integer> maxFiles;
    private final Optional<Long> maxBytes;

    private AdmissionLimits(Optional<Integer> maxFiles, Optional<Long> maxBytes) {
      this.maxFiles = maxFiles;
      this.maxBytes = maxBytes;
    }

    public static AdmissionLimits from(ReadLimit limit) {
      if (limit instanceof ReadAllAvailable) {
        return new AdmissionLimits(Optional.empty(), Optional.empty());
      } else if (limit instanceof ReadMaxFiles) {
        ReadMaxFiles maxFiles = (ReadMaxFiles) limit;
        return new AdmissionLimits(Optional.of(maxFiles.maxFiles()), Optional.empty());
      } else if (limit instanceof ReadMaxBytes) {
        ReadMaxBytes maxBytes = (ReadMaxBytes) limit;
        return new AdmissionLimits(Optional.empty(), Optional.of(maxBytes.maxBytes()));
      } else if (limit instanceof CompositeLimit) {
        CompositeLimit composite = (CompositeLimit) limit;
        return new AdmissionLimits(
            Optional.of(composite.maxFiles().maxFiles()),
            Optional.of(composite.bytes().maxBytes()));
      } else {
        LOG.warn("Unknown ReadLimit type: {}", limit.getClass());
        return new AdmissionLimits(Optional.empty(), Optional.empty());
      }
    }

    public Optional<Integer> maxFiles() {
      return maxFiles;
    }

    public Optional<Long> maxBytes() {
      return maxBytes;
    }

    @Override
    public String toString() {
      return "AdmissionLimits{maxFiles=" + maxFiles + ", maxBytes=" + maxBytes + "}";
    }
  }

  // Helper method to convert AddFile to PartitionedFile
  private org.apache.spark.sql.execution.datasources.PartitionedFile
      createPartitionedFileFromAddFile(io.delta.kernel.internal.actions.AddFile addFile) {

    // Create file path
    org.apache.spark.paths.SparkPath filePath =
        org.apache.spark.paths.SparkPath.fromPathString(
            snapshotAtSourceInit.getPath() + "/" + addFile.getPath());

    long length = addFile.getSize();
    long modificationTime = addFile.getModificationTime();

    // Get partition values from the AddFile
    java.util.Map<String, String> partitionValueMap =
        VectorUtils.toJavaMap(addFile.getPartitionValues());

    // Initialize with empty row as default
    org.apache.spark.sql.catalyst.InternalRow partitionValues =
        org.apache.spark.sql.catalyst.InternalRow.empty();

    // Convert partition values if we have partition schema
    if (partitionSchema != null
        && partitionSchema.fields().length > 0
        && partitionValueMap != null
        && !partitionValueMap.isEmpty()) {

      // Create partition values array
      Object[] partitionArray = new Object[partitionSchema.fields().length];

      for (int i = 0; i < partitionSchema.fields().length; i++) {
        String fieldName = partitionSchema.fields()[i].name();
        String value = partitionValueMap.get(fieldName);

        if (value != null) {
          // For now, store as string - a full implementation would need proper type conversion
          partitionArray[i] = org.apache.spark.unsafe.types.UTF8String.fromString(value);
        } else {
          partitionArray[i] = null;
        }
      }

      partitionValues =
          org.apache.spark.sql.catalyst.InternalRow.fromSeq(
              scala.collection.JavaConverters.asScalaBuffer(java.util.Arrays.asList(partitionArray))
                  .toSeq());
    }

    return new org.apache.spark.sql.execution.datasources.PartitionedFile(
        partitionValues, // Partition values
        filePath, // File path
        0, // Start offset
        length, // File length
        new String[0], // Host locations
        modificationTime, // Last modified time
        length, // File size
        toScalaMap(new HashMap<>()));
  }

  // Helper methods for file changes and offset management

  /** Gets file changes without rate limits (for partition planning). */
  private CloseableIterator<IndexedFile> getFileChanges(
      long fromVersion,
      long fromIndex,
      boolean isInitialSnapshot,
      Optional<DeltaSourceOffset> endOffset) {

    long endVersion =
        endOffset
            .map(DeltaSourceOffset::reservoirVersion)
            .orElseGet(
                () -> {
                  try {
                    return tableHelper.getLatestVersion();
                  } catch (TableNotFoundException e) {
                    throw new RuntimeException("Failed to get latest version", e);
                  }
                });

    return getFileChangesWithRateLimit(
        fromVersion,
        fromIndex,
        endVersion,
        isInitialSnapshot,
        new AdmissionLimits(Optional.empty(), Optional.empty()));
  }

  /** Gets file changes with rate limiting applied. */
  private CloseableIterator<IndexedFile> getFileChangesWithRateLimit(
      long fromVersion,
      long fromIndex,
      long endVersion,
      boolean isInitialSnapshot,
      AdmissionLimits limits) {
    // TODO: initial snapshot should return all the add files.
    LOG.info(
        "Getting file changes from version {} (index {}) with limits {}",
        fromVersion,
        fromIndex,
        limits);

    try {
      Set<DeltaLogActionUtils.DeltaAction> actionSet = new HashSet<>();
      actionSet.add(DeltaLogActionUtils.DeltaAction.ADD);

      CloseableIterator<ColumnarBatch> changes =
          tableHelper.getIncrementalChanges(fromVersion, Optional.of(endVersion), actionSet);

      return new IndexedFileIterator(changes, fromVersion, fromIndex, isInitialSnapshot, limits);

    } catch (Exception e) {
      LOG.error("Failed to get file changes from version {} to latest", fromVersion, e);
      throw new RuntimeException("Failed to get file changes", e);
    }
  }

  /** Builds a DeltaSourceOffset from an IndexedFile. */
  private Optional<DeltaSourceOffset> buildOffsetFromIndexedFile(
      IndexedFile file, long prevVersion, boolean wasInitial) {

    long version = file.version;
    long index = file.index;

    if (index == DeltaSourceOffset.END_INDEX()) {
      // End of version, move to next version
      return Optional.of(
          DeltaSourceOffset.apply(tableId, version + 1, DeltaSourceOffset.BASE_INDEX(), false));
    } else {
      boolean isInitialSnapshot = (version == prevVersion) && wasInitial;
      return Optional.of(DeltaSourceOffset.apply(tableId, version, index, isInitialSnapshot));
    }
  }

  /** Internal representation of a file action with version and index information. */
  private static class IndexedFile {
    public final long version;
    public final long index;
    public final io.delta.kernel.internal.actions.AddFile add;

    public IndexedFile(long version, long index, io.delta.kernel.internal.actions.AddFile add) {
      this.version = version;
      this.index = index;
      this.add = add;
    }

    public boolean hasFileAction() {
      return add != null;
    }
  }

  /** Iterator that converts ColumnarBatch data to IndexedFile objects. */
  private static class IndexedFileIterator implements CloseableIterator<IndexedFile> {
    private final CloseableIterator<ColumnarBatch> batches;
    private final long startVersion;
    private final long startIndex;
    private final AdmissionLimits limits;

    private java.util.Iterator<IndexedFile> currentBatchIterator;
    private int fileCount = 0;
    private long bytesRead = 0;

    public IndexedFileIterator(
        CloseableIterator<ColumnarBatch> batches,
        long startVersion,
        long startIndex,
        boolean isInitialSnapshot,
        AdmissionLimits limits) {
      this.batches = batches;
      this.startVersion = startVersion;
      this.startIndex = startIndex;
      this.limits = limits;
    }

    @Override
    public boolean hasNext() {
      // Check rate limits
      if (limits.maxFiles().isPresent() && fileCount >= limits.maxFiles().get()) {
        return false;
      }
      if (limits.maxBytes().isPresent() && bytesRead >= limits.maxBytes().get()) {
        return false;
      }

      if (currentBatchIterator != null && currentBatchIterator.hasNext()) {
        return true;
      }

      return loadNextBatch();
    }

    @Override
    public IndexedFile next() {
      if (!hasNext()) {
        throw new java.util.NoSuchElementException();
      }

      IndexedFile file = currentBatchIterator.next();
      fileCount++;

      // Estimate bytes (simplified)
      if (file.add != null) {
        bytesRead += file.add.getSize();
      }

      return file;
    }

    private boolean loadNextBatch() {
      while (batches.hasNext()) {
        ColumnarBatch batch = batches.next();
        java.util.List<IndexedFile> files = convertBatchToIndexedFiles(batch);
        if (!files.isEmpty()) {
          currentBatchIterator = files.iterator();
          return true;
        }
      }
      return false;
    }

    private java.util.List<IndexedFile> convertBatchToIndexedFiles(ColumnarBatch batch) {
      java.util.List<IndexedFile> files = new java.util.ArrayList<>();

      try (CloseableIterator<io.delta.kernel.data.Row> rows = batch.getRows()) {
        long currentVersion = startVersion;
        long currentIndex = startIndex;

        while (rows.hasNext()) {
          io.delta.kernel.data.Row row = rows.next();
          Tuple2<Long, io.delta.kernel.internal.actions.AddFile> addFile = null;
          try {
            if (!row.isNullAt(2)) {
              addFile = parseAddFile(row);
              if (addFile._1 > currentVersion) {
                currentVersion = addFile._1;
                currentIndex = 0;
              }
            }
            if (addFile != null) {
              IndexedFile indexedFile = new IndexedFile(addFile._1, currentIndex, addFile._2);
              files.add(indexedFile);
              currentIndex++;
            }

          } catch (Exception e) {
            LOG.debug("Failed to parse row as file action, skipping: {}", e.getMessage());
          }
        }

      } catch (Exception e) {
        LOG.warn("Error parsing ColumnarBatch: {}", e.getMessage());
      }

      return files;
    }

    // Simplified parsers for different action types
    private Tuple2<Long, io.delta.kernel.internal.actions.AddFile> parseAddFile(
        io.delta.kernel.data.Row row) {
      return new Tuple2<>(row.getLong(0), new AddFile(row.getStruct(2)));
    }

    @Override
    public void close() {
      try {
        batches.close();
      } catch (Exception e) {
        LOG.warn("Error closing batches iterator", e);
      }
    }
  }

  public static <K, V> scala.collection.immutable.Map<K, V> toScalaMap(
      java.util.Map<K, V> javaMap) {
    scala.collection.immutable.Map<K, V> result = new scala.collection.immutable.HashMap<>();
    for (java.util.Map.Entry<K, V> entry : javaMap.entrySet()) {
      result = result.updated(entry.getKey(), entry.getValue());
    }
    return result;
  }
}
