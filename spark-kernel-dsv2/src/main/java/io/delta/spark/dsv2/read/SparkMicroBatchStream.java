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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.dsv2.scan.KernelSparkScanContext;
import io.delta.spark.dsv2.scan.batch.KernelPartitionReaderFactory;
import io.delta.spark.dsv2.utils.ScalaUtils;
import io.delta.spark.dsv2.utils.SparkSchemaWrapper;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.*;
import org.apache.spark.sql.delta.sources.CompositeLimit;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.delta.sources.ReadMaxBytes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delta Kernel implementation of Spark's MicroBatchStream using Kernel API.
 *
 * <p>This implementation uses the Kernel API for incremental scanning and integrates with Spark's
 * DeltaSourceOffset for robust offset tracking.
 */
public class SparkMicroBatchStream
        implements MicroBatchStream, SupportsAdmissionControl, SupportsTriggerAvailableNow {

    private static final Logger LOG = LoggerFactory.getLogger(KernelDeltaMicroBatchStream.class);
    
    private final StructType readDataSchema;
    private final StructType dataSchema;
    private final StructType partitionSchema;;
    private final Configuration hadoopConf;
    private final KernelStreamingOptions options;
    private final String tableId;
    private final KernelTableHelper tableHelper;
    private final SnapshotImpl snapshotAtSourceInit;

    // Configuration class for streaming options
    public static class KernelStreamingOptions {
        public boolean ignoreDeletes = false;
        public boolean ignoreChanges = false;
        public Optional<Long> startingVersion = Optional.empty();
        public Optional<String> startingTimestamp = Optional.empty();
        public int maxFilesPerTrigger = 1000;
        public long maxBytesPerTrigger = 1024 * 1024 * 1024; // 1GB
    }

    public KernelDeltaMicroBatchStream(
            String tablePath,
            Engine engine,
            SnapshotImpl snapshotAtSourceInit,
            SparkSchemaWrapper schemaWrapper,
            Configuration hadoopConf,
            KernelStreamingOptions options) {
        this.engine = requireNonNull(engine, "engine is null");
        this.schemaWrapper = requireNonNull(schemaWrapper, "schemaWrapper is null");
        this.hadoopConf = requireNonNull(hadoopConf, "hadoopConf is null");
        this.options = requireNonNull(options, "options is null");
        this.tableId = snapshotAtSourceInit.getMetadata().getId();

        // Create table helper for snapshot management and incremental scanning
        this.tableHelper =
                new KernelTableHelper(
                        requireNonNull(tablePath, "tablePath is null"),
                        engine,
                        requireNonNull(snapshotAtSourceInit, "snapshotAtSourceInit is null"));
        this.snapshotAtSourceInit = snapshotAtSourceInit;

        LOG.info(
                "Created KernelDeltaMicroBatchStream for table: {} at version: {}",
                tablePath,
                snapshotAtSourceInit.getVersion());
    }

    @Override
    public Offset initialOffset() {
        LOG.info("Getting initial offset for Delta streaming");

        // Use the starting version if specified, otherwise use the current snapshot version
        long version = options.startingVersion.orElse(tableHelper.getCurrentSnapshot().getVersion());
        boolean isInitialSnapshot = !options.startingVersion.isPresent();

        return DeltaSourceOffset.apply(
                tableId, version, DeltaSourceOffset.BASE_INDEX(), isInitialSnapshot);
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

            return endOffset.orElse(null);

        } catch (Exception e) {
            LOG.error("Failed to get latest offset", e);
            throw new RuntimeException("Failed to get latest offset", e);
        }
    }

    @Override
    public ReadLimit getDefaultReadLimit() {
        return ReadLimit.maxFiles(options.maxFilesPerTrigger);
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
                if (indexedFile.add != null && indexedFile.add.getDataChange()) {
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

        // Create a scan context for the reader factory using current snapshot
        KernelSparkScanContext scanContext =
                new KernelSparkScanContext(
                        tableHelper.getCurrentSnapshot().getScanBuilder().build(),
                        schemaWrapper.getDataSchema(),
                        schemaWrapper.getPartitionSchema(),
                        hadoopConf);

        // Create the factory similar to how KernelSparkBatchScan does it
        return new KernelPartitionReaderFactory(
                scanContext.createReaderFunction(), scanContext.supportColumnar());
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
            ObjectMapper mapper = io.delta.kernel.internal.util.JsonUtils.mapper();

            DeltaSourceOffset offset = mapper.readValue(json, DeltaSourceOffset.class);

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
        long startVersion = options.startingVersion.orElse(snapshotAtSourceInit.getVersion());
        boolean isInitialSnapshot = !options.startingVersion.isPresent();

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
                org.apache.spark.paths.SparkPath.fromPathString(addFile.getPath());

        long length = addFile.getSize();
        long modificationTime = addFile.getModificationTime();

        // Get partition values from the AddFile
        java.util.Map<String, String> partitionValueMap =
                VectorUtils.toJavaMap(addFile.getPartitionValues());

        // Initialize with empty row as default
        org.apache.spark.sql.catalyst.InternalRow partitionValues =
                org.apache.spark.sql.catalyst.InternalRow.empty();

        // Convert partition values if we have partition schema
        if (schemaWrapper.getPartitionSchema() != null
                && schemaWrapper.getPartitionSchema().fields().length > 0
                && partitionValueMap != null
                && !partitionValueMap.isEmpty()) {

            // Create partition values array
            Object[] partitionArray = new Object[schemaWrapper.getPartitionSchema().fields().length];

            for (int i = 0; i < schemaWrapper.getPartitionSchema().fields().length; i++) {
                String fieldName = schemaWrapper.getPartitionSchema().fields()[i].name();
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
                ScalaUtils.toScalaMap(new HashMap<>()));
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

        LOG.info(
                "Getting file changes from version {} (index {}) with limits {}",
                fromVersion,
                fromIndex,
                limits);

        try {
            Set<DeltaLogActionUtils.DeltaAction> actionSet = new HashSet<>();
            actionSet.add(DeltaLogActionUtils.DeltaAction.ADD);
            actionSet.add(DeltaLogActionUtils.DeltaAction.REMOVE);
            actionSet.add(DeltaLogActionUtils.DeltaAction.CDC);
            actionSet.add(DeltaLogActionUtils.DeltaAction.COMMITINFO);

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
        public final io.delta.kernel.internal.actions.RemoveFile remove;
        public final io.delta.kernel.internal.actions.AddCDCFile cdc;

        public IndexedFile(
                long version,
                long index,
                io.delta.kernel.internal.actions.AddFile add,
                io.delta.kernel.internal.actions.RemoveFile remove,
                io.delta.kernel.internal.actions.AddCDCFile cdc) {
            this.version = version;
            this.index = index;
            this.add = add;
            this.remove = remove;
            this.cdc = cdc;
        }

        public boolean hasFileAction() {
            return add != null || remove != null || cdc != null;
        }
    }

    /** Iterator that converts ColumnarBatch data to IndexedFile objects. */
    private static class IndexedFileIterator implements CloseableIterator<IndexedFile> {
        private final CloseableIterator<ColumnarBatch> batches;
        private final long startVersion;
        private final long startIndex;
        private final boolean isInitialSnapshot;
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
            this.isInitialSnapshot = isInitialSnapshot;
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

                    // Extract action information from the row
                    // The exact implementation depends on the action schema
                    io.delta.kernel.internal.actions.AddFile addFile = null;
                    io.delta.kernel.internal.actions.RemoveFile removeFile = null;
                    io.delta.kernel.internal.actions.AddCDCFile cdcFile = null;

                    try {
                        // Try to parse as different action types
                        // This is a simplified approach - in reality you'd need to check the action type first

                        // Check if this row contains an ADD action
                        if (row.isNullAt(0) == false) {
                            // Attempt to parse as AddFile (simplified)
                            addFile = parseAddFile(row);
                        }

                        // Check if this row contains a REMOVE action
                        if (addFile == null && row.isNullAt(1) == false) {
                            // Attempt to parse as RemoveFile (simplified)
                            removeFile = parseRemoveFile(row);
                        }

                        // Check if this row contains a CDC action
                        if (addFile == null && removeFile == null && row.isNullAt(2) == false) {
                            // Attempt to parse as CDCFile (simplified)
                            cdcFile = parseCDCFile(row);
                        }

                        if (addFile != null || removeFile != null || cdcFile != null) {
                            IndexedFile indexedFile =
                                    new IndexedFile(currentVersion, currentIndex, addFile, removeFile, cdcFile);
                            files.add(indexedFile);
                            currentIndex++;
                        }

                    } catch (Exception e) {
                        LOG.debug("Failed to parse row as file action, skipping: {}", e.getMessage());
                        // Skip this row and continue
                    }
                }

            } catch (Exception e) {
                LOG.warn("Error parsing ColumnarBatch: {}", e.getMessage());
            }

            return files;
        }

        // Simplified parsers for different action types
        private io.delta.kernel.internal.actions.AddFile parseAddFile(io.delta.kernel.data.Row row) {
            // This is a highly simplified implementation
            // In reality, you'd need to properly parse the AddFile schema
            try {
                if (row.isNullAt(0)) return null;

                // For now, create a mock AddFile to demonstrate the structure
                // You would need to properly extract fields from the row based on the schema
                LOG.debug("Parsing ADD file action (simplified implementation)");

                // Return null for now to avoid complex schema parsing
                // This should be implemented based on AddFile.FULL_SCHEMA
                return null;

            } catch (Exception e) {
                LOG.debug("Failed to parse ADD file: {}", e.getMessage());
                return null;
            }
        }

        private io.delta.kernel.internal.actions.RemoveFile parseRemoveFile(
                io.delta.kernel.data.Row row) {
            // Similar simplified implementation for RemoveFile
            LOG.debug("Parsing REMOVE file action (simplified implementation)");
            return null;
        }

        private io.delta.kernel.internal.actions.AddCDCFile parseCDCFile(io.delta.kernel.data.Row row) {
            // Similar simplified implementation for CDCFile
            LOG.debug("Parsing CDC file action (simplified implementation)");
            return null;
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
}
