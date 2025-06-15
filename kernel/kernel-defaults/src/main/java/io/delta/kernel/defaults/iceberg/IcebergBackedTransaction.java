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
package io.delta.kernel.defaults.iceberg;

import static io.delta.kernel.internal.actions.SingleAction.*;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.ConcurrentWriteException;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Format;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.iceberg.*;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;

public class IcebergBackedTransaction implements Transaction {

  private final org.apache.iceberg.Table icebergTable;
  private final String engineInfo;
  private final io.delta.kernel.Operation operation;
  private final TableMetadata initialTableMetadata;
  private final long readVersion;
  private final int maxRetries;
  private final String tablePath;
  private final FileIO fileIO;

  // Transaction state
  private final AtomicBoolean committed = new AtomicBoolean(false);
  private final UUID transactionId = UUID.randomUUID();

  // Builder-provided configuration that will be applied during commit
  private final TransactionConfiguration config;

  // Pending operations (will be calculated during commit)
  private org.apache.iceberg.Schema pendingSchema;
  private PartitionSpec pendingPartitionSpec;
  private final Map<String, String> pendingProperties = new HashMap<>();
  private final Set<String> propertiesToRemove = new HashSet<>();

  // Action tracking for proper Iceberg operation selection
  private final List<DataFile> filesToAdd = new ArrayList<>();
  private final List<DataFile> filesToRemove = new ArrayList<>();
  private boolean hasDataChange = false;

  public IcebergBackedTransaction(
      org.apache.iceberg.Table icebergTable,
      String engineInfo,
      io.delta.kernel.Operation operation,
      TableMetadata initialTableMetadata,
      int maxRetries,
      TransactionConfiguration config) {

    this.icebergTable = requireNonNull(icebergTable, "icebergTable cannot be null");
    this.engineInfo = requireNonNull(engineInfo, "engineInfo cannot be null");
    this.operation = requireNonNull(operation, "operation cannot be null");
    this.initialTableMetadata =
        requireNonNull(initialTableMetadata, "initialTableMetadata cannot be null");
    this.maxRetries = maxRetries;
    this.config = requireNonNull(config, "config cannot be null");
    this.tablePath = icebergTable.location();
    this.fileIO = icebergTable.io();

    // Calculate read version from Iceberg snapshot
    Snapshot currentSnapshot = initialTableMetadata.currentSnapshot();
    this.readVersion = currentSnapshot != null ? currentSnapshot.sequenceNumber() - 1 : -1;

    // Initialize pending state with current table state
    this.pendingSchema = initialTableMetadata.schema();
    this.pendingPartitionSpec = initialTableMetadata.spec();
  }

  @Override
  public StructType getSchema(Engine engine) {
    // Return the configured schema if available, otherwise current schema
    if (config.getSchema().isPresent()) {
      return config.getSchema().get();
    }
    return IcebergToDeltaSchemaConverter.toKernelSchema(new Schema(pendingSchema.columns()));
  }

  @Override
  public List<String> getPartitionColumns(Engine engine) {
    // Return configured partition columns if available, otherwise current partition columns
    if (config.getPartitionColumns().isPresent()) {
      return config.getPartitionColumns().get();
    }
    return pendingPartitionSpec.fields().stream()
        .map(PartitionField::name)
        .collect(Collectors.toList());
  }

  @Override
  public long getReadTableVersion() {
    return readVersion;
  }

  @Override
  public Row getTransactionState(Engine engine) {
    // Create a Delta Metadata object from current state
    Metadata metadata = createMetadataFromIcebergTable();

    // Use Delta's TransactionStateRow to create the proper transaction state
    return TransactionStateRow.of(metadata, tablePath, maxRetries);
  }

  /**
   * Creates a Delta Metadata object from the current Iceberg table state. This mirrors the approach
   * used in IcebergBackedScan.
   */
  private Metadata createMetadataFromIcebergTable() {
    // Generate a stable ID based on table location
    String tableId = UUID.nameUUIDFromBytes(tablePath.getBytes()).toString();

    // Get the current schema to use (configured schema takes precedence)
    StructType currentSchema = getSchema(null); // engine not needed for this call

    // Create partition columns array
    List<String> partitionColumnNames = getPartitionColumns(null); // engine not needed
    ArrayValue partitionColumns =
        VectorUtils.buildArrayValue(partitionColumnNames, StringType.STRING);

    // Use the existing Format class - defaults to "parquet" provider with empty options
    Format format = new Format();

    // Create configuration map from Iceberg table properties plus pending changes
    Map<String, String> configurationMap = new HashMap<>(icebergTable.properties());

    // Add any pending properties
    configurationMap.putAll(pendingProperties);

    // Remove any properties marked for removal
    for (String key : propertiesToRemove) {
      configurationMap.remove(key);
    }

    MapValue configurationMapValue = VectorUtils.stringStringMapValue(configurationMap);

    // Get current timestamp for creation time
    long currentTime = System.currentTimeMillis();

    return new Metadata(
        tableId,
        Optional.empty(), // name
        Optional.empty(), // description
        format,
        currentSchema.toJson(), // schema string
        currentSchema, // parsed schema
        partitionColumns,
        Optional.of(currentTime), // creation time
        configurationMapValue);
  }

  @Override
  public void addDomainMetadata(String domain, String config) {
    throw new UnsupportedOperationException(
        "Domain metadata is not supported in Iceberg-backed transactions");
  }

  @Override
  public void removeDomainMetadata(String domain) {
    throw new UnsupportedOperationException(
        "Domain metadata is not supported in Iceberg-backed transactions");
  }

  @Override
  public TransactionCommitResult commit(Engine engine, CloseableIterable<Row> dataActions)
      throws ConcurrentWriteException {

    if (!committed.compareAndSet(false, true)) {
      throw new IllegalStateException("Transaction has already been committed");
    }

    try {
      return commitWithRetry(engine, dataActions);
    } catch (CommitFailedException e) {
      throw new ConcurrentWriteException("Concurrent write detected: " + e.getMessage());
    }
  }

  private TransactionCommitResult commitWithRetry(Engine engine, CloseableIterable<Row> dataActions)
      throws ConcurrentWriteException {
    long commitVersion = performCommit(engine, dataActions);
    return new TransactionCommitResult(commitVersion, new ArrayList<>(), null);
  }

  private long performCommit(Engine engine, CloseableIterable<Row> dataActions)
      throws CommitFailedException {

    // Apply configuration changes before creating the Iceberg transaction
    applyConfigurationChanges();

    // Create Iceberg transaction
    org.apache.iceberg.BaseTransaction icebergTxn = (BaseTransaction) icebergTable.newTransaction();

    try {
      // Apply schema changes if any
      if (!pendingSchema.sameSchema(initialTableMetadata.schema())) {
        throw new UnsupportedOperationException("todo: support schema evolution");
      }

      // Apply partition spec changes if any
      if (!pendingPartitionSpec.equals(initialTableMetadata.spec())) {
        throw new UnsupportedOperationException("todo: support partition evolution");
      }

      // Apply property changes
      applyPropertyChanges(icebergTxn);

      // Process data actions and determine the appropriate Iceberg operation
      processDataActions(engine, icebergTxn, dataActions);

      // Commit the transaction
      icebergTxn.commitTransaction();

      icebergTable.refresh();
      System.out.println(((BaseTable) icebergTable).operations().current().metadataFileLocation());
      // Get the new version
      Snapshot newSnapshot = icebergTable.currentSnapshot();
      System.out.println(newSnapshot.summary());
      return newSnapshot != null ? newSnapshot.sequenceNumber() - 1 : readVersion + 1;

    } catch (Exception e) {
      if (e instanceof CommitFailedException) {
        throw (CommitFailedException) e;
      }
      throw new RuntimeException("Failed to commit transaction", e);
    }
  }

  private void applyConfigurationChanges() {
    // Apply schema changes
    if (config.getSchema().isPresent()) {
      try {
        this.pendingSchema =
            DeltaToIcebergSchemaConverter.toIcebergSchema(config.getSchema().get());
      } catch (Exception e) {
        throw new KernelException("Failed to convert schema: " + e.getMessage(), e);
      }
    }

    // Apply partition column changes
    if (config.getPartitionColumns().isPresent()) {
      if (operation != io.delta.kernel.Operation.CREATE_TABLE) {
        throw new KernelException("Cannot change partition columns on existing table");
      }

      try {
        this.pendingPartitionSpec = createPartitionSpec(config.getPartitionColumns().get());
      } catch (Exception e) {
        throw new KernelException("Failed to create partition spec: " + e.getMessage(), e);
      }
    }

    // Apply clustering columns (convert to sort order - placeholder)
    if (config.getClusteringColumns().isPresent()) {
      // todo: convert to Iceberg sort order
      System.out.println(
          "Clustering columns specified but not yet implemented: "
              + config.getClusteringColumns().get().stream()
                  .map(col -> String.join(".", col.getNames()))
                  .collect(Collectors.joining(", ")));
    }

    // Apply table properties
    if (config.getTableProperties().isPresent()) {
      pendingProperties.putAll(config.getTableProperties().get());
    }

    if (config.getPropertiesToRemove().isPresent()) {
      propertiesToRemove.addAll(config.getPropertiesToRemove().get());
    }
  }

  private PartitionSpec createPartitionSpec(List<String> partitionColumns) {
    if (partitionColumns.isEmpty()) {
      return PartitionSpec.unpartitioned();
    }

    PartitionSpec.Builder builder = PartitionSpec.builderFor(pendingSchema);

    for (String columnName : partitionColumns) {
      builder.identity(columnName);
    }

    return builder.build();
  }

  private void applyPropertyChanges(org.apache.iceberg.Transaction icebergTxn) {
    if (pendingProperties.isEmpty() && propertiesToRemove.isEmpty()) {
      return;
    }

    UpdateProperties updateProps = icebergTxn.updateProperties();

    // Apply property changes
    for (Map.Entry<String, String> entry : pendingProperties.entrySet()) {
      updateProps.set(entry.getKey(), entry.getValue());
    }

    for (String key : propertiesToRemove) {
      updateProps.remove(key);
    }

    updateProps.commit();
  }

  // ... (rest of the methods remain the same: processDataActions, convertDataActions, etc.)
  // I'm keeping the existing data action processing methods unchanged as they're working correctly

  private void processDataActions(
      Engine engine,
      org.apache.iceberg.Transaction icebergTxn,
      CloseableIterable<Row> dataActions) {
    // Clear previous file operations
    filesToAdd.clear();
    filesToRemove.clear();
    hasDataChange = false;

    // Convert Delta actions to Iceberg DataFiles
    convertDataActions(engine, dataActions);

    if (filesToAdd.isEmpty() && filesToRemove.isEmpty()) {
      return; // No file operations to perform
    }

    // Determine the appropriate Iceberg operation based on the action mix
    if (!filesToRemove.isEmpty() && !filesToAdd.isEmpty()) {
      // Mixed operations - use overwrite
      performOverwriteOperation(icebergTxn);
    } else if (!filesToAdd.isEmpty() && hasDataChange) {
      // Add files with data change - use append
      performAppendOperation(icebergTxn);
    } else if (!filesToRemove.isEmpty()) {
      // Remove files only - use delete
      performDeleteOperation(icebergTxn);
    } else {
      // Add files without data change (like compaction) - use rewrite
      performRewriteOperation(icebergTxn);
    }
  }

  private void convertDataActions(Engine engine, CloseableIterable<Row> dataActions) {
    try {
      for (Row action : dataActions) {
        ActionType actionType = determineActionType(action);

        switch (actionType) {
          case ADD_FILE:
            DataFile addFile = convertAddFileAction(engine, action);
            if (addFile != null) {
              filesToAdd.add(addFile);
              // Check if this is a data change operation
              hasDataChange |= extractDataChange(action, true);
            }
            break;

          case REMOVE_FILE:
            DataFile removeFile = convertRemoveFileAction(engine, action);
            if (removeFile != null) {
              filesToRemove.add(removeFile);
              // Check if this is a data change operation
              hasDataChange |= extractDataChange(action, false);
            }
            break;

          case METADATA:
          case PROTOCOL:
          case COMMIT_INFO:
            // These are handled elsewhere or ignored
            break;

          default:
            // Log unknown action types but don't fail
            System.out.println("Unknown action type encountered: " + action);
            break;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to process data actions", e);
    }
  }

  private ActionType determineActionType(Row action) {
    // Check specific ordinals as defined in SingleAction to determine action type
    if (!action.isNullAt(ADD_FILE_ORDINAL)) {
      return ActionType.ADD_FILE;
    } else if (!action.isNullAt(REMOVE_FILE_ORDINAL)) {
      return ActionType.REMOVE_FILE;
    } else if (!action.isNullAt(METADATA_ORDINAL)) {
      return ActionType.METADATA;
    }

    return ActionType.UNKNOWN;
  }

  private DataFile convertAddFileAction(Engine engine, Row action) {
    try {
      // Create AddFile instance from the action row
      AddFile addFile = new AddFile(action.getStruct(ADD_FILE_ORDINAL));

      // Extract fields using AddFile methods
      String path = addFile.getPath();
      long size = addFile.getSize();

      // Build the DataFile using Iceberg's DataFiles builder
      DataFiles.Builder builder =
          DataFiles.builder(pendingPartitionSpec)
              .withPath(canonicalizeFilePath(path))
              .withFileSizeInBytes(size)
              .withFormat(FileFormat.PARQUET);

      // Add record count if available
      Optional<Long> recordCount = addFile.getNumRecords();
      if (recordCount.isPresent()) {
        builder = builder.withRecordCount(recordCount.get());
      }

      // Add partition values if needed
      MapValue partitionValues = addFile.getPartitionValues();
      // You might need to convert Delta partition values to Iceberg partition values
      // This depends on how you want to handle partitioning

      return builder.build();

    } catch (Exception e) {
      System.err.println("Failed to convert AddFile action: " + e.getMessage());
      return null;
    }
  }

  private DataFile convertRemoveFileAction(Engine engine, Row action) {
    try {
      // Create RemoveFile instance from the action row
      RemoveFile removeFile = new RemoveFile(action.getStruct(REMOVE_FILE_ORDINAL));

      // Extract fields using RemoveFile methods
      String path = removeFile.getPath();
      Optional<Long> sizeOpt = removeFile.getSize();
      long size = sizeOpt.orElse(0L);

      DataFiles.Builder builder =
          DataFiles.builder(pendingPartitionSpec)
              .withPath(canonicalizeFilePath(path))
              .withFileSizeInBytes(size)
              .withFormat(FileFormat.PARQUET);

      return builder.build();

    } catch (Exception e) {
      System.err.println("Failed to convert RemoveFile action: " + e.getMessage());
      return null;
    }
  }

  private boolean extractDataChange(Row action, boolean isAdd) {
    try {
      if (isAdd) {
        AddFile addFile = new AddFile(action.getStruct(ADD_FILE_ORDINAL));
        return addFile.getDataChange();
      } else {
        RemoveFile removeFile = new RemoveFile(action.getStruct(REMOVE_FILE_ORDINAL));
        return removeFile.getDataChange();
      }
    } catch (Exception e) {
      // Default to true for safety
      return true;
    }
  }

  // Update the enum to include all action types
  private enum ActionType {
    ADD_FILE,
    REMOVE_FILE,
    METADATA,
    PROTOCOL,
    COMMIT_INFO,
    SET_TRANSACTION,
    DOMAIN_METADATA,
    UNKNOWN
  }

  private void performAppendOperation(org.apache.iceberg.Transaction icebergTxn) {
    AppendFiles appendFiles = icebergTxn.newAppend();
    for (DataFile dataFile : filesToAdd) {
      System.out.println("adding file" + dataFile.toString());
      appendFiles.appendFile(dataFile);
    }
    appendFiles.commit();
  }

  private void performDeleteOperation(org.apache.iceberg.Transaction icebergTxn) {
    DeleteFiles deleteFiles = icebergTxn.newDelete();
    for (DataFile dataFile : filesToRemove) {
      deleteFiles.deleteFile(dataFile);
    }
    deleteFiles.commit();
  }

  private void performOverwriteOperation(org.apache.iceberg.Transaction icebergTxn) {
    OverwriteFiles overwriteFiles = icebergTxn.newOverwrite();

    // Add files to remove
    for (DataFile dataFile : filesToRemove) {
      overwriteFiles.deleteFile(dataFile);
    }

    // Add files to add
    for (DataFile dataFile : filesToAdd) {
      overwriteFiles.addFile(dataFile);
    }

    overwriteFiles.commit();
  }

  private void performRewriteOperation(org.apache.iceberg.Transaction icebergTxn) {
    RewriteFiles rewriteFiles = icebergTxn.newRewrite();

    if (!filesToRemove.isEmpty()) {
      Set<DataFile> toReplace = new HashSet<>(filesToRemove);
      Set<DataFile> replacements = new HashSet<>(filesToAdd);
      rewriteFiles.rewriteFiles(toReplace, replacements);
    }

    rewriteFiles.commit();
  }

  // Helper methods for extracting information from Delta action Rows
  private String extractFilePath(Row action) {
    return action.getString(0); // Assuming path is at ordinal 0
  }

  private long extractFileSize(Row action) {
    return action.getLong(1); // Assuming size is at ordinal 1
  }

  private Long extractRecordCount(Row action) {
    return null; // Return null if not available
  }

  private String canonicalizeFilePath(String path) {
    if (path.startsWith("/") || path.contains("://")) {
      return path; // Already absolute
    } else {
      return tablePath + "/" + path; // Make relative path absolute
    }
  }

  // Configuration holder class
  public static class TransactionConfiguration {
    private final Optional<StructType> schema;
    private final Optional<List<String>> partitionColumns;
    private final Optional<List<Column>> clusteringColumns;
    private final Optional<Map<String, String>> tableProperties;
    private final Optional<Set<String>> propertiesToRemove;

    public TransactionConfiguration(
        Optional<StructType> schema,
        Optional<List<String>> partitionColumns,
        Optional<List<Column>> clusteringColumns,
        Optional<Map<String, String>> tableProperties,
        Optional<Set<String>> propertiesToRemove) {
      this.schema = schema;
      this.partitionColumns = partitionColumns;
      this.clusteringColumns = clusteringColumns;
      this.tableProperties = tableProperties;
      this.propertiesToRemove = propertiesToRemove;
    }

    public Optional<StructType> getSchema() {
      return schema;
    }

    public Optional<List<String>> getPartitionColumns() {
      return partitionColumns;
    }

    public Optional<List<Column>> getClusteringColumns() {
      return clusteringColumns;
    }

    public Optional<Map<String, String>> getTableProperties() {
      return tableProperties;
    }

    public Optional<Set<String>> getPropertiesToRemove() {
      return propertiesToRemove;
    }
  }
}
