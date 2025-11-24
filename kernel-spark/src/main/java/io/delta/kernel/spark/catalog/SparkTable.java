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
package io.delta.kernel.spark.table;

import static io.delta.kernel.spark.utils.ScalaUtils.toScalaMap;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.spark.read.SparkScanBuilder;
import io.delta.kernel.spark.snapshot.DeltaSnapshotManager;
import io.delta.kernel.spark.snapshot.PathBasedSnapshotManager;
import io.delta.kernel.spark.utils.SchemaUtils;
import io.delta.kernel.spark.write.SparkRowLevelOperationBuilder;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.delta.DeltaTableUtils;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** DataSource V2 Table implementation for Delta Lake using the Delta Kernel API. */
public class SparkTable
    implements Table,
        SupportsRead,
        SupportsWrite,
        SupportsRowLevelOperations,
        org.apache.spark.sql.connector.catalog.SupportsMetadataColumns {

  private static final Set<TableCapability> CAPABILITIES =
      Collections.unmodifiableSet(
          EnumSet.of(
              TableCapability.BATCH_READ,
              TableCapability.MICRO_BATCH_READ,
              TableCapability.BATCH_WRITE));

  private final Identifier identifier;
  private final String tablePath;
  private final Map<String, String> options;
  private final DeltaSnapshotManager snapshotManager;
  /** Snapshot created during connector setup */
  private final Snapshot initialSnapshot;

  private final Configuration hadoopConf;
  private final io.delta.kernel.engine.Engine engine;

  private final StructType schema;
  private final List<String> partColNames;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final Column[] columns;
  private final Transform[] partitionTransforms;
  private final Optional<CatalogTable> catalogTable;

  /**
   * Creates a SparkTable from a filesystem path without a catalog table.
   *
   * @param identifier logical table identifier used by Spark's catalog
   * @param tablePath filesystem path to the Delta table root
   * @throws NullPointerException if identifier or tablePath is null
   */
  public SparkTable(Identifier identifier, String tablePath) {
    this(identifier, tablePath, Collections.emptyMap(), Optional.empty());
  }

  /**
   * Creates a SparkTable from a filesystem path with options.
   *
   * @param identifier logical table identifier used by Spark's catalog
   * @param tablePath filesystem path to the Delta table root
   * @param options table options used to configure the Hadoop conf, table reads and writes
   * @throws NullPointerException if identifier or tablePath is null
   */
  public SparkTable(Identifier identifier, String tablePath, Map<String, String> options) {
    this(identifier, tablePath, options, Optional.empty());
  }

  /**
   * Constructor that accepts a Spark CatalogTable and user-provided options. Extracts the table
   * location and storage properties from the catalog table, then merges with user options. User
   * options take precedence over catalog properties in case of conflicts.
   *
   * @param identifier logical table identifier used by Spark's catalog
   * @param catalogTable the Spark CatalogTable containing table metadata including location
   * @param options user-provided options to override catalog properties
   */
  public SparkTable(Identifier identifier, CatalogTable catalogTable, Map<String, String> options) {
    this(
        identifier,
        getDecodedPath(requireNonNull(catalogTable, "catalogTable is null").location()),
        options,
        Optional.of(catalogTable));
  }

  /**
   * Creates a SparkTable backed by a Delta Kernel snapshot manager and initializes Spark-facing
   * metadata (schemas, partitioning, capabilities).
   *
   * <p>Side effects: - Initializes a SnapshotManager for the given tablePath. - Loads the latest
   * snapshot via the manager. - Builds Hadoop configuration from options for subsequent I/O. -
   * Derives data schema, partition schema, and full table schema from the snapshot.
   *
   * <p>Notes: - Partition column order from the snapshot is preserved for partitioning and appended
   * after data columns in the public Spark schema, per Spark conventions. - Read-time scan options
   * are later merged with these options.
   */
  private SparkTable(
      Identifier identifier,
      String tablePath,
      Map<String, String> userOptions,
      Optional<CatalogTable> catalogTable) {
    this.identifier = requireNonNull(identifier, "identifier is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.catalogTable = catalogTable;
    // Merge options: file system options from catalog + user options (user takes precedence)
    // This follows the same pattern as DeltaTableV2 in delta-spark
    Map<String, String> merged = new HashMap<>();
    // Only extract file system options from table storage properties
    catalogTable.ifPresent(
        table ->
            scala.collection.JavaConverters.mapAsJavaMap(table.storage().properties())
                .forEach(
                    (key, value) -> {
                      if (DeltaTableUtils.validDeltaTableHadoopPrefixes()
                          .exists(prefix -> key.startsWith(prefix))) {
                        merged.put(key, value);
                      }
                    }));
    // User options override catalog properties
    merged.putAll(userOptions);
    this.options = Collections.unmodifiableMap(merged);

    this.hadoopConf =
        SparkSession.active().sessionState().newHadoopConfWithOptions(toScalaMap(options));
    this.engine = io.delta.kernel.defaults.engine.DefaultEngine.create(hadoopConf);
    this.snapshotManager = new PathBasedSnapshotManager(tablePath, hadoopConf);
    // Load the initial snapshot through the manager
    this.initialSnapshot = snapshotManager.loadLatestSnapshot();
    this.schema = SchemaUtils.convertKernelSchemaToSparkSchema(initialSnapshot.getSchema());
    this.partColNames =
        Collections.unmodifiableList(new ArrayList<>(initialSnapshot.getPartitionColumnNames()));

    final List<StructField> dataFields = new ArrayList<>();
    final List<StructField> partitionFields = new ArrayList<>();

    // Build a map for O(1) field lookups to improve performance
    Map<String, StructField> fieldMap = new HashMap<>();
    for (StructField field : schema.fields()) {
      fieldMap.put(field.name(), field);
    }

    // IMPORTANT: Add partition fields in the exact order specified by partColNames
    // This is crucial because the order in partColNames may differ from the order
    // in snapshotSchema, and we need to preserve the partColNames order for
    // proper partitioning behavior
    for (String partColName : partColNames) {
      StructField field = fieldMap.get(partColName);
      if (field != null) {
        partitionFields.add(field);
      }
    }

    // Add remaining fields as data fields (non-partition columns)
    // These are fields that exist in the schema but are not partition columns
    for (StructField field : schema.fields()) {
      if (!partColNames.contains(field.name())) {
        dataFields.add(field);
      }
    }
    this.dataSchema = new StructType(dataFields.toArray(new StructField[0]));
    this.partitionSchema = new StructType(partitionFields.toArray(new StructField[0]));

    this.columns = CatalogV2Util.structTypeToV2Columns(schema);
    this.partitionTransforms =
        partColNames.stream().map(Expressions::identity).toArray(Transform[]::new);
  }

  /**
   * Helper method to decode URI path handling URL-encoded characters correctly. E.g., converts
   * "spark%25dir%25prefix" to "spark%dir%prefix"
   */
  private static String getDecodedPath(java.net.URI location) {
    return new java.io.File(location).getPath();
  }

  /**
   * Returns the CatalogTable if this SparkTable was created from a catalog table.
   *
   * @return Optional containing the CatalogTable, or empty if this table was created from a path
   */
  public Optional<CatalogTable> getCatalogTable() {
    return catalogTable;
  }

  /** Returns the snapshot manager for this table. */
  public DeltaSnapshotManager snapshotManager() {
    return snapshotManager;
  }

  /** Returns the initial snapshot for this table. */
  public Snapshot initialSnapshot() {
    return initialSnapshot;
  }

  /** Returns the table path. */
  public String tablePath() {
    return tablePath;
  }

  /** Returns the data schema (non-partition columns). */
  public StructType dataSchema() {
    return dataSchema;
  }

  /** Returns the partition schema. */
  public StructType partitionSchema() {
    return partitionSchema;
  }

  /**
   * Creates a new SparkTable instance with additional time travel options.
   *
   * <p>This method is used by {@link org.apache.spark.sql.delta.catalog.DeltaCatalog} to inject
   * time travel parameters from SQL queries (e.g., VERSION AS OF, TIMESTAMP AS OF).
   *
   * <p>The time travel options will be merged with existing options and applied during scan
   * building.
   *
   * @param timeTravelOptions map containing time travel parameters (versionAsOf or timestampAsOf)
   * @return a new SparkTable instance with time travel options merged
   */
  public SparkTable withTimeTravelOptions(Map<String, String> timeTravelOptions) {
    Map<String, String> mergedOptions = new HashMap<>(this.options);
    mergedOptions.putAll(timeTravelOptions);

    if (catalogTable.isPresent()) {
      return new SparkTable(identifier, catalogTable.get(), mergedOptions);
    } else {
      // Path-based table
      return new SparkTable(identifier, this.tablePath, mergedOptions);
    }
  }

  @Override
  public String name() {
    return identifier.name();
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Column[] columns() {
    return columns;
  }

  @Override
  public Transform[] partitioning() {
    return partitionTransforms;
  }

  @Override
  public Map<String, String> properties() {
    Map<String, String> props = new HashMap<>(initialSnapshot.getTableProperties());
    return Collections.unmodifiableMap(props);
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap scanOptions) {
    Map<String, String> combined = new HashMap<>(this.options);
    combined.putAll(scanOptions.asCaseSensitiveMap());
    CaseInsensitiveStringMap merged = new CaseInsensitiveStringMap(combined);

    // Determine which snapshot to use (time travel or latest)
    Snapshot snapshotToUse = resolveSnapshot(merged);

    // Update schema if snapshot changed (time travel might have different schema)
    StructType dataSchemaToUse = this.dataSchema;
    StructType partitionSchemaToUse = this.partitionSchema;
    if (snapshotToUse != initialSnapshot) {
      // Time travel: recompute schemas from the time-traveled snapshot
      StructType timeTravelSchema =
          SchemaUtils.convertKernelSchemaToSparkSchema(snapshotToUse.getSchema());
      List<String> timeTravelPartCols = new ArrayList<>(snapshotToUse.getPartitionColumnNames());

      List<StructField> dataFields = new ArrayList<>();
      List<StructField> partitionFields = new ArrayList<>();
      Map<String, StructField> fieldMap = new HashMap<>();
      for (StructField field : timeTravelSchema.fields()) {
        fieldMap.put(field.name(), field);
      }

      for (String partColName : timeTravelPartCols) {
        StructField field = fieldMap.get(partColName);
        if (field != null) {
          partitionFields.add(field);
        }
      }

      for (StructField field : timeTravelSchema.fields()) {
        if (!timeTravelPartCols.contains(field.name())) {
          dataFields.add(field);
        }
      }

      dataSchemaToUse = new StructType(dataFields.toArray(new StructField[0]));
      partitionSchemaToUse = new StructType(partitionFields.toArray(new StructField[0]));
    }

    return new SparkScanBuilder(
        name(), snapshotToUse, snapshotManager, dataSchemaToUse, partitionSchemaToUse, merged);
  }

  /**
   * Resolves the snapshot to use based on time travel options.
   *
   * <p>Supports the following options:
   *
   * <ul>
   *   <li>{@code versionAsOf}: Read table at a specific version number
   *   <li>{@code timestampAsOf}: Read table as it existed at a specific timestamp
   * </ul>
   *
   * <p>If no time travel options are specified, returns the initial (latest) snapshot.
   *
   * @param options the merged options containing potential time travel parameters
   * @return the resolved snapshot (either time-traveled or latest)
   * @throws IllegalArgumentException if both versionAsOf and timestampAsOf are specified
   */
  private Snapshot resolveSnapshot(CaseInsensitiveStringMap options) {
    boolean hasVersionAsOf = options.containsKey("versionAsOf");
    boolean hasTimestampAsOf = options.containsKey("timestampAsOf");

    // Cannot specify both
    if (hasVersionAsOf && hasTimestampAsOf) {
      throw new IllegalArgumentException(
          "Cannot specify both 'versionAsOf' and 'timestampAsOf' options");
    }

    // Handle versionAsOf
    if (hasVersionAsOf) {
      long version;
      try {
        version = Long.parseLong(options.get("versionAsOf"));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid 'versionAsOf' value: "
                + options.get("versionAsOf")
                + ". Must be a valid long integer.",
            e);
      }

      if (version < 0) {
        throw new IllegalArgumentException(
            "Invalid 'versionAsOf' value: " + version + ". Version must be non-negative.");
      }

      return snapshotManager.loadSnapshotAt(version);
    }

    // Handle timestampAsOf
    if (hasTimestampAsOf) {
      String timestampStr = options.get("timestampAsOf");
      long timestampMillis;

      try {
        // Parse as milliseconds since epoch
        // TODO: Support ISO-8601 timestamp format (e.g., "2024-01-15T10:30:00Z")
        timestampMillis = Long.parseLong(timestampStr);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid 'timestampAsOf' value: "
                + timestampStr
                + ". Must be epoch milliseconds (e.g., "
                + System.currentTimeMillis()
                + ").",
            e);
      }

      // Find the commit active at this timestamp
      io.delta.kernel.internal.DeltaHistoryManager.Commit commit =
          snapshotManager.getActiveCommitAtTime(
              timestampMillis,
              /*canReturnLastCommit=*/ true,
              /*mustBeRecreatable=*/ true,
              /*canReturnEarliestCommit=*/ false);

      return snapshotManager.loadSnapshotAt(commit.getVersion());
    }

    // No time travel: use initial snapshot
    return initialSnapshot;
  }

  @Override
  public String toString() {
    return "SparkTable{identifier=" + identifier + '}';
  }

  /**
   * Create a WriteBuilder for batch write operations.
   *
   * <p>Uses Kernel's UpdateTableTransactionBuilder to create a transaction for writing data.
   */
  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    // Get the initial snapshot to build transaction from
    Snapshot snapshot = initialSnapshot;

    // Create UpdateTableTransactionBuilder from snapshot
    // This is the correct API for updating an existing table
    io.delta.kernel.transaction.UpdateTableTransactionBuilder txnBuilder =
        snapshot.buildUpdateTableTransaction(
            "kernel-spark", // engineInfo
            Operation.WRITE // operation
            );

    // Return WriteBuilder that wraps the TransactionBuilder
    // Pass hadoopConf instead of engine for serialization
    return new io.delta.kernel.spark.write.SparkWriteBuilder(
        txnBuilder, hadoopConf, info.schema(), info.queryId());
  }

  // ========== SupportsDelete Implementation ==========

  /**
   * Check if we can delete rows matching the given filters.
   *
   * <p>For now, we support delete operations without filters (DELETE * FROM table) or metadata-only
   * deletes. Row-level deletes that require rewriting files are not yet implemented.
   *
   * @param filters the filters to check
   * @return true if delete is supported
   */
  // ========================================================================================
  // SupportsRowLevelOperations Implementation (DELETE/UPDATE/MERGE Support)
  // ========================================================================================

  @Override
  public RowLevelOperationBuilder newRowLevelOperationBuilder(RowLevelOperationInfo info) {
    return new SparkRowLevelOperationBuilder(this, initialSnapshot, engine, hadoopConf, info);
  }

  // ========================================================================================
  // SupportsMetadataColumns Implementation (for DV-based DELETE/UPDATE)
  // ========================================================================================

  /**
   * Define metadata columns that can be used in DV-based DELETE/UPDATE operations.
   *
   * <p>These columns are automatically added by Spark when reading data and can be used to identify
   * which rows in which files should be deleted.
   *
   * <p>Supported metadata columns: - `_metadata.file_path`: The path of the file containing the row
   * - `_metadata.file_row_index`: The row index within that file (0-based)
   */
  @Override
  public org.apache.spark.sql.connector.catalog.MetadataColumn[] metadataColumns() {
    return new org.apache.spark.sql.connector.catalog.MetadataColumn[] {
      // File path metadata column (following Iceberg's naming: _file)
      new org.apache.spark.sql.connector.catalog.MetadataColumn() {
        @Override
        public String name() {
          return "_file";
        }

        @Override
        public org.apache.spark.sql.types.DataType dataType() {
          return org.apache.spark.sql.types.DataTypes.StringType;
        }

        @Override
        public boolean isNullable() {
          return false;
        }

        @Override
        public String comment() {
          return "Path of the file in which a row is stored";
        }
      },
      // Row position metadata column (following Iceberg's naming: _pos)
      new org.apache.spark.sql.connector.catalog.MetadataColumn() {
        @Override
        public String name() {
          return "_pos";
        }

        @Override
        public org.apache.spark.sql.types.DataType dataType() {
          return org.apache.spark.sql.types.DataTypes.LongType;
        }

        @Override
        public boolean isNullable() {
          return false;
        }

        @Override
        public String comment() {
          return "Ordinal position of a row in the source data file";
        }
      }
    };
  }
}
