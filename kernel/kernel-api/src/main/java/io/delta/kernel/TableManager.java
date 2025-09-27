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

package io.delta.kernel;

import io.delta.kernel.annotation.Experimental;
import io.delta.kernel.internal.CreateTableTransactionBuilderImpl;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.commitrange.CommitRangeBuilderImpl;
import io.delta.kernel.internal.table.SnapshotBuilderImpl;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.types.StructType;
import java.sql.Timestamp;
import java.util.Map;

/**
 * The entry point for loading and creating Delta tables.
 *
 * <p>TableManager provides both static factory methods for direct table access and instance methods
 * for catalog-aware table management. This is the unified interface for all Delta table operations.
 *
 * <p><b>Static methods</b> (existing): Used for direct filesystem table access
 *
 * <ul>
 *   <li>{@link #loadSnapshot(String)} - Load snapshot from path
 *   <li>{@link #buildCreateTableTransaction} - Create table transaction
 *   <li>{@link #loadCommitRange(String)} - Load commit range
 * </ul>
 *
 * <p><b>Instance methods</b> (new): Used for catalog-managed tables (Unity Catalog, etc.)
 *
 * <ul>
 *   <li>{@link #unsafeVolatileSnapshot()} - Get cached snapshot
 *   <li>{@link #update()} - Refresh snapshot
 *   <li>Time travel and streaming support methods
 *   <li>Lifecycle management methods
 * </ul>
 *
 * <p>Implementations should provide a no-arg constructor for dynamic loading and call {@link
 * #initialize(String, Map)} after construction.
 */
@Experimental
public interface TableManager {

  /**
   * Creates a builder for loading a snapshot at the given path.
   *
   * <p>The returned builder can be configured to load the snapshot at a specific version or with
   * additional metadata to optimize the loading process. If no version is specified, the builder
   * will resolve to the latest version of the table.
   *
   * @param path the file system path to the Delta table
   * @return a {@link SnapshotBuilder} that can be used to load a {@link Snapshot} at the given path
   */
  static SnapshotBuilder loadSnapshot(String path) {
    return new SnapshotBuilderImpl(path);
  }

  /**
   * Creates a {@link CreateTableTransactionBuilder} to build a create table transaction.
   *
   * @param path the file system path for the delta table being created
   * @param engineInfo information about the engine that is making the update.
   * @param schema the schema for the delta table being created
   * @return create table builder instance to build the transaction
   * @since 3.4.0
   */
  static CreateTableTransactionBuilder buildCreateTableTransaction(
      String path, StructType schema, String engineInfo) {
    return new CreateTableTransactionBuilderImpl(path, schema, engineInfo);
  }

  /**
   * Creates a builder for loading a CommitRange at a given path.
   *
   * <p>The returned builder can be configured with start version or timestamp and an end version or
   * timestamp, and with additional metadata to optimize the loading process.
   *
   * @param path the file system path to the Delta table
   * @return a {@link CommitRangeBuilder} that can be used to load a {@link CommitRange} at the
   *     given path
   */
  static CommitRangeBuilder loadCommitRange(String path) {
    return new CommitRangeBuilderImpl(path);
  }

  // =============================================================================
  // Instance Methods for Catalog-Aware Table Management
  // =============================================================================

  /**
   * Initialize the table manager with table path and configuration properties. Called once after
   * construction, similar to Iceberg's Catalog.initialize().
   *
   * <p>All necessary configuration (including Spark configs, Hadoop configs, etc.) should be passed
   * via the properties map to avoid direct engine dependencies.
   *
   * @param tablePath the Delta table path
   * @param properties configuration properties from catalog table and engine conf
   */
  default void initialize(String tablePath, Map<String, String> properties) {
    // Default implementation does nothing - backwards compatible
    // Implementations that need initialization should override this
  }

  /**
   * Clean up resources when the table manager is no longer needed. Should be idempotent and safe to
   * call multiple times.
   */
  default void close() {
    // Default implementation does nothing - backwards compatible
    // Implementations that need cleanup should override this
  }

  /**
   * Gets a cached snapshot without guaranteeing freshness. May return stale data for performance
   * reasons.
   *
   * <p>For catalog-managed tables, this may use cached snapshots from the catalog service. For
   * direct table access, this behaves like a single call to {@link #update()}.
   *
   * @return a potentially stale snapshot of the table
   */
  default Snapshot unsafeVolatileSnapshot() {
    // Default implementation calls update() - backwards compatible
    return update();
  }

  /**
   * Refreshes and returns the latest snapshot from the table. This method guarantees to return the
   * most recent version available.
   *
   * <p>For catalog-managed tables, this will contact the catalog service. For direct table access,
   * this reads the latest commit from the Delta log.
   *
   * @return the latest snapshot of the table
   */
  default Snapshot update() {
    // Default implementation throws - subclasses must implement
    throw new UnsupportedOperationException(
        "update() method not implemented. "
            + "Use static loadSnapshot() method for direct table access, "
            + "or implement this method for catalog-managed tables.");
  }

  /**
   * Get the commit active at a specific timestamp for streaming and time travel scenarios.
   *
   * <p>This method is used by streaming queries to find the appropriate starting point and by time
   * travel queries to resolve historical versions.
   *
   * @param timeStamp the target timestamp
   * @param canReturnLastCommit whether to return the latest commit if timestamp is too recent
   * @param mustBeRecreatable whether the commit must be recreatable (affects earliest bounds)
   * @param canReturnEarliestCommit whether to return earliest commit if timestamp is too old
   * @return the commit information for the specified timestamp
   */
  default DeltaHistoryManager.Commit getActiveCommitAtTime(
      Timestamp timeStamp,
      Boolean canReturnLastCommit,
      Boolean mustBeRecreatable,
      Boolean canReturnEarliestCommit) {
    // Default implementation throws - subclasses must implement for time travel support
    throw new UnsupportedOperationException(
        "getActiveCommitAtTime() method not implemented. "
            + "This method is required for streaming and time travel functionality.");
  }

  /**
   * Verify that a specific version exists and is accessible.
   *
   * <p>This method is used to validate version parameters in streaming and time travel scenarios
   * before attempting to access the data.
   *
   * @param version the version to check
   * @param mustBeRecreatable whether the version must be recreatable
   * @param allowOutOfRange whether to allow versions beyond the latest available
   * @throws TableManagerException if the version doesn't exist or isn't accessible
   */
  default void checkVersionExists(Long version, Boolean mustBeRecreatable, Boolean allowOutOfRange)
      throws TableManagerException {
    // Default implementation throws - subclasses must implement for version validation
    throw new UnsupportedOperationException(
        "checkVersionExists() method not implemented. "
            + "This method is required for version validation in streaming scenarios.");
  }

  /**
   * Return the type identifier for this table manager implementation. Used for logging and
   * debugging purposes.
   *
   * @return a string identifying the table manager type
   */
  default String getManagerType() {
    return this.getClass().getSimpleName();
  }

  /**
   * Exception class for table manager operations. Replaces engine-specific exceptions to avoid
   * dependencies.
   */
  class TableManagerException extends Exception {
    public TableManagerException(String message) {
      super(message);
    }

    public TableManagerException(String message, Throwable cause) {
      super(message, cause);
    }

    public static TableManagerException versionNotFound(long version) {
      return new TableManagerException("Version " + version + " not found");
    }

    public static TableManagerException tableNotFound(String tablePath) {
      return new TableManagerException("Table not found at path: " + tablePath);
    }
  }
}
