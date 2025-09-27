package io.delta.kernel.spark.catalog.utils;

import io.delta.kernel.Snapshot;
import io.delta.kernel.internal.DeltaHistoryManager;
import java.sql.Timestamp;
import java.util.Map;
import org.apache.spark.sql.AnalysisException;

/**
 * Unified interface for Delta table catalog operations. Mirrors Iceberg's Catalog interface pattern
 * with Delta-specific extensions.
 *
 * <p>Implementation Requirements: 1. Must have a no-arg constructor for dynamic loading 2. Must
 * call initialize() after construction 3. Should be thread-safe for concurrent snapshot access
 */
public interface DeltaTableManager {

  // Core snapshot operations (preserved from existing interface)
  Snapshot unsafeVolatileSnapshot();

  Snapshot update();

  // Streaming and time travel support (preserved from existing interface)
  DeltaHistoryManager.Commit getActiveCommitAtTime(
      Timestamp timeStamp,
      Boolean canReturnLastCommit,
      Boolean mustBeRecreatable,
      Boolean canReturnEarliestCommit);

  void checkVersionExists(Long version, Boolean mustBeRecreatable, Boolean allowOutOfRange)
      throws AnalysisException;

  // Lifecycle management (following Iceberg pattern)
  /**
   * Initialize the table manager with table path and configuration properties. Called once after
   * construction, similar to Iceberg's Catalog.initialize().
   *
   * @param tablePath the Delta table path
   * @param properties configuration properties from catalog table and Spark conf
   */
  void initialize(String tablePath, Map<String, String> properties);

  /**
   * Clean up resources when the table manager is no longer needed. Should be idempotent and safe to
   * call multiple times.
   */
  void close();

  // Optional: Enable custom implementations to provide metadata
  /**
   * Return the type identifier for this table manager implementation. Used for logging and
   * debugging purposes.
   */
  default String getManagerType() {
    return this.getClass().getSimpleName();
  }
}
