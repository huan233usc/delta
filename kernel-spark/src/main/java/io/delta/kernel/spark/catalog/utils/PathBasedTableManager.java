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

package io.delta.kernel.spark.catalog.utils;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.SnapshotImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Path-based implementation for filesystem-only tables.
 * Located in delta-kernel-spark module as it only uses core kernel functionality.
 * No catalog involved, direct Delta log access.
 */
public class PathBasedTableManager implements TableManager {

  private String tablePath;
  private Engine kernelEngine;
  private final AtomicReference<Snapshot> cachedSnapshot;

  /** Required no-arg constructor for dynamic loading. */
  public PathBasedTableManager() {
    this.cachedSnapshot = new AtomicReference<>();
  }

  /**
   * Legacy constructor for backward compatibility. This will be deprecated in favor of no-arg
   * constructor + initialize().
   *
   * @param tablePath the path to the Delta table
   * @param hadoopConf the Hadoop configuration
   */
  @Deprecated
  public PathBasedTableManager(String tablePath, Configuration hadoopConf) {
    this();
    Map<String, String> properties = new java.util.HashMap<>();
    // Convert Configuration to properties if needed
    initialize(tablePath, properties);
  }

  @Override
  public void initialize(String tablePath, Map<String, String> properties) {
    this.tablePath = tablePath;

    // Create Hadoop configuration from properties passed from Spark layer
    Configuration hadoopConf = SparkSession.active().sessionState().newHadoopConf();

    // Apply configuration properties passed via properties
    properties.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith("hadoop."))
        .forEach(entry -> hadoopConf.set(
            entry.getKey().substring(7), // Remove "hadoop." prefix
            entry.getValue()));

    // Apply other storage properties that may be relevant to Hadoop
    properties.entrySet().stream()
        .filter(entry -> !entry.getKey().startsWith("spark.")
            && !entry.getKey().startsWith("delta.")
            && !entry.getKey().equals("table.path")
            && !entry.getKey().startsWith("hadoop."))
        .forEach(entry -> hadoopConf.set(entry.getKey(), entry.getValue()));

    this.kernelEngine = DefaultEngine.create(hadoopConf);
  }

  @Override
  public Snapshot unsafeVolatileSnapshot() {
    Snapshot cached = cachedSnapshot.get();
    return cached != null ? cached : update();
  }

  @Override
  public Snapshot update() {
    if (tablePath == null) {
      throw new IllegalStateException("PathBasedTableManager is not initialized. Call initialize() first.");
    }

    Snapshot snapshot = TableManager.loadSnapshot(tablePath).build(kernelEngine);
    cachedSnapshot.set(snapshot);
    return snapshot;
  }

  @Override
  public DeltaHistoryManager.Commit getActiveCommitAtTime(
      Timestamp timeStamp,
      Boolean canReturnLastCommit,
      Boolean mustBeRecreatable,
      Boolean canReturnEarliestCommit) {

    if (tablePath == null) {
      throw new IllegalStateException("PathBasedTableManager is not initialized. Call initialize() first.");
    }

    // Direct filesystem access to Delta log
    SnapshotImpl snapshot = (SnapshotImpl) update();
    return DeltaHistoryManager.getActiveCommitAtTimestamp(
        kernelEngine,
        snapshot,
        snapshot.getLogPath(),
        timeStamp.getTime(),
        mustBeRecreatable,
        canReturnLastCommit,
        canReturnEarliestCommit,
        new ArrayList<>() // No ratified commits for path-based
    );
  }

  @Override
  public void checkVersionExists(Long version, Boolean mustBeRecreatable, Boolean allowOutOfRange)
      throws TableManagerException {

    if (tablePath == null) {
      throw new IllegalStateException("PathBasedTableManager is not initialized. Call initialize() first.");
    }

    try {
      SnapshotImpl snapshot = (SnapshotImpl) update();

      long earliest = mustBeRecreatable
          ? DeltaHistoryManager.getEarliestRecreatableCommit(
              kernelEngine, snapshot.getLogPath(), Optional.empty())
          : DeltaHistoryManager.getEarliestDeltaFile(
              kernelEngine, snapshot.getLogPath(), Optional.empty());

      long latest = snapshot.getVersion();

      if (version < earliest || ((version > latest) && !allowOutOfRange)) {
        throw TableManagerException.versionNotFound(version);
      }
    } catch (Exception e) {
      if (e instanceof TableManagerException) {
        throw e;
      }
      throw new TableManagerException(
          "Error checking version " + version + " for path-based table: " + e.getMessage(), e);
    }
  }

  @Override
  public String getManagerType() {
    return "Path-based";
  }

  @Override
  public void close() {
    // Path-based manager doesn't hold external resources to close
    cachedSnapshot.set(null);
  }
}