package io.delta.kernel.spark.catalog.utils;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.SnapshotImpl;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.VersionNotFoundException;

/**
 * Path-based implementation for filesystem-only tables. Located in delta-kernel-spark module as it
 * only uses core kernel functionality. No catalog involved, direct Delta log access.
 */
public class PathBasedTableManager extends AbstractDeltaTableManager {

  /** Required no-arg constructor for dynamic loading. */
  public PathBasedTableManager() {
    super();
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
    super(tablePath, DefaultEngine.create(hadoopConf));
  }

  @Override
  protected Engine createEngine(Map<String, String> properties) {
    // Create Hadoop configuration from properties
    Configuration hadoopConf = SparkSession.active().sessionState().newHadoopConf();

    // Apply configuration properties
    properties.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith("hadoop."))
        .forEach(
            entry ->
                hadoopConf.set(
                    entry.getKey().substring(7), // Remove "hadoop." prefix
                    entry.getValue()));

    // Apply storage properties that may be relevant to Hadoop
    properties.entrySet().stream()
        .filter(
            entry ->
                !entry.getKey().startsWith("spark.")
                    && !entry.getKey().startsWith("delta.")
                    && !entry.getKey().equals("table.path"))
        .forEach(entry -> hadoopConf.set(entry.getKey(), entry.getValue()));

    return DefaultEngine.create(hadoopConf);
  }

  @Override
  public Snapshot update() {
    checkInitialized();
    checkNotClosed();

    Snapshot snapshot = TableManager.loadSnapshot(tablePath).build(kernelEngine);
    return cacheAndReturn(snapshot);
  }

  @Override
  public DeltaHistoryManager.Commit getActiveCommitAtTime(
      Timestamp timeStamp,
      Boolean canReturnLastCommit,
      Boolean mustBeRecreatable,
      Boolean canReturnEarliestCommit) {
    checkInitialized();
    checkNotClosed();

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
      throws AnalysisException {
    checkInitialized();
    checkNotClosed();

    SnapshotImpl snapshot = (SnapshotImpl) update();

    long earliest =
        mustBeRecreatable
            ? DeltaHistoryManager.getEarliestRecreatableCommit(
                kernelEngine, snapshot.getLogPath(), Optional.empty())
            : DeltaHistoryManager.getEarliestDeltaFile(
                kernelEngine, snapshot.getLogPath(), Optional.empty());

    long latest = snapshot.getVersion();

    if (version < earliest || ((version > latest) && !allowOutOfRange)) {
      throw new VersionNotFoundException(version, earliest, latest);
    }
  }

  @Override
  public String getManagerType() {
    return "Path-based";
  }

  @Override
  protected void doClose() {
    // Path-based manager doesn't hold external resources to close
    // Just ensure parent cleanup happens
    super.doClose();
  }
}
