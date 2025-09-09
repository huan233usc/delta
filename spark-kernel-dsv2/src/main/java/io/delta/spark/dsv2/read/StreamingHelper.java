package io.delta.spark.dsv2.read;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.SnapshotBuilder;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to manage Delta table snapshot lifecycle for streaming. to be replaced via ccv2's
 * get change api
 */
public class StreamingHelper {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingHelper.class);

  private final String tablePath;
  private final Engine engine;
  private final AtomicReference<SnapshotImpl> currentSnapshotRef;

  public StreamingHelper(String tablePath, Engine engine, SnapshotImpl initialSnapshot) {
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.engine = requireNonNull(engine, "engine is null");
    this.currentSnapshotRef =
        new AtomicReference<>(requireNonNull(initialSnapshot, "initialSnapshot is null"));

    LOG.info(
        "Created KernelTableHelper for table: {} at initial version: {}",
        tablePath,
        initialSnapshot.getVersion());
  }

  /**
   * Refreshes the current snapshot to the latest version.
   *
   * @return the latest snapshot
   * @throws TableNotFoundException if the table cannot be found
   */
  public SnapshotImpl refreshToLatest() throws TableNotFoundException {
    LOG.info("Refreshing table snapshot for path: {}", tablePath);
    SnapshotBuilder builder = TableManager.loadSnapshot(tablePath);
    SnapshotImpl latest = (SnapshotImpl) builder.build(engine);
    currentSnapshotRef.set(latest);
    LOG.info("Refreshed to latest snapshot version: {}", latest.getVersion());
    return latest;
  }

  /**
   * Gets the current snapshot without refreshing.
   *
   * @return the current snapshot
   */
  public SnapshotImpl getCurrentSnapshot() {
    return currentSnapshotRef.get();
  }

  /**
   * Gets the latest table version by refreshing the snapshot.
   *
   * @return the latest version number
   * @throws TableNotFoundException if the table cannot be found
   */
  public long getLatestVersion() throws TableNotFoundException {
    SnapshotImpl latest = refreshToLatest();
    return latest.getVersion();
  }

  /**
   * Gets incremental changes between versions using Kernel API.
   *
   * @param startVersion the starting version (inclusive)
   * @param endVersion the ending version (inclusive), or empty for latest
   * @param actionSet the set of actions to retrieve
   * @return an iterator of columnar batches containing the actions
   */
  public CloseableIterator<ColumnarBatch> getIncrementalChanges(
      long startVersion,
      Optional<Long> endVersion,
      Set<DeltaLogActionUtils.DeltaAction> actionSet) {

    long endVer =
        endVersion.orElseGet(
            () -> {
              try {
                return getLatestVersion();
              } catch (TableNotFoundException e) {
                throw new RuntimeException(
                    "Failed to get latest version for incremental changes", e);
              }
            });

    LOG.info(
        "Getting incremental changes from version {} to {} for actions: {}",
        startVersion,
        endVer,
        actionSet);

    List<FileStatus> commitFiles =
        DeltaLogActionUtils.getCommitFilesForVersionRange(
            engine, new Path(tablePath), startVersion, Optional.empty());

    return DeltaLogActionUtils.getActionsFromCommitFilesWithProtocolValidation(
        engine, tablePath, commitFiles, actionSet);
  }
}
