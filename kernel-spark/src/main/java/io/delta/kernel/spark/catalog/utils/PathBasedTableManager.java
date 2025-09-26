package io.delta.kernel.spark.catalog.utils;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.SnapshotImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.delta.VersionNotFoundException;

import java.sql.Timestamp;


import java.util.ArrayList;
import java.util.Optional;

public class PathBasedTableManager extends AbstractDeltaTableManager {

    public PathBasedTableManager(String tablePath, Configuration hadoopConf) {
        super(tablePath, DefaultEngine.create(hadoopConf));
    }

    @Override
    public Snapshot update() {
        Snapshot snapshot = TableManager.loadSnapshot(tablePath).build(kernelEngine);
        return cacheAndReturn(snapshot);
    }

    @Override
    public DeltaHistoryManager.Commit getActiveCommitAtTime(Timestamp timeStamp,
                                                            Boolean canReturnLastCommit,
                                                            Boolean mustBeRecreatable,
                                                            Boolean canReturnEarliestCommit) {
        SnapshotImpl snapshot = (SnapshotImpl)update();
        return DeltaHistoryManager.getActiveCommitAtTimestamp(
                kernelEngine,
                snapshot,snapshot.getLogPath(),
                timeStamp.getTime(),
                mustBeRecreatable,
                canReturnLastCommit,
                canReturnEarliestCommit,
                new ArrayList<>());
    }

    @Override
    public void checkVersionExists(Long version, Boolean mustBeRecreatable, Boolean allowOutOfRange) throws AnalysisException {
        SnapshotImpl snapshot = (SnapshotImpl) update();
        long earliest = mustBeRecreatable ? DeltaHistoryManager.getEarliestRecreatableCommit(
                kernelEngine, snapshot.getLogPath(), Optional.empty()
        ) : DeltaHistoryManager.getEarliestDeltaFile(kernelEngine, snapshot.getLogPath(), Optional.empty());

        long latest = snapshot.getVersion();
        if (version < earliest || ((version > latest) && !allowOutOfRange)) {
            throw new VersionNotFoundException(version, earliest, latest);
        }

    }
}
