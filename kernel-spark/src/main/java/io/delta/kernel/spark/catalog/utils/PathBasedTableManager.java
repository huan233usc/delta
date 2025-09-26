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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class PathBasedTableManager implements CatalogTableManager {

    private final String tablePath;
    private final AtomicReference<Snapshot> snapshotAtomicReference;
    private final Engine kernelEngine;


    public PathBasedTableManager(String tablePath, Configuration hadoopConf) {
        this.tablePath = tablePath;
        this.snapshotAtomicReference = new AtomicReference<>();
        this.kernelEngine = DefaultEngine.create(hadoopConf);

    }

    @Override
    public Snapshot unsafeVolatileSnapshot() {
        Snapshot unsafeVolatileSnapshot = snapshotAtomicReference.get();
        if(unsafeVolatileSnapshot == null) {
            return update();
        }
        return unsafeVolatileSnapshot;
    }

    @Override
    public Snapshot update() {
        Snapshot snapshot = TableManager.loadSnapshot(tablePath).build(kernelEngine);
        snapshotAtomicReference.set(snapshot);
        return snapshot;
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
