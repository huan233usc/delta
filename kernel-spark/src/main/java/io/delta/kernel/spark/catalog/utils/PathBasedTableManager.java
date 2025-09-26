package io.delta.kernel.spark.catalog.utils;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import org.apache.hadoop.conf.Configuration;
import java.sql.Timestamp;


import java.util.Map;
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
    public DeltaHistoryManager.Commit getActiveCommitAtTime(Timestamp timeStamp, ) {
        return null;
    }
}
