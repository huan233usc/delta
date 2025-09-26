package io.delta.kernel.spark.catalog.utils;

import io.delta.kernel.Snapshot;
import io.delta.kernel.internal.DeltaHistoryManager;

public interface CatalogTableManager {

    Snapshot unsafeVolatileSnapshot();

    Snapshot update();

    DeltaHistoryManager.Commit getActiveCommitAtTime();

}
