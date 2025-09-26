package io.delta.kernel.spark.catalog.utils;

import io.delta.kernel.Snapshot;
import io.delta.kernel.internal.DeltaHistoryManager;
import org.apache.spark.sql.AnalysisException;

import java.sql.Timestamp;

public interface CatalogTableManager {

    Snapshot unsafeVolatileSnapshot();

    Snapshot update();

    DeltaHistoryManager.Commit getActiveCommitAtTime(Timestamp timeStamp,  Boolean canReturnLastCommit,
                                                     Boolean mustBeRecreatable, Boolean canReturnEarliestCommit);
    void checkVersionExists(Long version, Boolean mustBeRecreatable, Boolean allowOutOfRange) throws AnalysisException;

}
