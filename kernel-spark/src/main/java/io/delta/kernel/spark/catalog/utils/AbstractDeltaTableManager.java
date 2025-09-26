package io.delta.kernel.spark.catalog.utils;

import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;

import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * Abstract base class for Delta table managers.
 * Provides common functionality and reduces code duplication between implementations.
 */
public abstract class AbstractDeltaTableManager implements DeltaTableManager {

    protected final String tablePath;
    protected final AtomicReference<Snapshot> snapshotCache;
    protected final Engine kernelEngine;

    /**
     * Constructs the abstract base with common fields.
     * 
     * @param tablePath the path to the Delta table
     * @param kernelEngine the Delta Kernel engine for operations
     */
    protected AbstractDeltaTableManager(String tablePath, Engine kernelEngine) {
        this.tablePath = requireNonNull(tablePath, "tablePath cannot be null");
        this.kernelEngine = requireNonNull(kernelEngine, "kernelEngine cannot be null");
        this.snapshotCache = new AtomicReference<>();
    }

    /**
     * Gets a cached snapshot without guaranteeing freshness.
     * This implementation checks the cache and calls update() if no snapshot is cached.
     */
    @Override
    public final Snapshot unsafeVolatileSnapshot() {
        Snapshot cachedSnapshot = snapshotCache.get();
        if (cachedSnapshot == null) {
            return update();
        }
        return cachedSnapshot;
    }

    /**
     * Refreshes and returns the latest snapshot from the table.
     * This method must be implemented by subclasses to provide the specific
     * snapshot loading logic for their table type.
     * 
     * @return the latest snapshot
     */
    @Override
    public abstract Snapshot update();

    /**
     * Helper method for subclasses to cache a snapshot after loading it.
     * 
     * @param snapshot the snapshot to cache
     * @return the same snapshot for method chaining
     */
    protected final Snapshot cacheAndReturn(Snapshot snapshot) {
        snapshotCache.set(snapshot);
        return snapshot;
    }
}
