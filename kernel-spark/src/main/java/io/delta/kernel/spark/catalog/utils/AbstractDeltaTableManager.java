package io.delta.kernel.spark.catalog.utils;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Abstract base class for Delta table managers. Provides common functionality and reduces code
 * duplication between implementations. Enhanced to support lifecycle management as specified in the
 * design document.
 */
public abstract class AbstractDeltaTableManager implements DeltaTableManager {

  protected String tablePath;
  protected Map<String, String> properties;
  protected final AtomicReference<Snapshot> snapshotCache;
  protected Engine kernelEngine;

  // Track initialization and closure state
  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Required no-arg constructor for dynamic loading. Subclasses must call initialize() after
   * construction.
   */
  protected AbstractDeltaTableManager() {
    this.snapshotCache = new AtomicReference<>();
  }

  /**
   * Legacy constructor for backward compatibility. This will be deprecated in favor of no-arg
   * constructor + initialize().
   *
   * @param tablePath the path to the Delta table
   * @param kernelEngine the Delta Kernel engine for operations
   */
  @Deprecated
  protected AbstractDeltaTableManager(String tablePath, Engine kernelEngine) {
    this();
    // Auto-initialize for legacy constructor
    this.tablePath = requireNonNull(tablePath, "tablePath cannot be null");
    this.kernelEngine = requireNonNull(kernelEngine, "kernelEngine cannot be null");
    this.initialized.set(true);
  }

  @Override
  public void initialize(String tablePath, Map<String, String> properties) {
    if (initialized.getAndSet(true)) {
      throw new IllegalStateException("DeltaTableManager is already initialized");
    }

    this.tablePath = requireNonNull(tablePath, "tablePath cannot be null");
    this.properties = requireNonNull(properties, "properties cannot be null");

    // Create engine from properties - to be implemented by subclasses
    this.kernelEngine = createEngine(properties);
    requireNonNull(this.kernelEngine, "kernelEngine cannot be null after initialization");

    // Allow subclasses to perform additional initialization
    doInitialize(properties);
  }

  @Override
  public void close() {
    if (!closed.getAndSet(true)) {
      // Clear snapshot cache
      snapshotCache.set(null);

      // Allow subclasses to clean up resources
      doClose();
    }
  }

  /**
   * Gets a cached snapshot without guaranteeing freshness. This implementation checks the cache and
   * calls update() if no snapshot is cached.
   */
  @Override
  public final Snapshot unsafeVolatileSnapshot() {
    checkInitialized();
    checkNotClosed();

    Snapshot cachedSnapshot = snapshotCache.get();
    if (cachedSnapshot == null) {
      return update();
    }
    return cachedSnapshot;
  }

  /**
   * Refreshes and returns the latest snapshot from the table. This method must be implemented by
   * subclasses to provide the specific snapshot loading logic for their table type.
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

  /**
   * Create engine from properties. To be implemented by subclasses. This is called during
   * initialization to set up the Delta Kernel engine.
   *
   * @param properties configuration properties
   * @return configured engine instance
   */
  protected abstract Engine createEngine(Map<String, String> properties);

  /**
   * Perform subclass-specific initialization. Called after basic initialization is complete.
   *
   * @param properties configuration properties
   */
  protected void doInitialize(Map<String, String> properties) {
    // Default implementation does nothing
    // Subclasses can override for custom initialization
  }

  /** Perform subclass-specific cleanup. Called when the manager is being closed. */
  protected void doClose() {
    // Default implementation does nothing
    // Subclasses can override for custom cleanup
  }

  // Helper methods for state validation
  protected final void checkInitialized() {
    if (!initialized.get()) {
      throw new IllegalStateException(
          "DeltaTableManager is not initialized. Call initialize() first.");
    }
  }

  protected final void checkNotClosed() {
    if (closed.get()) {
      throw new IllegalStateException("DeltaTableManager is closed and cannot be used.");
    }
  }
}
