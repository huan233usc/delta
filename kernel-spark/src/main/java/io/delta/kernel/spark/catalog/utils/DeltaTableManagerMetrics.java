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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple metrics collection for Delta table manager operations.
 * Provides basic timing and counting metrics for monitoring and debugging.
 */
public class DeltaTableManagerMetrics {
    
    private final String managerType;
    private final AtomicLong snapshotLoadCount = new AtomicLong(0);
    private final AtomicLong snapshotLoadTimeTotal = new AtomicLong(0);
    private final AtomicLong snapshotCacheHits = new AtomicLong(0);
    private final AtomicLong snapshotCacheMisses = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final ConcurrentHashMap<String, AtomicLong> errorsByType = new ConcurrentHashMap<>();
    
    public DeltaTableManagerMetrics(String managerType) {
        this.managerType = managerType;
    }
    
    /**
     * Record a snapshot load operation.
     */
    public void recordSnapshotLoad(long durationMs) {
        snapshotLoadCount.incrementAndGet();
        snapshotLoadTimeTotal.addAndGet(durationMs);
    }
    
    /**
     * Record a snapshot cache hit.
     */
    public void recordSnapshotCacheHit() {
        snapshotCacheHits.incrementAndGet();
    }
    
    /**
     * Record a snapshot cache miss.
     */
    public void recordSnapshotCacheMiss() {
        snapshotCacheMisses.incrementAndGet();
    }
    
    /**
     * Record an error occurrence.
     */
    public void recordError(String errorType) {
        errorCount.incrementAndGet();
        errorsByType.computeIfAbsent(errorType, k -> new AtomicLong(0)).incrementAndGet();
    }
    
    /**
     * Execute an operation and record timing metrics.
     */
    public <T> T timeOperation(String operationName, ThrowingSupplier<T> supplier) throws Exception {
        long startTime = System.currentTimeMillis();
        try {
            T result = supplier.get();
            long duration = System.currentTimeMillis() - startTime;
            
            if ("snapshot_load".equals(operationName)) {
                recordSnapshotLoad(duration);
            }
            
            return result;
        } catch (Exception e) {
            recordError(e.getClass().getSimpleName());
            throw e;
        }
    }
    
    // Getters for metrics
    public String getManagerType() {
        return managerType;
    }
    
    public long getSnapshotLoadCount() {
        return snapshotLoadCount.get();
    }
    
    public long getSnapshotLoadTimeTotal() {
        return snapshotLoadTimeTotal.get();
    }
    
    public double getAverageSnapshotLoadTime() {
        long count = snapshotLoadCount.get();
        return count > 0 ? (double) snapshotLoadTimeTotal.get() / count : 0.0;
    }
    
    public long getSnapshotCacheHits() {
        return snapshotCacheHits.get();
    }
    
    public long getSnapshotCacheMisses() {
        return snapshotCacheMisses.get();
    }
    
    public double getCacheHitRate() {
        long hits = snapshotCacheHits.get();
        long misses = snapshotCacheMisses.get();
        long total = hits + misses;
        return total > 0 ? (double) hits / total : 0.0;
    }
    
    public long getErrorCount() {
        return errorCount.get();
    }
    
    public long getErrorCount(String errorType) {
        AtomicLong count = errorsByType.get(errorType);
        return count != null ? count.get() : 0;
    }
    
    /**
     * Get a summary of current metrics.
     */
    public String getMetricsSummary() {
        return String.format(
            "DeltaTableManager[%s] Metrics: " +
            "snapshots_loaded=%d, avg_load_time=%.2fms, " +
            "cache_hits=%d, cache_misses=%d, cache_hit_rate=%.2f%%, " +
            "errors=%d",
            managerType,
            getSnapshotLoadCount(),
            getAverageSnapshotLoadTime(),
            getSnapshotCacheHits(),
            getSnapshotCacheMisses(),
            getCacheHitRate() * 100,
            getErrorCount()
        );
    }
    
    /**
     * Reset all metrics to zero.
     */
    public void reset() {
        snapshotLoadCount.set(0);
        snapshotLoadTimeTotal.set(0);
        snapshotCacheHits.set(0);
        snapshotCacheMisses.set(0);
        errorCount.set(0);
        errorsByType.clear();
    }
    
    @FunctionalInterface
    public interface ThrowingSupplier<T> {
        T get() throws Exception;
    }
}
