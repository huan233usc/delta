# Delta Kernel-based DSv2 Connector: DeltaTableManager Architecture

This document describes the new DeltaTableManager architecture implemented according to the design document for centralized catalog management.

## Overview

The DeltaTableManager architecture follows Iceberg's proven centralized factory pattern, providing:

- **Unified interface** for all catalog types (Unity Catalog, Path-based, custom)
- **Dynamic class loading** for extensibility  
- **Properties-based configuration** for flexible setup
- **Lifecycle management** with proper resource cleanup
- **Consistent error handling** across all implementations

## Architecture Components

### 1. DeltaTableManager Interface

The core interface that all table manager implementations must follow:

```java
public interface DeltaTableManager {
    // Core snapshot operations
    Snapshot unsafeVolatileSnapshot();
    Snapshot update();
    
    // Streaming and time travel support  
    DeltaHistoryManager.Commit getActiveCommitAtTime(Timestamp timeStamp, ...);
    void checkVersionExists(Long version, ...);
    
    // Lifecycle management
    void initialize(String tablePath, Map<String, String> properties);
    void close();
    String getManagerType();
}
```

### 2. DeltaTableManagerFactory

Configuration-driven factory for creating table manager instances:

```java
public class DeltaTableManagerFactory {
    public static DeltaTableManager create(CatalogTable catalogTable);
    public static DeltaTableManager loadTableManager(String impl, ...);
}
```

### 3. Built-in Implementations

- **UnityCatalogTableManager** (in delta-unity module): For Unity Catalog managed tables
- **PathBasedTableManager** (in delta-kernel-spark): For direct filesystem access

## Usage Examples

### Basic Usage (Automatic Detection)

The factory automatically detects the appropriate table manager type:

```java
// Unity Catalog table (auto-detected)
CatalogTable ucTable = ...; // Has UC table ID
DeltaTableManager manager = DeltaTableManagerFactory.create(ucTable);
Snapshot snapshot = manager.unsafeVolatileSnapshot();
manager.close(); // Clean up resources

// Path-based table (auto-detected)  
CatalogTable pathTable = ...; // No UC table ID
DeltaTableManager manager = DeltaTableManagerFactory.create(pathTable);
Snapshot snapshot = manager.unsafeVolatileSnapshot();
manager.close();
```

### Unity Catalog Configuration

```properties
# Spark configuration for Unity Catalog
spark.sql.catalog.unity = io.unitycatalog.spark.UCSingleCatalog
spark.sql.catalog.unity.uri = https://uc-server.example.com
spark.sql.catalog.unity.token = <token>

# Delta automatically uses UnityCatalogTableManager for UC tables
```

### Explicit Type Configuration

```properties
# Force specific table manager type
spark.sql.catalog.unity.delta.table-manager.type = unity
spark.sql.catalog.custom.delta.table-manager.type = path
```

### Custom Implementation

```properties
# Use custom DeltaTableManager implementation
spark.sql.catalog.custom.delta.table-manager.impl = com.example.MyCustomTableManager
```

Example custom implementation:

```java
public class MyCustomTableManager implements DeltaTableManager {
    // Required: no-arg constructor for dynamic loading
    public MyCustomTableManager() {}
    
    @Override
    public void initialize(String tablePath, Map<String, String> properties) {
        // Custom initialization logic
    }
    
    @Override
    public Snapshot unsafeVolatileSnapshot() {
        // Custom snapshot loading logic
    }
    
    // ... implement other required methods
}
```

## Spark DSv2 Integration

The SparkTable class automatically uses the DeltaTableManagerFactory:

```java
public class SparkTable implements Table, SupportsRead, AutoCloseable {
    private final DeltaTableManager catalogTableManager;
    
    public SparkTable(Identifier identifier, CatalogTable catalogTable, ...) {
        // Factory automatically chooses the right implementation
        this.catalogTableManager = DeltaTableManagerFactory.create(catalogTable);
        this.snapshot = (SnapshotImpl) catalogTableManager.unsafeVolatileSnapshot();
    }
    
    @Override
    public void close() {
        if (catalogTableManager != null) {
            catalogTableManager.close();
        }
    }
}
```

## Error Handling

Unified error handling with DeltaTableManagerException:

```java
try {
    DeltaTableManager manager = DeltaTableManagerFactory.create(catalogTable);
    Snapshot snapshot = manager.update();
} catch (DeltaTableManagerException e) {
    if (e.isRetryable()) {
        // Retry logic for transient errors
    } else {
        // Handle permanent errors  
        logger.error("Permanent error: {} [{}]", e.getMessage(), e.getErrorType());
    }
}
```

### Error Types

- `CATALOG_UNAVAILABLE`: Catalog service is unavailable (retryable)
- `NETWORK_ERROR`: Network communication error (retryable)  
- `PERMISSION_DENIED`: Access permission denied (not retryable)
- `TABLE_NOT_FOUND`: Table not found (not retryable)
- `INVALID_CONFIGURATION`: Invalid configuration (not retryable)

## Metrics and Monitoring

Built-in metrics collection:

```java
DeltaTableManager manager = DeltaTableManagerFactory.create(catalogTable);

// Metrics are automatically collected
Snapshot snapshot = manager.update(); // Recorded as snapshot load

// Get metrics summary  
if (manager instanceof AbstractDeltaTableManager) {
    String summary = ((AbstractDeltaTableManager) manager).getMetricsSummary();
    logger.info("Table manager metrics: {}", summary);
}
```

Example metrics output:
```
DeltaTableManager[Unity Catalog] Metrics: snapshots_loaded=42, avg_load_time=145.32ms, 
cache_hits=38, cache_misses=4, cache_hit_rate=90.48%, errors=0
```

## Module Structure

```
delta-kernel-spark/
├── DeltaTableManager.java          # Core interface
├── DeltaTableManagerFactory.java   # Factory with dynamic loading
├── AbstractDeltaTableManager.java  # Base implementation
├── PathBasedTableManager.java      # Path-based implementation  
├── DeltaTableManagerException.java # Unified error handling
└── DeltaTableManagerMetrics.java   # Metrics collection

delta-unity/
└── UnityCatalogTableManager.java   # Unity Catalog implementation
```

## Benefits of This Architecture

### Iceberg Alignment
- **Proven pattern**: Uses the same centralized factory approach as Iceberg
- **Future-proof**: Positioned for CCv2 → IRC evolution
- **Industry standard**: Familiar pattern for developers

### Operational Excellence  
- **Unified debugging**: All table managers have consistent logging and metrics
- **Resource management**: Proper cleanup with close() methods
- **Error handling**: Consistent exception types across all implementations

### Extensibility
- **Plugin ecosystem**: Third parties can add custom implementations
- **Zero core changes**: New catalog types don't require delta-spark modifications  
- **Configuration-driven**: Properties-based selection mechanism

### Performance
- **Efficient caching**: Built-in snapshot caching with hit/miss metrics
- **Lazy loading**: Resources initialized only when needed
- **Connection pooling**: UC client reuse and proper cleanup

## Migration Guide

### From Legacy DeltaTableV2 (Path-based)

**Before:**
```java
DeltaTableV2 table = DeltaTableV2(spark, path);
Snapshot snapshot = table.snapshot();
```

**After:**
```java  
CatalogTable catalogTable = createCatalogTable(path);
DeltaTableManager manager = DeltaTableManagerFactory.create(catalogTable);
Snapshot snapshot = manager.unsafeVolatileSnapshot();
manager.close(); // Important: clean up resources
```

### From Legacy UC Integration

**Before:**
```java
// Manual UC client setup and snapshot loading
UCCatalogManagedClient client = ...;
Snapshot snapshot = client.loadSnapshot(...);
```

**After:**
```java
// Automatic UC detection and client setup
CatalogTable ucTable = ...; // With UC table ID
DeltaTableManager manager = DeltaTableManagerFactory.create(ucTable);
Snapshot snapshot = manager.unsafeVolatileSnapshot();
manager.close();
```

## Future Extensions

The architecture is designed to easily accommodate future catalog types:

### AWS Glue Support
```java
// New implementation in delta-glue module
public class GlueTableManager implements DeltaTableManager {
    // Glue API → CCv2 translation
}

// Configuration
spark.sql.catalog.glue.delta.table-manager.type = glue
```

### Apache Hive Metastore Support  
```java
// New implementation in delta-hms module
public class HiveMetastoreTableManager implements DeltaTableManager {
    // HMS API → CCv2 translation
}

// Configuration
spark.sql.catalog.hive.delta.table-manager.type = hms
```

This architecture provides a solid foundation for Delta's multi-catalog future while maintaining compatibility with existing deployments.
