# Unity Catalog Integration Test Guide

This guide explains how to run Delta Lake tests with Unity Catalog master branch integration.

## Overview

The Unity Catalog integration uses an embedded server approach similar to Unity Catalog's own test setup. The test infrastructure:
- Automatically clones and compiles Unity Catalog from the `main` branch
- Patches Unity Catalog to support Scala 2.12 (required for Delta Lake compatibility)
- Starts an embedded Unity Catalog server during test execution
- Uses the Unity Catalog Spark connector for table operations
- Cleans up automatically after tests complete

## Prerequisites

- Java 11 or higher (Java 17+ required for Unity Catalog server compilation)
- Python 3.6 or higher
- SBT (included in the Delta Lake repository)
- Git

## Quick Start

### Step 1: Generate Unity Catalog JARs

The first time you run the tests, or when you want to update to the latest Unity Catalog master branch, generate the JARs:

```bash
cd unityCatalogShaded
python3 generate_uc_jars.py
```

This will:
1. Clone Unity Catalog from GitHub (main branch)
2. Patch the build configuration for Scala 2.12 compatibility
3. Compile Unity Catalog components:
   - Server (shaded JAR with all dependencies, ~123MB)
   - Spark connector (~39KB)
   - Client API (~321KB)
4. Copy JARs to `unityCatalogShaded/lib/`

**Options:**
- `--force`: Force regeneration even if JARs already exist
- The script will skip generation if JARs are already present (unless `--force` is used)

### Step 2: Run Integration Tests

```bash
# Run all Unity Catalog integration tests
build/sbt "spark/testOnly *UnityCatalogIntegrationSuite"

# Run a specific test
build/sbt 'spark/testOnly *UnityCatalogIntegrationSuite -- -z "connector should be available"'
```

## What the Tests Do

### Test Infrastructure

**UnityCatalogIntegrationTestUtils.scala**
- Manages embedded Unity Catalog server lifecycle
- Creates Spark sessions configured with Unity Catalog
- Provides helper methods for catalog/schema management
- Handles server cleanup and port allocation

**UnityCatalogIntegrationSuite.scala**
- Test suite with multiple integration tests
- Tests Unity Catalog connector availability
- Tests Delta table operations (CREATE, INSERT, UPDATE, DELETE)
- Tests metadata operations

### Embedded Server Lifecycle

1. **Before All Tests**: 
   - Starts Unity Catalog server on a random available port
   - Creates test catalog (`unity`) and schema (`default`)
   - Creates a SparkSession configured with Unity Catalog and Delta Lake

2. **During Tests**: 
   - Tests execute against the embedded server
   - Each test can create tables, query data, etc.

3. **After All Tests**: 
   - Cleans up database connections
   - Stops the Unity Catalog server
   - Closes SparkSession

## Current Test Results

✅ **All tests passing!**

| Test | Status | Notes |
|------|--------|-------|
| Unity Catalog connector should be available | ✅ PASSED | Confirms UC connection works |
| Create and query Delta table in Unity Catalog | ✅ PASSED | External table with LOCATION |
| UPDATE and DELETE operations | ✅ PASSED | All DML operations work |
| Verify Unity Catalog metadata | ✅ PASSED | DESCRIBE TABLE EXTENDED works |

## Important Notes

### Unity Catalog Requires External Tables

**Unity Catalog does NOT support managed tables.** All tables must be created with explicit `LOCATION` clauses:

```sql
-- ✅ Correct: External table with LOCATION
CREATE TABLE unity.default.my_table (
  id INT,
  name STRING
) USING DELTA
LOCATION '/path/to/table'

-- ❌ Wrong: Managed table without LOCATION  
CREATE TABLE unity.default.my_table (
  id INT,
  name STRING
) USING DELTA
-- Error: "Unity Catalog does not support managed table"
```

This is by design - Unity Catalog follows a storage-agnostic architecture where table metadata is separated from storage. You must always specify where the table data should be stored.

### Version Compatibility

The integration uses:
- **Unity Catalog**: master branch (patched for Scala 2.12)
- **Spark**: 3.5.3 (for UC connector)
- **Delta**: 3.2.1 (for UC connector)
- **Delta Lake tests**: master branch (4.0.0+)

This version combination works because:
1. UC connector is compatible with Delta 3.2.1
2. Delta Lake maintains backward compatibility
3. External tables with explicit locations work across versions

## Architecture

### Build System Integration

The `build.sbt` includes a `unityCatalogShaded` project that:
- Executes the `generate_uc_jars.py` script when needed
- Adds the generated JARs to the test classpath via `unmanagedJars`
- Ensures JARs are available before test compilation

### JAR Generation Process

```
generate_uc_jars.py
    ↓
Clone unitycatalog/unitycatalog (main branch)
    ↓
Patch build.sbt:
  - Add Scala 2.12 support
  - Change Spark version: 4.0.0 → 3.5.3
  - Change Delta version: 4.0.0 → 3.2.1
    ↓
Compile:
  - build/sbt ++2.12.18 spark/package (Spark connector)
  - build/sbt client/package (Client API)
  - build/sbt serverShaded/assembly (Server with all deps)
    ↓
Copy JARs to unityCatalogShaded/lib/
```

### Test Execution Flow

```
Test Suite Start
    ↓
beforeAll()
    ↓
Start UC Server (random port)
    ↓
Create catalog 'unity'
    ↓
Create schema 'unity.default'
    ↓
Create SparkSession with UC config
    ↓
Run Tests
    ↓
afterAll()
    ↓
Stop SparkSession
    ↓
Stop UC Server
```

## Configuration

### Spark Configuration

The test SparkSession is configured with:
```scala
// Unity Catalog
spark.sql.catalog.unity = io.unitycatalog.spark.UCSingleCatalog
spark.sql.catalog.unity.uri = http://localhost:<random-port>
spark.sql.catalog.unity.token = (empty)
spark.sql.catalog.unity.warehouse = <temp-dir>

// Delta Lake
spark.sql.extensions = io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog
```

### Unity Catalog Server

The embedded server uses:
- Random available port (to avoid conflicts)
- In-memory H2 database
- Hibernate for ORM
- Default configuration (no authentication)

## Troubleshooting

### JARs Not Found

If you see errors about missing Unity Catalog classes:
```bash
cd unityCatalogShaded
python3 generate_uc_jars.py --force
```

### Port Already in Use

The server automatically finds an available port. If tests hang, check for orphaned Unity Catalog server processes:
```bash
ps aux | grep unitycatalog
kill <pid>
```

### Compilation Errors

If Unity Catalog compilation fails, check:
- Java version (must be 17+ for UC server compilation)
- Network connection (needs to download dependencies)
- Disk space (server compilation creates large artifacts)

### Scala Version Conflicts

If you see `NoClassDefFoundError: scala/collection/StringOps$`:
- The test is running with Scala 2.13 but JARs are compiled with Scala 2.12
- Regenerate JARs or ensure tests run with Scala 2.12 (default)

## Development

### Adding New Tests

Add tests to `UnityCatalogIntegrationSuite.scala`:

```scala
test("My new test") {
  val tableName = s"$UC_CATALOG.$UC_SCHEMA.my_table"
  
  // Your test code here
  spark.sql(s"CREATE TABLE $tableName ...")
  
  // Cleanup
  try {
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
  } catch {
    case _: Exception => // Ignore cleanup errors
  }
}
```

### Updating Unity Catalog Version

To update to a newer Unity Catalog version:
1. Modify `uc_src_branch` in `generate_uc_jars.py` if needed
2. Run `python3 generate_uc_jars.py --force`
3. Run tests to verify compatibility

### Debugging

Enable debug logging by modifying `UnityCatalogIntegrationTestUtils.scala`:
```scala
// Add more println statements
println(s"Server URL: ${getUCServerUrl()}")
println(s"Catalogs: ${spark.sql("SHOW CATALOGS").collect().mkString(", ")}")
```

## References

- [Unity Catalog GitHub](https://github.com/unitycatalog/unitycatalog)
- [Unity Catalog Documentation](https://docs.unitycatalog.io/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Unity Catalog Spark Connector](https://github.com/unitycatalog/unitycatalog/tree/main/connectors/spark)

## Summary

✅ **Full integration complete!** This setup successfully integrates Unity Catalog master branch with Delta Lake tests. All tests pass, demonstrating:

- Unity Catalog server management (embedded, no external dependencies)
- Catalog and schema operations
- Delta table operations (CREATE, INSERT, SELECT, UPDATE, DELETE)  
- Metadata operations (DESCRIBE TABLE EXTENDED)
- Proper separation of compute and storage (external tables)

The key insight was understanding Unity Catalog's design: it requires external tables with explicit storage locations, which aligns with modern data lake architectures. This provides a robust foundation for testing Delta Lake with Unity Catalog.
