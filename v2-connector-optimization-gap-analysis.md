# Delta V2 Connector: Optimization Gap Analysis & Roadmap

## 0. Purpose & Team Responsibilities

### Document Purpose
This document provides a comprehensive analysis of query optimization capabilities across Delta Lake connectors:
- **PrepareDeltaScan** (OSS Delta V1 Connector)
- **PrepareDeltaScanEdge** (Databricks Runtime Enhancement)
- **V2 Connector** (Delta Kernel-based DSV2 implementation)

The goal is to:
1. **Clarify current state** of optimization features in each connector
2. **Identify performance gaps** in V2 connector
3. **Define project roadmap** to achieve performance parity
4. **Assign team ownership** for implementation work

### Team Responsibilities

**Delta Kernel Team**:
- Provide APIs for statistics aggregation
- Support parallel log replay
- Expose parsed statistics (not JSON strings)
- Enable interruptible scan operations for limit pushdown

**Spark Connector Team**:
- Implement DSV2 optimization interfaces
- Integrate Kernel capabilities into V2 connector
- Ensure feature parity with V1 connector
- Maintain performance benchmarks

**Shared Responsibilities**:
- API design for metadata-only queries
- Performance testing and validation
- Documentation and migration guidance

--------------------------------------------------------------------------

## 1. PrepareDeltaScan (OSS V1 Connector)

### Architecture Overview

PrepareDeltaScan is a **Catalyst Optimizer Rule** that runs during the **Logical Plan optimization phase**, before physical planning begins.

```
Query Execution Pipeline:
  Logical Plan (Analyzer)
       |
       v
  Catalyst Optimizer
       |- Standard Rules (PushDownPredicate, ColumnPruning, etc.)
       +- PrepareDeltaScan  <-- HERE (Optimizer Phase)
            |- Pin snapshot (snapshot isolation)
            |- Call DataSkippingReader
            |- Generate PreparedDeltaFileIndex
            +- Replace FileIndex in plan
       |
       v
  Physical Planning
       |- Use PreparedDeltaFileIndex (files already filtered)
       +- Generate SparkPlan
       |
       v
  Execution
```

### Core Optimizations

#### 1.1 Snapshot Isolation (via Snapshot Pinning)
**Purpose**: Ensure consistent reads within a single query by pinning to a specific snapshot version

**Implementation**: Snapshot Pinning
- **First Access**: When a query first accesses a Delta table, capture and cache the current snapshot
- **Subsequent Access**: Reuse the cached snapshot for all subsequent accesses to the same table within the query
- **Scope**: Per-query, per-table

**Technical Details**:
```scala
// Cache: (TableId, TablePath) -> Snapshot
private val scannedSnapshots = 
  new ConcurrentHashMap[(String, Path), Snapshot]

// Pin on first access, reuse on subsequent accesses
protected def getDeltaScanGenerator(index: TahoeLogFileIndex) = {
  scannedSnapshots.computeIfAbsent(
    index.deltaLog.compositeId,
    _ => index.getSnapshot  // Pin: capture current version
  )
}
```

**Benefit**:
- Self-joins see consistent data (both sides use same snapshot)
- Prevents anomalies from concurrent writes during query execution
- Essential for ACID compliance at query level

----------

#### 1.2 Data Skipping (File Pruning)
**Purpose**: Skip files that cannot contain matching rows

**Supported Techniques**:
1. **Partition Filtering**: Skip files in non-matching partitions
2. **Min/Max Statistics**: Use `stats.minValues`, `stats.maxValues`
3. **Null Count**: Use `stats.nullCount` for `IS NULL`/`IS NOT NULL`
4. **IsNull Expression Expansion**: Rewrite `IsNull(a AND b)` -> `IsNull(a) OR IsNull(b)` to utilize stats more effectively (configurable depth)
5. **InSubquery Handling**: Accept materialized `InSubqueryExec` results (Spark materializes) and apply as IN-list for data skipping
6. **StartsWith Optimization**: Convert `StartsWith(col, 'prefix')` -> `substring(min/max, 0, len) comparison` for stats-based skipping

**Implementation (V1 uses Distributed DataFrame API)**:
```scala
// OSS V1 uses DataFrame operations (distributed execution possible)
def stateReconstruction: Dataset[SingleAction] = {
  loadActions  // Checkpoint + Deltas
    .repartition(50, col("path"))  // Always called! Default 50 partitions
    .sortWithinPartitions(COMMIT_VERSION_COLUMN)
    // InMemoryLogReplay deduplication
}

val filtered = allFiles  // Dataset[AddFile]
  .withColumn("stats", from_json(col("stats"), statsSchema))  // Can run on executors
  .where(partitionFilters)  // Can run on executors
  .where(dataSkippingFilters)  // Can run on executors
  .collect()  // Final collection to driver
```

**When is it actually distributed?**
- **Large tables** (1M+ files): Parallelized across 50 partitions → multiple executors
- **Small tables** (100 files): May run on 1-2 executors despite repartition(50)
- **Local mode**: All runs on driver, but still uses DataFrame engine
- **Cluster mode**: Distributed when data > executors × partition size

**Example**:
```sql
SELECT * FROM employees WHERE salary > 100000

-- DataSkippingReader (Distributed):
-- 1. Executors: Parse stats for each file
-- 2. Executors: Skip files where maxValues.salary <= 100000
-- 3. Driver: Collect filtered results
```

**Performance**: Can reduce files scanned by 90%+ with good statistics

----------

#### 1.3 Limit Pushdown
**Purpose**: Stop scanning early when limit is reached

**Constraint**: Only works when **all filters are on partition columns**

**Implementation**:
```scala
// PrepareDeltaScan.scala
case LocalLimit(IntegerLiteral(limit),
  PhysicalOperation(_, filters, delta))
    if containsPartitionFiltersOnly(filters) =>
  scanGenerator.filesForScan(limit, filters)

// DataSkippingReader.scala
protected def pruneFilesWithIterator(
    withNumRecords: Iterator[(AddFile, NumRecords)],
    limit: Long): ScanAfterLimit = {
  
  var logicalRowsToScan = 0L
  val filesToScan = ArrayBuffer[AddFile]()
  
  while (iter.hasNext && logicalRowsToScan < limit) {
    val (file, numRecords) = iter.next()
    logicalRowsToScan += numRecords.numLogicalRecords
    filesToScan += file
  }
  // Stop early - don't scan remaining files!
}
```

**Why "Partition Filters Only" Constraint?**

The algorithm cumulates `numRecords` from file statistics to stop early. This only works correctly when:
1. **No data-level filtering needed**: All filters can be evaluated at file level
2. **Accurate row counts**: `numRecords` reflects actual rows that will be returned

**Example - Partition Filter Only** ✅:
```sql
-- Safe for Limit Pushdown
SELECT * FROM orders WHERE date = '2024-01-01' LIMIT 1000

Execution:
- Filter partitions: date=2024-01-01 (eliminate other dates)
- File 1 (date=2024-01-01): 500 rows (ALL match!)
- File 2 (date=2024-01-01): 300 rows (ALL match!)
- File 3 (date=2024-01-01): 400 rows (ALL match!)
  → Cumulative: 500 + 300 + 400 = 1200 ≥ 1000 ✓ STOP!
- Result: Read 3 files, get 1000 rows (correct!)
```

**Example - Data Column Filter** ❌:
```sql
-- UNSAFE for Limit Pushdown
SELECT * FROM orders 
WHERE date = '2024-01-01' AND amount > 100 
LIMIT 1000

Problem with naive pushdown:
- File 1 (date=2024-01-01): numRecords=500
  → But only 50 rows have amount > 100!
- File 2 (date=2024-01-01): numRecords=300
  → But only 30 rows have amount > 100!
- File 3 (date=2024-01-01): numRecords=400
  → But only 20 rows have amount > 100!

Naive algorithm (WRONG):
  Cumulative: 500 + 300 = 800 < 1000, continue
              800 + 400 = 1200 ≥ 1000 → STOP at File 3
  Read 3 files → Get only 50 + 30 + 20 = 100 rows!
  Expected: 1000 rows
  Bug: Returned 900 rows less! ❌

Correct behavior (without pushdown):
  Read all files (1-10), filter each, accumulate until 1000 rows
  Need to read 10+ files to get 1000 matching rows ✓
```

**Technical Reason**:
```
Limit Pushdown Algorithm:
  1. Filter files by partition (eliminates entire files)
  2. Iterate files in order
  3. Accumulate numRecords from stats.numRecords
  4. Stop when sum(numRecords) >= limit

This works ONLY when:
  ✓ numRecords accurately represents rows that will be returned
  ✓ Every row in the file passes all filters
  
Partition filters guarantee this because:
  - Partition filtering happens at FILE level (include/exclude entire file)
  - If file is included, ALL rows match the filter
  - Therefore: stats.numRecords = actual rows returned
  
Data column filters break this because:
  - Filtering happens at ROW level (within each file)
  - stats.numRecords = total rows in file
  - actual rows returned = unknown until file is read
  - Therefore: Cannot use stats.numRecords for limit calculation
```

**Workaround for data filters**: Use min/max statistics (future work)
```scala
// Possible enhancement (not implemented in OSS)
if (filter: amount > 100 && stats.maxValues.amount > 100) {
  // File MIGHT have rows, but we don't know how many
  // Would need to estimate: numRecords * selectivity
  // Too complex and error-prone, so OSS doesn't do it
}
```

**Performance**: Can reduce files scanned by orders of magnitude (when applicable)

----------

#### 1.4 Metadata-Only Query Optimization
**Purpose**: Answer queries using only Delta Log statistics, without reading data files

**Supported Queries**:
```sql
-- COUNT(*), MIN, MAX without filters/GROUP BY
SELECT COUNT(*), MIN(age), MAX(salary) FROM employees

-- Execution:
-- 1. Read Delta Log
-- 2. Aggregate stats.numRecords, stats.minValues, stats.maxValues
-- 3. Return LocalRelation with computed values
-- Result: No data file reads!
```

**Constraints**:
- No WHERE clause (or only partition filters)
- No GROUP BY clause
- Only COUNT(*), MIN(column), MAX(column)
- All files must have statistics
- No Deletion Vectors (or special handling)
- Supported data types: Numeric, Date (not Decimal, Timestamp)

**Performance**: 1000x+ speedup (seconds vs minutes for TB tables)

----------

#### 1.5 Generated Column Optimization
**Purpose**: Automatically generate partition filters from data column filters

**Example**:
```sql
CREATE TABLE events (
  eventTime timestamp,
  day date GENERATED ALWAYS AS (DATE(eventTime))
) PARTITIONED BY (day)

-- User query:
SELECT * FROM events 
WHERE eventTime >= '2024-01-01' AND eventTime < '2024-01-02'

-- PrepareDeltaScan auto-generates:
WHERE eventTime >= '2024-01-01' AND eventTime < '2024-01-02'
  AND day = '2024-01-01'  <-- Added automatically!
```

**Implementation**:
```scala
GeneratedColumn.generatePartitionFilters(
  snapshot, dataFilters, delta
) = {
  // For each data filter, derive partition filters
  dataFilters.flatMap {
    case GreaterThanOrEqual(eventTime, lit) =>
      // eventTime >= '2024-01-01' => day >= '2024-01-01'
      partitionExpr.greaterThanOrEqual(lit)
  }
}
```

**Performance**: Can prune 99%+ of partitions for time-based queries

----------

#### 1.6 Query Plan Canonicalization
**Purpose**: Avoid duplicate file listings for structurally identical queries

**Implementation**:
```scala
val deltaScans = new HashMap[LogicalPlan, DeltaScan]()

val preparedScan = deltaScans.getOrElseUpdate(
  plan.canonicalized,  // Normalized key (ignores column order, etc.)
  filesForScan(...)
)
```

**Benefit**: Reuse file listings for queries that differ only in projection order

----------

### Summary: PrepareDeltaScan Feature Matrix

| Feature | Supported | Constraints | Performance Impact |
|---------|-----------|-------------|-------------------|
| Snapshot Isolation | ✅ | Always enabled | Correctness |
| Partition Filtering | ✅ | Requires partition columns | High (90%+ reduction) |
| Min/Max Skipping | ✅ | Requires statistics | High (50-90% reduction) |
| Limit Pushdown | ✅ | Partition filters only | High (early termination) |
| Metadata-Only Query | ✅ | No filters/GROUP BY | Extreme (1000x+) |
| Generated Column | ✅ | Requires feature enabled | High (partition pruning) |
| TopN Pushdown | ❌ | Not implemented | - |
| Offset Pushdown | ❌ | Not implemented | - |

--------------------------------------------------------------------------

## 2. PrepareDeltaScanEdge (Databricks Runtime)

### Key Enhancements Over OSS

PrepareDeltaScanEdge builds on PrepareDeltaScan with **parallel execution** and **V2 Checkpoint optimization**.

----------

### 2.1 Parallel Query Planning (Multiple Scans)

**Note**: Both OSS and DBR use distributed processing (via DataFrames) for individual scans. The difference is in handling **multiple scans in the same query**.

**OSS Limitation**: Processes multiple Delta scans sequentially
- Query with 4 Delta table scans: processes scan1, then scan2, then scan3, then scan4
- Each scan is distributed internally, but scans are serialized

**DBR Solution**: Parallel execution across multiple Delta scans
- Query with 4 Delta table scans: processes all 4 scans in parallel (using thread pool)
- Each scan is still distributed internally, AND scans run concurrently

**Implementation**:
```scala
// Pseudo-code
val scanTasks = deltaScans.map { scan =>
  Future {
    // Each scan processed in parallel
    scan.filesForScan(filters)
  }
}

ExecutorService.invokeAll(scanTasks)
```

**Architecture**:
```
+------------------------------------------------+
|          Thread Pool (4 threads)               |
+------------------------------------------------+
|  Thread 1  Thread 2  Thread 3  Thread 4       |
|     |          |         |         |          |
|  Scan A    Scan B    Scan C    Scan D         |
|  Parse     Parse    Parse    Parse            |
|  Stats     Stats    Stats    Stats            |
|  Filter    Filter   Filter   Filter           |
|  collect() collect() collect() collect()      |
|     +----------+---------+---------+          |
|                  |                             |
|            Await All                           |
|                  |                             |
|         Merge Results & Metrics                |
+------------------------------------------------+
```

**Performance Gain**: 3-4x faster planning for queries with multiple scans

----------

### 2.2 V2 Checkpoint Format Support

**Problem with OSS**: Statistics stored as JSON strings in checkpoints
```scala
// V1 Checkpoint Schema
add: struct {
  path: string
  size: long
  stats: string  // JSON string - can't pushdown filters!
}
```

**DBR Solution**: V2 Checkpoint with structured statistics
```scala
// V2 Checkpoint Schema
add: struct {
  path: string
  size: long
  stats_parsed: struct {  // Structured columns!
    numRecords: long
    minValues: struct { age: int, salary: long, ... }
    maxValues: struct { age: int, salary: long, ... }
    nullCount: struct { age: long, salary: long, ... }
  }
  partitionValues_parsed: struct {  // Direct partition access
    date: string
    department: string
  }
}
```

**Advantages**:
1. **Parquet Predicate Pushdown**: Filter directly on statistics
   ```scala
   checkpoint.filter("stats_parsed.maxValues.age > 50")
   // Parquet skips row groups where maxValues.age <= 50
   ```

2. **Column Pruning**: Read only needed statistics columns
   ```scala
   checkpoint.select(
     "path", 
     "stats_parsed.numRecords",
     "stats_parsed.minValues.salary"
   )
   // Don't read other statistics
   ```

3. **Reduced JSON Parsing**: No need to parse JSON for every file

----------

### 2.3 DataSkippingReaderV2

**Key Innovation**: Direct Parquet checkpoint reading + distributed log replay

**Architecture**:
```
V1 (OSS) - Read & Process Flow:

Step 1: Read Checkpoint        Step 2: Read Deltas          Step 3: Merge & Replay
+------------------+           +------------------+         +----------------------+
| Load Checkpoint  |  Union    | Load JSON Deltas |  Union  | DataFrame Replay     |
| - 1 Parquet file |  ----->   | - 1000 files     |  -----> | - repartition(50)    |
| - 1M AddFile     |           | - 100K AddFile   |         |   (always called)    |
| - stats=JSON str |           | - Parse each     |         | - sortWithinParts    |
+------------------+           +------------------+         | - InMemoryLogReplay  |
                                                            | - Parse JSON stats   |
                                                            +----------+-----------+
                                                            | Parallelized if:     |
                                                            | - Large data (1M+)   |
                                                            | - Cluster mode       |
                                                            +----------+-----------+
                                                                       |
                                                                       v
                                                            +---------------------+
                                                            | File List (1.1M)    |
                                                            | - collect() to drv  |
                                                            | - Stats parsed      |
                                                            +---------------------+

V2 (DBR Edge) - Optimized Flow:

Step 1: Smart Checkpoint Read   Step 2: Merge Deltas        Step 3: Efficient Replay
+------------------------+      +------------------+        +----------------------+
| V2 Checkpoint          |      | JSON Deltas      |        | Deferred Collection  |
| - Structured stats!    | Mrg  | - Only 10 files  |  Mrg   | - Distributed dedup  |
| - Predicate pushdown   | ---> | - Recent changes | -----> | - Lazy evaluation    |
| - Read ONLY 10K rows   |      +------------------+        | - Aggregation on DF  |
|   (filtered by stats)  |                                  +----------+-----------+
+------------------------+                                             |
                                                                       v
                                                            +---------------------+
                                                            | File List (11K)     |
                                                            | - 100x less data    |
                                                            | - Less driver mem   |
                                                            +---------------------+

Key: ---> = Data flow (read, then merge, then process)
     Union/Merge = DataFrame union operation
```

**Performance Comparison (Large Table: 1M files, 1000 delta commits)**:
```
Step                   V1 (OSS)              V2 (DBR Edge)       Improvement
----------------------------------------------------------------------------------
Step 1: Checkpoint     Read 1M rows (2s)     Read 10K rows (0.2s)   10x faster
        Stats          JSON strings          Structured Parquet     No parsing
        Filtering      After read            During read (pushdown) Earlier filter

Step 2: Deltas         Read 1000 files (15s) Read 10 files (1s)     15x faster
        Reason         All since checkpoint  Recent only            Fewer files

Step 3: Replay         1.1M rows (8s)        11K rows (1s)          8x faster
        Dedup          Eager collect         Distributed dedup      Less driver mem
        Stats Parse    from_json (5s)        Already parsed (0s)    No overhead
----------------------------------------------------------------------------------
Total Time             30s                   ~2s                     15x faster!
Driver Memory          High (1.1M rows)      Low (can stream)       100x less
```

**Note on "Distributed"**:
- **API**: V1 always uses distributed DataFrame API (`repartition(50)` is always called)
- **Execution**: Actual parallelization depends on:
  ```
  Small table (100 files):
    - repartition(50) called → creates 50 partitions
    - But only 2-3 partitions have data
    - Runs on 2-3 executors (or locally)
  
  Large table (1M files):
    - repartition(50) called → creates 50 partitions
    - All 50 partitions have data (~20K files each)
    - Runs on all available executors
    - True distributed parallelization
  
  Local mode (spark.master = "local[*]"):
    - Still uses DataFrame engine (optimized)
    - But all runs on driver JVM threads
  ```
- **Key Point**: The code is written for distributed execution, and Spark automatically scales based on data size and cluster configuration

----------

### 2.4 Enhanced Metadata-Only Queries

**Additional Support Beyond OSS**:

1. **Partition Column Aggregations**:
   ```sql
   -- Table partitioned by date, department
   SELECT MAX(date), MIN(date) FROM employees
   SELECT DISTINCT department FROM employees
   SELECT MAX(date) FROM employees GROUP BY department
   ```

2. **Mixed Queries** (GROUP BY partition + aggregate data columns):
   ```sql
   SELECT date, MAX(salary), COUNT(*) 
   FROM employees 
   GROUP BY date
   WHERE date >= '2024-01-01'
   ```

3. **Better DV Handling**: Account for deletion vectors in COUNT queries
   ```scala
   val numLogicalRecords = col("stats.numRecords") - 
                          coalesce(col("deletionVector.cardinality"), lit(0))
   ```

----------

### 2.5 Enhanced Distributed Processing

**Context**: Both OSS and DBR Edge use distributed processing via DataFrames, but DBR Edge optimizes this further.

#### OSS Approach (Distributed with Eager Collection):
```scala
// Step 1: Distributed filtering (on executors)
val filtered = allFiles
  .where(partitionFilters)
  .withColumn("stats", from_json(col("stats"), statsSchema))  // Distributed JSON parsing
  .where(dataSkippingFilters)

// Step 2: Eager collection to driver
val files = filtered.as[AddFile].collect()  // All results to driver memory

// Step 3: Final processing in driver
val deduplicated = deduplicateInDriver(files)
val preparedIndex = PreparedDeltaFileIndex(deduplicated)
```

#### DBR Edge Enhancement (Distributed with Deferred Collection):
```scala
// Step 1: Better distributed filtering with V2 Checkpoint
val filtered = v2WithStats  // V2 checkpoint has structured stats
  .select("path", "stats_parsed.numRecords")  // Column pruning (less I/O)
  .filter("stats_parsed.maxValues.age > 50")  // Parquet predicate pushdown

// Step 2: Distributed deduplication (on executors)
val replayed = logReplayDistributed(filtered)  // Still DataFrame, not collected

// Step 3: Deferred collection - only when needed
if (isMetadataQuery) {
  // Aggregate on DataFrame, might not collect at all
  replayed.agg(sum("numRecords"))
} else if (hasLimit) {
  // Collect only necessary rows
  replayed.limit(limit).collect()
} else {
  // Normal collection
  replayed.collect()
}
```

**Key Improvements**:
1. **V2 Checkpoint Optimization**: Structured stats enable Parquet-level predicate pushdown
2. **Deferred Collection**: Avoid pulling all data to driver prematurely
3. **Distributed Deduplication**: Remove/add reconciliation runs on executors
4. **Lower Driver Memory**: Results can be streamed or aggregated without full materialization

**Example - Memory Comparison**:
```
Large table: 1M files, 100GB of metadata

OSS:
  - Executors filter to 100K files
  - collect() brings 10GB to driver
  - Driver deduplicates in memory
  - Peak driver memory: 10GB+

DBR Edge:
  - Executors filter to 100K files
  - Executors deduplicate (distributed)
  - collect() brings 50K files (5GB)
  - Peak driver memory: 5GB
  
Benefit: 50% reduction in driver memory pressure
```

----------

### Summary: PrepareDeltaScanEdge Enhancements

| Feature | OSS | DBR | Improvement |
|---------|-----|-----|-------------|
| Parallel Planning | ❌ | ✅ | 3-4x faster |
| V2 Checkpoint | ❌ | ✅ | 6-15x faster stats |
| Parquet Pushdown | ❌ | ✅ | Significant I/O reduction |
| Metadata Query (Partition) | ❌ | ✅ | 1000x+ for partition aggs |
| Enhanced Distributed Processing | ✅ Basic | ✅ Optimized | 50% lower driver memory |

--------------------------------------------------------------------------

## 3. Current V2 Connector Status

### Architecture: DSV2 vs Catalyst Integration

**V1 (PrepareDeltaScan)**: Integrates at **Optimizer phase**
```
Logical Plan
   |
   v
Catalyst Optimizer
   +- PrepareDeltaScan  <-- File pruning happens HERE
   |
   v
Physical Planning (files already filtered)
```

**V2 (Kernel)**: Integrates at **Physical Planning phase**
```
Logical Plan
   |
   v
Catalyst Optimizer (no Delta-specific rules)
   |
   v
Physical Planning
   +- Table.newScanBuilder()  <-- File pruning happens HERE
   |
   v
Execution
```

**Implication**: V2 misses **Optimizer-phase** Delta optimizations, but some happen later:

| Optimization | V1 (Optimizer) | V2 (Later Phases) | Impact |
|--------------|----------------|-------------------|--------|
| **Dynamic Filtering** | ❌ Not in Optimizer | ✅ Execution (SupportsRuntimeV2Filtering) | Late, but works |
| **Static Data Skipping** | ✅ PrepareDeltaScan | ✅ Physical Planning (pushFilters) | Same timing |
| **Metadata-Only Queries** | ✅ OptimizeMetadataOnlyDeltaQuery (Optimizer) | ✅ Can do via SupportsPushDownAggregates (Physical Planning) | Not implemented yet |
| **Limit Pushdown** | ✅ PrepareDeltaScan (early stop) | ❌ No API | Missing! |
| **Generated Column** | ✅ PrepareDeltaScan | ❌ Not integrated | Missing! |
| **Plan Canonicalization** | ✅ PrepareDeltaScan | ❌ No LogicalPlan access | Missing! |

**Why Some Optimizations Must Happen in Optimizer Phase?**

1. **Metadata-Only Queries** - ✅ CAN be done in Physical Planning (I was wrong!):
   ```sql
   SELECT COUNT(*) FROM orders
   
   V1 approach (Optimizer):
   - OptimizeMetadataOnlyDeltaQuery rule
   - Replace entire Scan node with LocalRelation
   - Happens in Optimizer phase
   
   V2 approach (Physical Planning - via DSV2 API):
   - Implement SupportsPushDownAggregates
   - When Spark pushes COUNT(*)/MIN/MAX aggregation:
     public boolean pushAggregation(Aggregation agg) {
       // Compute from Kernel statistics
       this.precomputedResult = computeFromStats(agg);
       return true;  // Tell Spark we handle it
     }
   - Return pre-computed results in Batch
   - ✅ Different phase, but functionally equivalent!
   
   Status: V2 Connector hasn't implemented this yet (but API exists)
   ```

2. **Limit Pushdown** - Must be in Optimizer:
   ```scala
   // Need to see LocalLimit + Scan together in logical plan
   case LocalLimit(limit, Filter(..., Scan(...))) =>
     scanGenerator.filesForScan(limit, filters)  // Early termination
   
   V1: Optimizer sees full pattern → Integrated planning
   V2: ScanBuilder only sees filters, not limit → Cannot coordinate
   ```

3. **Dynamic Filtering** - Can happen later:
   ```java
   // DSV2 has specific API for this
   interface SupportsRuntimeV2Filtering {
     void filter(Predicate[] predicates);  // Called at Execution
   }
   
   V1: Doesn't do this in Optimizer (happens at execution too)
   V2: ✅ Works the same way (via DSV2 API)
   ```

4. **Static Data Skipping** - Can happen in Physical Planning:
   ```java
   // DSV2 has API for static filters
   interface SupportsPushDownFilters {
     Filter[] pushFilters(Filter[] filters);  // Called at Physical Planning
   }
   
   V1: Optimizer phase via PrepareDeltaScan
   V2: ✅ Physical Planning phase (slightly later but still effective)
   ```

**Summary**: 
- ✅ **Can compensate with DSV2 APIs**: 
  - Static filtering (SupportsPushDownFilters)
  - Runtime pruning (SupportsRuntimeV2Filtering)
  - **Metadata-only queries** (SupportsPushDownAggregates) ← Can implement!
- ❌ **Cannot compensate** (no DSV2 API): 
  - Limit coordination (no pushLimit API)
  - Plan canonicalization (no LogicalPlan access)
  - Generated column integration (requires Optimizer-level analysis)

----------

### 3.1 Currently Implemented Features

#### ✅ Basic Data Skipping
```java
// SparkScanBuilder.java
public class SparkScanBuilder implements 
    SupportsPushDownFilters,
    SupportsPushDownRequiredColumns {
    
  @Override
  public Filter[] pushFilters(Filter[] filters) {
    // Convert Spark filters to Kernel predicates
    // Kernel applies min/max skipping internally
  }
}
```

**What works**:
- Partition filtering
- Min/Max statistics-based skipping (via Kernel)
- Column pruning

**Limitations (Data Skipping Optimizations)**:
- **No IsNull expansion**: Kernel supports `IS_NULL` expression but doesn't expand `IsNull(And(...))` or `IsNull(Or(...))` into disjunctive forms for better statistics utilization (10-20% more files scanned in edge cases)
- **No StartsWith optimization**: Kernel supports `STARTS_WITH` expression but doesn't convert it to `substring(min/max, 0, len) comparison` for stats-based skipping (10-30% more files scanned for prefix queries)
- Kernel's log replay doesn't expose parallelization APIs (vs. OSS DataFrame-based approach)

**Note on InSubquery**: V2 handles subquery materialization the same as V1 (Spark automatically materializes `InSubqueryExec`). The real performance gap comes from the lack of **Limit Pushdown** to stop scanning early when combined with IN filters.

----------

#### ✅ Runtime Filtering (Dynamic Pruning)
```java
// SparkScan.java
public class SparkScan implements 
    SupportsRuntimeV2Filtering {
    
  @Override
  public NamedReference[] filterAttributes() {
    // Return partition columns for runtime filtering
    return partitionColumns;
  }
  
  @Override
  public void filter(Predicate[] predicates) {
    // Filter already-planned files with runtime predicates
    // (e.g., from JOIN)
  }
}
```

**Note**: Runtime filtering happens **after** static planning

**Example explaining the limitation**:
```sql
-- Query with static + dynamic filters
SELECT o.* 
FROM orders o  -- 10K files total
JOIN high_value_customers hv ON o.customer_id = hv.id
WHERE o.date = '2024-01-01'  -- Static filter

Execution Flow:

Planning Phase:
  1. pushFilters([date='2024-01-01'])  -- Static filter pushed
  2. kernelScan.getScanFiles(engine)
     - Log replay (5s)
     - Data skipping with date filter
     - Result: 1000 files (90% pruned!) ✓
  3. Planning cost: 5s for 1000 files

Execution Phase:
  1. Execute hv table (small)
  2. Broadcast customer_ids: [1, 5, 9, ...]
  3. scan.filter([customer_id IN (1,5,9,...)])  -- Runtime filter
     - Filter the 1000 already-planned files
     - Result: 100 files (90% pruned again!) ✓
  4. Read 100 files (1s)

Result:
  ✅ Static pruning: 10K → 1K files (saved planning cost!)
  ✅ Runtime filtering: 1K → 100 files (saved I/O cost!)
  ❌ But planning still processed 1K files (not 100)
  
Ideal (if both filters known at planning):
  - Apply date + customer_id filters together
  - Log replay with combined filter
  - Result: 100 files directly
  - Planning cost: 0.5s (10x less!)
  
The Gap:
  - Current: Planning 5s (1K files) + Execution 1s = 6s
  - Ideal: Planning 0.5s (100 files) + Execution 1s = 1.5s
  - Difference: 4.5s wasted in planning 900 unnecessary files
```

**Key Point**: Static pruning works well, but dynamic filters arrive too late to further reduce planning cost

----------

#### ✅ Statistics Reporting (Partial)
```java
@Override
public Statistics estimateStatistics() {
  return new Statistics() {
    public OptionalLong sizeInBytes() {
      return OptionalLong.of(totalBytes);  // Available
    }
    
    public OptionalLong numRows() {
      return OptionalLong.empty();  // NOT available!
    }
  };
}
```

**Problem**: Kernel has `numRecords` but doesn't expose aggregated statistics

----------

### 3.2 NOT Implemented Features

#### ❌ Limit Pushdown
**Why not implemented**: 
- V2 connector doesn't implement `SupportsPushDownLimit`
- Kernel cannot stop log replay early

**Impact**: LIMIT queries still scan many unnecessary files

----------

#### ❌ TopN Pushdown
**Why not implemented**:
- V2 connector doesn't implement `SupportsPushDownTopN`
- Would require file sorting by statistics

**Impact**: ORDER BY ... LIMIT queries don't benefit from file ordering

----------

#### ❌ Offset Pushdown
**Why not implemented**: Low priority (limited benefit)

----------

#### ❌ Metadata-Only Query Optimization
**Why not implemented** (but CAN be implemented):
- ✅ DSV2 has `SupportsPushDownAggregates` API (available since Spark 3.2.0)
- ⚠️ V2 Connector hasn't implemented it yet
- ⚠️ Kernel doesn't expose aggregated statistics in a convenient way (needs iteration)

**How it COULD work**:
1. Implement `SupportsPushDownAggregates` in `SparkScanBuilder`
2. When receiving COUNT(*)/MIN/MAX, iterate Kernel's scan files
3. Aggregate statistics from `DataFileStatistics` 
4. Return pre-computed results in custom Batch

**Impact**: COUNT(*)/MIN/MAX queries read data files instead of just metadata (600x slower)

**Example**:
```sql
SELECT COUNT(*) FROM huge_table

-- V1: 1 second (read Delta Log only)
-- V2: 10 minutes (full table scan)
-- 600x slower!
```

----------

#### ❌ Generated Column Optimization
**Why not implemented**: Not integrated with V2 connector

**Impact**: Queries on generated partition columns don't get auto-derived filters

----------

#### ❌ Parallel Query Planning (Multiple Scans)
**Why not implemented**: 
- V2 connector doesn't implement parallel processing for multiple Delta scans
- DSV2 architecture processes scans at Physical Planning phase (no cross-scan optimization)
- Would require connector-level thread pool management (not currently implemented)

**Note**: Individual scans can be distributed (via Kernel), but multiple scans in the same query are processed sequentially

**Impact**: Planning phase 3-4x slower for queries with multiple Delta table scans

----------

#### ❌ Query Plan Canonicalization
**Why not implemented**: 
- DSV2 doesn't pass LogicalPlan to data sources
- `newScanBuilder()` only receives options, no plan context
- Cannot compute `plan.canonicalized` to identify duplicate scans
- Each `ScanBuilder` is independent, cannot share state

**Possible workaround**:
- Implement snapshot-level caching in `SparkTable` (avoids duplicate log replay)
- But cannot deduplicate data skipping computation (no plan information)

**Full solution requires**: DSV2 API changes to pass PlanContext

**Impact**: Duplicate scans in same query recalculate file lists (2x slower for self-joins)

----------

### 3.3 Kernel API Limitations

The following limitations in Kernel prevent V2 connector from implementing optimizations:

| Missing Capability | Impact on V2 Connector |
|-------------------|------------------------|
| Aggregated statistics (numRows, min/max across all files) | Cannot implement metadata-only queries |
| Interruptible scan (stop at limit) | Cannot optimize LIMIT pushdown |
| Parallel log replay API | Cannot parallelize planning |
| Parsed statistics exposure (not JSON) | Must manually parse JSON, inefficient |
| Limit-aware scan builder | Cannot tell Kernel to stop early |
| File-level statistics in scan result | Hard to implement TopN sorting |
| Advanced data skipping optimizations (IsNull expansion, StartsWith conversion) | Slightly less effective data skipping (10-30% more files scanned in specific edge cases with complex null/prefix predicates) |

--------------------------------------------------------------------------

## 4. Gap Analysis & Project Roadmap

### 4.1 Performance Impact Summary

| Missing Feature | Query Impact | Frequency | Priority |
|----------------|--------------|-----------|----------|
| Metadata-Only Query | **1000x slower** | Common (BI dashboards) | 🔴 Critical |
| Limit Pushdown | **10-100x slower** | Very Common (pagination, sampling) | 🔴 Critical |
| Parallel Planning | **3-4x slower** | Common (complex queries) | 🟡 High |
| Generated Column | **10-100x slower** | Medium (time-series tables) | 🟡 High |
| TopN Pushdown | **2-10x slower** | Medium (sorted reports) | 🟢 Medium |
| Query Plan Cache | **2x slower** | Low (self-joins) | 🟢 Medium |

----------

### 4.2 Project 1: Metadata-Only Query Optimization (P0)

**Objective**: Enable COUNT(*)/MIN/MAX queries to run against Delta Log only

**Owner**: Shared (Kernel + Connector teams)

#### Kernel Work:
1. **Add aggregated statistics API**:
   ```java
   interface Scan {
     Optional<AggregatedStatistics> getAggregatedStatistics(
       Engine engine, 
       AggregationRequest request
     );
   }
   
   class AggregatedStatistics {
     long totalRecords;
     Map<String, Object> minValues;
     Map<String, Object> maxValues;
   }
   ```

2. **Optimize implementation**:
   - For V2 checkpoints: direct Parquet aggregation
   - Parallel processing across partitions
   - Cached results for repeated calls

#### Connector Work:
1. **Implement `SupportsPushDownAggregates`**:
   ```java
   public class SparkScanBuilder implements 
       SupportsPushDownAggregates {
       
     @Override
     public boolean pushAggregation(Aggregation agg) {
       if (canOptimizeWithMetadata(agg)) {
         this.pushedAgg = agg;
         return true;
       }
       return false;
     }
   }
   ```

2. **Create `MetadataOnlyBatch`**:
   - Return pre-computed aggregation result
   - No data file reads

**Success Criteria**:
- COUNT(*) queries on 1TB table: < 5 seconds (vs. current 10+ minutes)
- Parity with V1 metadata-only performance

**Timeline**: 2-3 months

----------

### 4.3 Project 2: Limit Pushdown (P0)

**Objective**: Stop scanning files early when LIMIT is reached

**Owner**: Shared (Kernel + Connector teams)

#### Kernel Work:
1. **Add limit-aware scan builder**:
   ```java
   interface ScanBuilder {
     ScanBuilder withLimit(long limit);
   }
   ```

2. **Implement interruptible log replay**:
   - Track cumulative row count during replay
   - Stop when `rowCount >= limit`
   - Requires parsing `stats.numRecords` during replay

3. **Handle edge cases**:
   - Files without statistics
   - Deletion vectors (logical vs physical rows)

#### Connector Work:
1. **Implement `SupportsPushDownLimit`**:
   ```java
   @Override
   public boolean pushLimit(int limit) {
     this.limit = limit;
     return true;
   }
   
   @Override
   public boolean isPartiallyPushed() {
     return true;  // Spark will apply limit again
   }
   ```

2. **Pass limit to Kernel**:
   ```java
   kernelScanBuilder.withLimit(limit);
   ```

**Success Criteria**:
- LIMIT queries scan only necessary files (not all files)
- Performance parity with V1 limit pushdown

**Timeline**: 2-3 months

----------

### 4.4 Project 3: Parallel Query Planning for Multiple Scans (P1)

**Objective**: Enable parallel processing of multiple Delta table scans in a single query

**Context**: Individual scans already use distributed processing. This project adds parallelization **across multiple scans**.

**Owner**: Connector team (with optional Kernel enhancements)

#### Connector Work (Primary):
1. **Implement parallel scan builder orchestration**:
   ```java
   public class ParallelScanCoordinator {
     private final ExecutorService threadPool;
     
     public List<Scan> buildScansInParallel(
         List<ScanBuilder> builders) {
       // Submit all scan builders to thread pool
       List<Future<Scan>> futures = builders.stream()
         .map(builder -> threadPool.submit(() -> builder.build()))
         .collect(Collectors.toList());
       
       // Wait for all to complete
       return futures.stream()
         .map(Future::get)
         .collect(Collectors.toList());
     }
   }
   ```

2. **Integration considerations**:
   - Thread pool sizing (default: 4 threads)
   - Exception handling across parallel builds
   - Metrics aggregation

#### Kernel Work (Optional Enhancement):
1. **Provide thread-safe guarantees** for concurrent ScanBuilder usage
2. **Cache optimization** for parallel access to same snapshot
3. **Resource pooling** for Engine instances

**Success Criteria**:
- Queries with 4+ Delta scans: 3-4x faster planning
- Thread-safe operation with no correctness issues
- Configurable parallelism level

**Timeline**: 2-3 months

----------

### 4.5 Project 4: Generated Column Integration (P1)

**Objective**: Auto-generate partition filters from data filters

**Owner**: Connector team

#### Work Items:
1. **Parse generated column metadata** from Kernel
2. **Implement filter derivation logic** (similar to V1):
   ```java
   // User query: eventTime >= '2024-01-01'
   // Auto-generate: day >= '2024-01-01'
   ```
3. **Add derived filters to Kernel scan**

**Success Criteria**:
- Time-range queries on generated partition columns achieve 90%+ partition pruning

**Timeline**: 1-2 months

----------

### 4.6 Project 5: TopN Pushdown (P2)

**Objective**: Optimize ORDER BY ... LIMIT queries using file statistics

**Owner**: Connector team

#### Work Items:
1. **Implement `SupportsPushDownTopN`**
2. **File sorting by statistics**:
   ```java
   // ORDER BY salary DESC LIMIT 100
   // Sort files by maxValues.salary DESC
   // Scan only top files until rowCount >= 100 * overselection_factor
   ```
3. **Handle multi-column sorts**

**Success Criteria**:
- TopN queries scan 50-90% fewer files (depending on data distribution)

**Timeline**: 2 months

----------

### 4.7 Project 6: Enhanced Statistics Exposure (P1)

**Objective**: Make Kernel statistics easier to consume

**Owner**: Kernel team

#### Work Items:
1. **Add parsed statistics to scan result**:
   ```java
   // Instead of JSON string in AddFile
   // Return structured statistics
   class ScanFile {
     AddFile file;
     ParsedStatistics stats;  // numRecords, min, max, nullCount
   }
   ```

2. **V2 Checkpoint optimization**:
   - Detect V2 checkpoint format
   - Use structured columns directly
   - Avoid JSON parsing

**Success Criteria**:
- No JSON parsing overhead in V2 connector
- Direct access to numRecords for all use cases

**Timeline**: 2-3 months

----------

### 4.8 Dependencies & Sequencing

```
Timeline (Quarters):

Q1 2026:
  - [Kernel] Aggregated Statistics API (P0)
  - [Connector] Metadata-Only Query (P0)
  - [Connector] Generated Column (P1)

Q2 2026:
  - [Kernel] Limit-Aware ScanBuilder (P0)
  - [Connector] Limit Pushdown (P0)
  - [Kernel] Enhanced Statistics (P1)

Q3 2026:
  - [Kernel] Parallel Log Replay (P1)
  - [Connector] TopN Pushdown (P2)

Q4 2026:
  - Performance validation
  - Edge case handling
  - Production rollout
```

**Critical Path**:
1. Metadata-Only Query (P0) - Biggest performance gap
2. Limit Pushdown (P0) - Very common use case
3. Parallel Planning (P1) - Improves all queries
4. Other optimizations (P2+) - Nice to have

----------

### 4.9 Success Metrics

**Performance Benchmarks**:
| Query Type | Target Performance | Current V2 | Gap |
|------------|-------------------|------------|-----|
| COUNT(*) | < 5s | > 600s | 120x |
| MIN/MAX | < 5s | > 300s | 60x |
| LIMIT 1000 | < 10s | > 60s | 6x |
| Complex Planning | < 30s | > 120s | 4x |

**Acceptance Criteria**:
- V2 performance within 20% of V1 for all optimization categories
- No query slower than 2x V1 performance
- 95th percentile latency improvements

----------

### 4.10 Risk Assessment

**High Risks**:
1. **API Design Complexity**: Aggregated statistics API must handle all edge cases
2. **Correctness**: Metadata queries must match data scan results exactly
3. **Performance Regression**: New code paths could slow down existing queries

**Mitigation**:
- Extensive testing with real workloads
- Feature flags for gradual rollout
- Performance regression suite
- Fallback to V1 connector if needed

**Medium Risks**:
1. **Kernel API Stability**: Changes may affect other Kernel users
2. **Resource Usage**: Parallel planning may increase memory usage

**Mitigation**:
- API versioning and deprecation policy
- Configurable thread pools with sensible defaults
- Memory pressure monitoring

--------------------------------------------------------------------------

## 5. Appendix: Detailed Code Examples

### A. Current V1 Metadata Query Implementation
```scala
// OptimizeMetadataOnlyDeltaQuery.scala
private def extractCountMinMaxFromStats(...): (Option[Long], Map[String, DeltaColumnStat]) = {
  val filesWithStatsForScan = deltaScanGenerator.filesWithStatsForScan(Nil)
  
  // Aggregate numRecords
  val dvCardinality = coalesce(col("deletionVector.cardinality"), lit(0))
  val numLogicalRecords = (col("stats.numRecords") - dvCardinality).as("numLogicalRecords")
  val count = filesWithStatsForScan.select(sum(numLogicalRecords)).head.getLong(0)
  
  // Aggregate min/max
  val minMaxExpr = dataColumns.flatMap { col =>
    Seq(
      s"min(`stats.minValues.$col`) as `min_$col`",
      s"max(`stats.maxValues.$col`) as `max_$col`"
    )
  }
  val stats = filesWithStatsForScan.selectExpr(minMaxExpr: _*).head
  
  (Some(count), columnStatsMap)
}
```

### B. Proposed V2 Metadata Query Implementation
```java
// SparkScan.java
private InternalRow computeAggregationFromMetadata() {
  // Call Kernel aggregated statistics API
  AggregationRequest request = new AggregationRequest()
      .addCount()
      .addMin("salary")
      .addMax("age");
  
  Optional<AggregatedStatistics> stats = 
      kernelScan.getAggregatedStatistics(engine, request);
  
  if (stats.isPresent()) {
    return InternalRow.fromSeq(
        stats.get().totalRecords,
        stats.get().minValues.get("salary"),
        stats.get().maxValues.get("age")
    );
  }
  
  // Fallback: manual aggregation
  return aggregateManually();
}
```

### C. Proposed Kernel Limit API
```java
// Kernel ScanBuilder
public interface ScanBuilder {
  ScanBuilder withLimit(long limit);
  
  ScanBuilder withOffset(long offset);
  
  Scan build();
}

// Usage in V2 connector
io.delta.kernel.Scan kernelScan = snapshot.getScanBuilder(engine)
    .withFilter(predicate)
    .withLimit(1000)  // <-- New API
    .build();
```

--------------------------------------------------------------------------

## 6. Conclusion

The Delta V2 Connector based on Kernel provides a solid foundation but currently lacks critical query optimizations present in V1. The most impactful missing features are:

1. **Metadata-Only Queries** (1000x performance impact)
2. **Limit Pushdown** (10-100x performance impact)  
3. **Parallel Query Planning** (3-4x performance impact)

Addressing these gaps requires coordinated work between Kernel and Connector teams, with an estimated timeline of 9-12 months to achieve performance parity with V1.

**Priority focus should be on Projects 1 & 2** (Metadata-Only and Limit Pushdown) as they address the largest performance gaps and most common query patterns.
