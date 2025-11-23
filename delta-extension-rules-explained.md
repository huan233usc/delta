# Delta Spark Session Extension Rules Explained

[Xin Huang](mailto:xin.huang@databricks.com)  
This doc explains all the rules injected by Delta's Spark Session Extension and their role in batch read operations.

## Overview

Delta injects rules at different phases of Spark's query planning:

```
Spark Query Planning Phases:
├─ 0. Parser (SQL Parsing)
│  └─ DeltaSqlParser               ⭐ Used in batch read (Delta SQL syntax)
│
├─ 1. Resolution (Analysis)
│  ├─ [Config] Parquet Field ID    ⭐ Used in batch read (column mapping)
│  ├─ ResolveDeltaPathTable        ⭐ Used in batch read
│  ├─ PreprocessTimeTravel         ⭐ Used in batch read (time travel queries)
│  └─ DeltaAnalysis                ⭐ Used in batch read (V1 fallback)
│
├─ 2. Check (Analysis Validation)
│  ├─ CheckUnresolvedRelationTimeTravel ⭐ Used in batch read (validation)
│  └─ DeltaUnsupportedOperationsCheck   ⭐ Used in batch read (validation)
│
├─ 3. PostHocResolution (After main resolution)
│  ├─ PreprocessTableUpdate        (DML only)
│  ├─ PreprocessTableMerge         (DML only)
│  ├─ PreprocessTableDelete        (DML only)
│  └─ PostHocResolveUpCast         (DML only)
│
├─ 4. PlanNormalization
│  └─ GenerateRowIDs               (Write only)
│
├─ 5. Optimizer
│  ├─ RangePartitionIdRewrite              (Advanced feature, rare)
│  └─ OptimizeConditionalIncrementMetric   ⭐ May be used (metric optimization)
│
├─ 6. PreCBO (Pre Cost-Based Optimization)
│  ├─ PrepareDeltaScan             ⭐⭐⭐ CRITICAL for batch read
│  ├─ ConstantFolding              ⭐ Used in batch read (cleanup)
│  └─ PrepareDeltaSharingScan      (Optional, Delta Sharing only)
│
├─ 7. Planner Strategy
│  └─ PreprocessTableWithDVsStrategy ⭐ Used in batch read (deletion vectors)
│
└─ 8. Table Value Functions (TVF)
   ├─ table_changes()              ⭐ Used in batch read (CDC)
   └─ table_changes_by_path()      ⭐ Used in batch read (CDC)
```

**⭐ = Used in batch read operations**

---

## Rules Used in Batch Read (Detailed)

### 0. DeltaSqlParser (Parser Phase)

**File**: `spark/src/main/scala/io/delta/sql/parser/DeltaSqlParser.scala`

**Purpose**: Parses Delta-specific SQL syntax extensions

**What it does**:
- Parses `VERSION AS OF` / `TIMESTAMP AS OF` syntax for time travel
- Parses Delta-specific commands (DESCRIBE HISTORY, OPTIMIZE, VACUUM, etc.)
- Parses table-valued functions (`table_changes()`, etc.)

**Used in batch read?** ✅ YES - ALWAYS
- Every Delta SQL query goes through this parser
- Enables time travel syntax: `SELECT * FROM table VERSION AS OF 10`
- Enables CDC queries: `SELECT * FROM table_changes('table', 1, 10)`

---

### 0.5. Parquet Field ID Configuration (Resolution Phase)

**File**: `spark/src/main/scala/io/delta/sql/AbstractDeltaSparkSessionExtension.scala` L62-63

**Purpose**: Enable Parquet field ID read/write for column mapping support

**What it does**:
```scala
// Set during DeltaAnalysis injection
session.sessionState.conf.setConf(SQLConf.PARQUET_FIELD_ID_READ_ENABLED, true)
session.sessionState.conf.setConf(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED, true)
```

**Used in batch read?** ✅ YES - CRITICAL for column mapping
- **Column Mapping Mode**: When enabled, Delta tracks columns by ID instead of name
- Enables schema evolution without breaking existing queries
- Required for reading tables with `columnMapping.mode = id`
- Without this config, reads would fail on column-mapped tables!

**Why it's important**:
```sql
-- Table with column mapping
CREATE TABLE t (id INT, name STRING) 
TBLPROPERTIES('delta.columnMapping.mode' = 'id')

-- Rename column (only works with column mapping)
ALTER TABLE t RENAME COLUMN name TO full_name

-- Old Parquet files still have "name" column
-- New queries use "full_name"
-- Field ID mapping enables this to work!
```

---

### 1. ResolveDeltaPathTable (Resolution Phase)

**File**: `spark/src/main/scala/org/apache/spark/sql/delta/ResolveDeltaPathTable.scala`

**Purpose**: Resolves path-based Delta table references

**What it does**:
```scala
// Converts path-based references to Delta tables
UnresolvedTable("delta.`/path/to/table`")
  └─> ResolvedTable(DeltaTableV2(path="/path/to/table"))
```

**Used in batch read?** ✅ YES
- For queries like: `spark.sql("SELECT * FROM delta.\`/path/to/table\`")`
- Transforms path identifier into DeltaTableV2 before standard resolution
- Enables path-based table access without catalog

**Key logic**:
```scala
case class ResolveDeltaPathTable(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case u: UnresolvedTable =>
      // Check if multipartIdentifier is a valid path
      if (DeltaTableUtils.isValidPath(tableId)) {
        val deltaTableV2 = DeltaTableV2(sparkSession, new Path(path))
        ResolvedTable.create(sessionCatalog, identifier, deltaTableV2)
      }
  }
}
```

---

### 2. PreprocessTimeTravel (Resolution Phase)

**File**: `spark/src/main/scala/org/apache/spark/sql/delta/PreprocessTimeTravel.scala`

**Purpose**: Resolves time travel queries (VERSION AS OF / TIMESTAMP AS OF)

**What it does**:
```scala
// For time travel queries
TimeTravel(UnresolvedRelation("table"), version=10)
  └─> TimeTravel(ResolvedTable(DeltaTableV2), version=10)
```

**Used in batch read?** ✅ YES (for time travel queries)
- For: `SELECT * FROM table VERSION AS OF 10`
- For: `SELECT * FROM table TIMESTAMP AS OF '2024-01-01'`
- Also used for RESTORE and CLONE commands

**Key logic**:
```scala
case class PreprocessTimeTravel(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case TimeTravel(ur: UnresolvedRelation, timestamp, version, _) =>
      // Manually resolve the table inside TimeTravel
      val resolved = ResolveRelations(ur)
      TimeTravel(resolved, timestamp, version, ...)
  }
}
```

---

### 3. DeltaAnalysis (Resolution Phase - customResolutionRules)

**File**: `spark/src/main/scala/org/apache/spark/sql/delta/DeltaAnalysis.scala`

**Purpose**: Main Delta-specific resolution logic, including V1 fallback

**What it does**:
```scala
// Immediate V1 fallback for batch read
DataSourceV2Relation(DeltaTableV2) 
  └─> LogicalRelation(HadoopFsRelation)  // V1 fallback

// Also handles: DML rewriting, schema evolution, type casting, etc.
```

**Used in batch read?** ✅ YES - CRITICAL
- **Pattern**: `FallbackToV1DeltaRelation` matches `DataSourceV2Relation(DeltaTableV2)`
- **Condition**: No `KEEP_AS_V2_RELATION_TAG` tag
- **Result**: Converts to V1 `LogicalRelation(HadoopFsRelation)`
- This is why batch read uses V1 execution path!

**Key logic**:
```scala
class DeltaAnalysis(session: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    // V1 fallback for batch read
    case FallbackToV1DeltaRelation(v1Relation) => v1Relation
    
    // ... many other patterns for DML, schema evolution, etc.
  }
}
```

---

### 4. PrepareDeltaScan (PreCBO Phase)

**File**: `spark/src/main/scala/org/apache/spark/sql/delta/stats/PrepareDeltaScan.scala`

**Purpose**: ⭐⭐⭐ **MOST CRITICAL** for batch read performance

**What it does**:
1. **Snapshot Isolation**: Ensures all reads use the same Delta Log snapshot
2. **File Pruning**: Applies filters to determine which files to scan
3. **Data Skipping**: Uses statistics (min/max/null_count) to skip files
4. **Generated Column Optimization**: Generates additional partition filters
5. **Limit Push Down**: Optimizes LIMIT queries
6. **Metadata-Only Queries**: Optimizes queries that only need metadata

**Used in batch read?** ✅ YES - ALWAYS
- Runs **before** CBO (Cost-Based Optimizer)
- Transforms `LogicalRelation(TahoeLogFileIndex)` → `LogicalRelation(PreparedDeltaFileIndex)`
- Determines exact set of files to scan

**Key transformations**:
```scala
class PrepareDeltaScan(protected val spark: SparkSession) extends Rule[LogicalPlan] {
  
  def apply(plan: LogicalPlan): LogicalPlan = {
    // Match: LogicalRelation with TahoeLogFileIndex
    case DeltaTableScan(filters, fileIndex, limit, delta) =>
      
      // 1. Get or create snapshot (for snapshot isolation)
      val scanGenerator = getDeltaScanGenerator(fileIndex)
      
      // 2. Apply filters and get files to scan
      val deltaScan = filesForScan(scanGenerator, limitOpt, filters, delta)
      // deltaScan contains:
      //   - files: Seq[AddFile] (pruned by filters)
      //   - partition values
      //   - scanned snapshot version
      
      // 3. Generate partition filters from generated columns
      val generatedPartitionFilters = 
        GeneratedColumn.generatePartitionFilters(spark, snapshot, filters, delta)
      
      // 4. Create PreparedDeltaFileIndex
      val preparedIndex = PreparedDeltaFileIndex(
        spark, deltaLog, path, catalogTable, deltaScan, version
      )
      
      // 5. Return new LogicalRelation with prepared index
      delta.copy(relation = delta.relation.copy(
        location = preparedIndex,
        partitionSchema = StructType(Nil) // Partitions already applied
      ))
  }
}
```

**Why it's critical**:
- Without this rule, Spark would scan **ALL files** in the table
- Data skipping can reduce files scanned by 90%+ in many queries
- Provides accurate statistics for CBO to make better join/aggregation decisions

**Example**:
```sql
-- Original query
SELECT * FROM large_table WHERE date = '2024-01-01' AND id > 1000

-- Before PrepareDeltaScan:
//   TahoeLogFileIndex: knows about ALL 10,000 files in the table

-- After PrepareDeltaScan:
//   PreparedDeltaFileIndex: knows about only 50 files after:
//     - Partition pruning (date = '2024-01-01')
//     - Data skipping using stats (id > 1000)
```

---

### 5. ConstantFolding (PreCBO Phase)

**File**: Spark built-in optimizer rule

**Purpose**: Fold constant expressions introduced by PrepareDeltaScan

**What it does**:
```scala
// Simplify expressions like:
1 + 2 + col → 3 + col
true AND condition → condition
```

**Used in batch read?** ✅ YES
- Cleans up constant expressions that PrepareDeltaScan may introduce
- Only needed in Spark 3.5 (later versions do this automatically)

---

### 6. PreprocessTableWithDVsStrategy (Planner Strategy)

**File**: `spark/src/main/scala/org/apache/spark/sql/delta/PreprocessTableWithDVsStrategy.scala`

**Purpose**: Handle deletion vectors in physical planning

**What it does**:
- For files with deletion vectors, inject row-level filtering
- Adds `__skip_row` column to filter deleted rows
- Transforms physical plan to apply deletion vector bitmap

**Used in batch read?** ✅ YES (when deletion vectors are present)
- For tables with Row-level DELETEs/UPDATEs
- Adds minimal overhead even when DVs are present
- Only active if table has `deletionVectors` feature enabled

**Key logic**:
```scala
// Physical plan transformation:
FileSourceScanExec(files with DVs)
  └─> FileSourceScanExec with __skip_row column
      └─> Filter(__skip_row = false)
```

---

### 7. CheckUnresolvedRelationTimeTravel (Check Phase)

**File**: `spark/src/main/scala/org/apache/spark/sql/delta/CheckUnresolvedRelationTimeTravel.scala`

**Purpose**: Validation for time travel queries

**What it does**:
- Checks if time travel table reference is resolved
- Throws proper error if table doesn't exist
- Fixes Spark bug [SPARK-45383] for better error messages

**Used in batch read?** ✅ YES (for time travel queries)
- Validation only, doesn't transform plan
- Ensures proper error messages for: `SELECT * FROM non_existent_table VERSION AS OF 10`

---

### 8. DeltaUnsupportedOperationsCheck (Check Phase)

**File**: `spark/src/main/scala/org/apache/spark/sql/delta/DeltaUnsupportedOperationsCheck.scala`

**Purpose**: Validation for unsupported operations on Delta tables

**What it does**:
- Checks for operations Delta doesn't support
- Throws descriptive errors early in analysis

**Used in batch read?** ✅ YES
- Validation only, catches unsupported read patterns
- Examples: Invalid time travel syntax, unsupported predicates

---

### 9. OptimizeConditionalIncrementMetric (Optimizer Phase)

**File**: `spark/src/main/scala/org/apache/spark/sql/delta/metric/IncrementMetric.scala` L179

**Purpose**: Optimize Delta metric collection expressions

**What it does**:
```scala
// Simplify metric expressions with constant conditions
ConditionalIncrementMetric(child, Literal(true), metric)
  → IncrementMetric(child, metric)

ConditionalIncrementMetric(child, Literal(false), metric)
  → child  // Remove metric entirely
```

**Used in batch read?** ✅ YES (when enabled)
- Optimizes internal Delta metrics
- Used for DeltaLog stats collection
- Removes unnecessary metric overhead
- Config: `spark.databricks.delta.optimize.conditionalIncrementMetric.enabled`

---

### 10. Table Value Functions (TVF)

**File**: `spark/src/main/scala/org/apache/spark/sql/delta/DeltaTableValueFunctions.scala`

**Purpose**: Provide table-valued functions for Delta-specific queries

**Functions**:
1. **`table_changes(table_name, start_version, end_version)`** - CDC queries
2. **`table_changes_by_path('/path/to/table', start, end)`** - CDC by path

**What they do**:
```sql
-- Get all changes between versions
SELECT * FROM table_changes('my_table', 0, 10)
-- Returns CDC rows with _change_type column (insert/update_preimage/update_postimage/delete)

-- By path
SELECT * FROM table_changes_by_path('s3://bucket/table', 0, 10)
```

**Used in batch read?** ✅ YES - CDC is a read operation!
- Reads Delta Log to find changed files
- Returns CDC data with change type metadata
- Powered by CDCReader

**Resolution flow**:
```
CDCNameBased (TVF logical plan)
  └─> DeltaAnalysis resolves to TableChanges
      └─> TableChanges resolves to LogicalRelation
          └─> CDCReader creates CDC relation
```

---

## Rules NOT Used in Batch Read

### PostHocResolution Rules (DML only)

These only run for DML operations (UPDATE/DELETE/MERGE):

**PreprocessTableUpdate**:
- Rewrites UPDATE command
- Adds schema evolution logic
- Used only for: `UPDATE table SET ...`

**PreprocessTableMerge**:
- Rewrites MERGE INTO command
- Handles WHEN MATCHED/NOT MATCHED clauses
- Used only for: `MERGE INTO target USING source ...`

**PreprocessTableDelete**:
- Rewrites DELETE command
- Determines delete strategy (metadata-only vs. rewrite)
- Used only for: `DELETE FROM table WHERE ...`

**PostHocResolveUpCast**:
- Resolves type casts introduced by above rules
- Only runs after DML preprocessing

### Other Rules

**CheckUnresolvedRelationTimeTravel**:
- Validation rule for time travel syntax
- Throws error if time travel table doesn't exist

**DeltaUnsupportedOperationsCheck**:
- Validation rule that checks for unsupported operations
- Examples: TRUNCATE on non-Delta tables, unsupported ALTER commands

**GenerateRowIDs** (PlanNormalization):
- Generates unique row IDs for write operations
- Used only for INSERT/UPDATE/DELETE/MERGE

**RangePartitionIdRewrite** (Optimizer):
- Rewrites range partition ID expressions
- Used for advanced partitioning features

**OptimizeConditionalIncrementMetric** (Optimizer):
- Optimizes metric collection expressions
- Internal optimization for Delta metrics

---

## Summary: Batch Read Flow with Rules

### Complete Flow

```
User Query: SELECT * FROM table WHERE id > 100

┌─────────────────────────────────────────────────────────────┐
│ 1. Resolution Phase (Analyzer)                              │
├─────────────────────────────────────────────────────────────┤
│ UnresolvedRelation("table")                                 │
│   └─> ResolveRelations (Spark)                             │
│       └─> DataSourceV2Relation(DeltaTableV2)               │
│   └─> DeltaAnalysis (Delta custom rule)                    │
│       └─> FallbackToV1DeltaRelation                        │
│           └─> LogicalRelation(HadoopFsRelation)            │
│                 location: TahoeLogFileIndex                 │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. Optimization Phase (Optimizer)                           │
├─────────────────────────────────────────────────────────────┤
│ Standard Spark optimizations:                               │
│   - PushDownPredicate (pushes WHERE to scan)               │
│   - ColumnPruning                                           │
│   - ConstantFolding                                         │
│   - etc.                                                    │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. PreCBO Phase (Before Cost-Based Optimization)            │
├─────────────────────────────────────────────────────────────┤
│ ⭐ PrepareDeltaScan (Delta rule)                            │
│   └─> Read Delta Log snapshot                              │
│   └─> Apply filters to file list                           │
│   └─> Data skipping using statistics                       │
│   └─> Generated column optimization                        │
│   └─> Create PreparedDeltaFileIndex                        │
│                                                             │
│ LogicalRelation(HadoopFsRelation)                          │
│   location: TahoeLogFileIndex (10,000 files)               │
│     ↓                                                       │
│ LogicalRelation(HadoopFsRelation)                          │
│   location: PreparedDeltaFileIndex (50 files)  ← Data skipping!│
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. CBO Phase (Cost-Based Optimization)                      │
├─────────────────────────────────────────────────────────────┤
│ Uses statistics from PreparedDeltaFileIndex:               │
│   - Number of files: 50 (not 10,000!)                      │
│   - Data size: 500MB (not 10GB!)                           │
│   - Row count estimate                                      │
│ Better join order, aggregation strategy decisions           │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│ 5. Physical Planning (Planner)                              │
├─────────────────────────────────────────────────────────────┤
│ LogicalRelation → FileSourceScanExec                        │
│                                                             │
│ ⭐ PreprocessTableWithDVsStrategy (if DVs present)          │
│   └─> Add __skip_row column                                │
│   └─> Add Filter(__skip_row = false)                       │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│ 6. Execution                                                │
├─────────────────────────────────────────────────────────────┤
│ FileSourceScanExec.doExecute()                             │
│   └─> PreparedDeltaFileIndex.listFiles() → 50 files       │
│   └─> Read 50 Parquet files                                │
│   └─> Apply deletion vectors (if present)                  │
│   └─> Return filtered results                              │
└─────────────────────────────────────────────────────────────┘
```

---

## Key Takeaways

### Rules Critical for Batch Read Performance

1. **DeltaAnalysis** (Resolution)
   - Converts DSv2 → V1 for execution
   - Without this: Delta wouldn't execute at all

2. **PrepareDeltaScan** (PreCBO)
   - THE most important performance rule
   - Data skipping, snapshot isolation, statistics
   - Without this: Would scan ALL files, very slow!

3. **PreprocessTableWithDVsStrategy** (Physical Planning)
   - Handles deletion vectors
   - Without this: Deleted rows would appear in results

### Rules NOT Used in Batch Read

- All PostHocResolution rules (UPDATE/DELETE/MERGE only)
- GenerateRowIDs (Write only)
- Most Check rules (validation only)

### Performance Impact

```
Example table: 10,000 files, 10GB data
Query: SELECT * FROM table WHERE date = '2024-01-01' AND id > 1000

Without PrepareDeltaScan:
  ❌ Scan 10,000 files
  ❌ Read 10GB
  ❌ CBO has wrong stats
  ❌ Time: ~10 minutes

With PrepareDeltaScan:
  ✅ Scan 50 files (partition + data skipping)
  ✅ Read 500MB
  ✅ CBO has accurate stats
  ✅ Time: ~30 seconds

20x speedup! 🚀
```

