# Life of a query of Delta

[Xin Huang](mailto:xin.huang@databricks.com)  
This doc tries to capture key pieces of how planning works for Delta connector today, keep evolving for more operations 

## Batch Read

TL;DR

```
Path 1: Catalog-Based (spark.read.table("delta_table"))
  
  User API:
    spark.read.table("delta_table")
    └─> Creates UnresolvedRelation(isStreaming=false)
  
  Analysis Phase (triggered by action like .show(), .collect()):
    0. Parser (if SQL): DeltaSqlParser handles Delta-specific syntax
    
    1. Config Setup: Enable Parquet Field ID for column mapping
    
    2. Resolution Batch (FixedPoint):
       Round 1:
         - ResolveDeltaPathTable (Delta): Path-based table resolution
         - PreprocessTimeTravel (Delta): Time travel query resolution  
         - ResolveRelations (Spark): UnresolvedRelation → DataSourceV2Relation
           └─> DeltaCatalog.loadTable() returns DeltaTableV2
       
       Round 2:
         - DeltaAnalysis (Delta customResolutionRules): Immediate V1 fallback ⭐
           └─> DataSourceV2Relation → LogicalRelation(HadoopFsRelation)
               ├─> Pattern: FallbackToV1DeltaRelation
               ├─> Condition: No KEEP_AS_V2_RELATION_TAG
               └─> Creates LogicalRelation with TahoeLogFileIndex (unprepared)
    
    3. Check Phase:
       - CheckUnresolvedRelationTimeTravel (Delta)
       - DeltaUnsupportedOperationsCheck (Delta)
  
  Optimization Phase:
    - Standard Spark: PushDownPredicate, ColumnPruning
    - RangePartitionIdRewrite (Delta, advanced partitioning)
    - OptimizeConditionalIncrementMetric (Delta, metric optimization)
  
  PreCBO Phase (CRITICAL ⭐⭐⭐):
    - PrepareDeltaScan (Delta): Data skipping happens here!
      ├─> Snapshot Isolation: Pin Delta Log version
      ├─> Data Skipping: Use min/max/null_count statistics
      ├─> Partition Pruning: Filter by partition values
      ├─> Generated Column Optimization: Generate extra filters
      └─> TahoeLogFileIndex → PreparedDeltaFileIndex (significantly fewer files)
    
    - ConstantFolding: Clean up constants from PrepareDeltaScan
  
  CBO Phase:
    - Uses accurate statistics from PreparedDeltaFileIndex
    - Makes better decisions on join order, aggregation strategy
  
  Physical Planning:
    - Spark Planner: LogicalRelation → FileSourceScanExec
    - PreprocessTableWithDVsStrategy (Delta): Deletion vector handling
  
  Execution:
    FileSourceScanExec.doExecute()
    └─> PreparedDeltaFileIndex.listFiles() (pruned file list)
    └─> DeltaParquetFileFormat.buildReader() (with deletion vectors)
    └─> Returns RDD[InternalRow]

Path 2: Path-Based (spark.read.format("delta").load("/path"))
  Similar flow through ResolveDeltaPathTable and ResolveDataSource
  
Special Query:
  - Time Travel: SELECT * FROM table VERSION AS OF 10
```

### 1. Entry Point

Delta batch read has two entry points:

**Path 1: Catalog-based read**
```scala
// User code
spark.read.table("catalog.schema.delta_table")

// Implementation: DataFrameReader.scala
def table(tableName: String): DataFrame = {
  val multipartIdent = sparkSession.sessionState.sqlParser
    .parseMultipartIdentifier(tableName)
  Dataset.ofRows(
    sparkSession, 
    UnresolvedRelation(multipartIdent, CaseInsensitiveStringMap.empty(), isStreaming = false)
  )
}
```

**Path 2: Path-based read**
```scala
// User code
spark.read.format("delta").load("/path/to/delta")

// Implementation: DataFrameReader.scala
def load(paths: String*): DataFrame = {
  Dataset.ofRows(
    sparkSession,
    UnresolvedDataSource(
      source = "delta",
      userSpecifiedSchema = None,
      extraOptions = options,
      isStreaming = false
    )
  )
}
```

### 2. Query Planning

Query planning is triggered when an action is called on the DataFrame (e.g., `.show()`, `.collect()`, `.count()`), which accesses [`df.queryExecution.analyzed`](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala#L149).

#### 2.0 Parser Phase (If SQL query)

For SQL queries, Delta's parser handles Delta-specific syntax:

**DeltaSqlParser** (injected by DeltaSparkSessionExtension):
```scala
// Parses Delta SQL extensions
spark.sql("SELECT * FROM table VERSION AS OF 10")
  └─> Parser recognizes VERSION AS OF syntax
      └─> Creates TimeTravel logical plan node
```

**Key capabilities**:
- Time travel syntax: `VERSION AS OF`, `TIMESTAMP AS OF`
- Delta commands: `DESCRIBE HISTORY`, `OPTIMIZE`, `VACUUM`, etc.

#### 2.1 Analysis Phase

The [Analyzer](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala#L265) applies resolution rules in a [`FixedPoint`](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/rules/RuleExecutor.scala#L207) strategy until the plan converges.

**Configuration Setup** (during DeltaAnalysis injection):
```scala
// Enable Parquet Field ID for column mapping support
// This is CRITICAL for reading tables with column mapping mode enabled
session.conf.setConf(SQLConf.PARQUET_FIELD_ID_READ_ENABLED, true)
session.conf.setConf(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED, true)
```

**Column Mapping Importance**: Without this config, reads would fail on tables with `columnMapping.mode = id`. Column mapping allows schema evolution (e.g., column renames) without breaking existing queries by tracking columns by ID instead of name.

**Key Batches and Rules:**

* Phase 1: Early Batches
  * Batch 1: "Substitution" (FixedPoint)
  * Batch 2-6: Hints, Sanity Checks, etc.
* Phase 2: Main Resolution
  * Batch 7: "Resolution" (FixedPoint) ⭐
    * **ResolveDeltaPathTable** (Delta injected) - path-based tables
    * **PreprocessTimeTravel** (Delta injected) - time travel queries
    * **ResolveRelations** (standard Spark rule)
    * ResolveReferences
    * ResolveFunctions
    * ... (many other rules)
    * extendedResolutionRules
      * ResolveDataSource
      * FindDataSourceTable
      * ResolveSessionCatalog
      * ...
      * customResolutionRules ⭐
        * **DeltaAnalysis** (injected by DeltaSparkSessionExtension)
* Phase 3: Check Rules (Validation)
  * **CheckUnresolvedRelationTimeTravel** (Delta injected)
  * **DeltaUnsupportedOperationsCheck** (Delta injected)

**For Path 1 (Catalog-based):**

In [ResolveRelations](https://github.com/apache/spark/blob/e3533a62ca0abb43ca16e1fc767374f54298a203/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/RelationResolution.scala#L133-L145), `UnresolvedRelation` is converted to `DataSourceV2Relation`:

* [Load Table](https://github.com/apache/spark/blob/e3533a62ca0abb43ca16e1fc767374f54298a203/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/RelationResolution.scala#L133-L134) from catalog: `DeltaCatalog.loadTable()` is called
  * Returns: `DeltaTableV2`
  * Note: `DeltaTableV2` implements:
    * `Table` (DSv2 interface)
    * `V2TableWithV1Fallback` (provides v1 fallback)
    * `SupportsWrite` (for writes only)
    * Capabilities: `ACCEPT_ANY_SCHEMA`, `BATCH_READ`, `V1_BATCH_WRITE`, etc.
    * ⚠️ **Does NOT implement `SupportsRead`** (no DSv2 read support)
* Create `DataSourceV2Relation(DeltaTableV2, catalog, identifier)`

**Immediate Fallback in DeltaAnalysis Rule:**

In the SAME Resolution batch (next FixedPoint iteration), [DeltaAnalysis](https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/DeltaAnalysis.scala#L82) rule converts `DataSourceV2Relation` → `LogicalRelation`:

```scala
// DeltaAnalysis.scala L491
case FallbackToV1DeltaRelation(v1Relation) => v1Relation

// FallbackToV1Relations.scala L24-32
/**
 * Fall back to V1 nodes, since we don't have a V2 reader for Delta right now
 */
object FallbackToV1DeltaRelation {
  def unapply(dsv2: DataSourceV2Relation): Option[LogicalRelation] = dsv2.table match {
    // Match if it's DeltaTableV2 AND no "KEEP_AS_V2_RELATION_TAG" tag
    case d: DeltaTableV2 if dsv2.getTagValue(DeltaRelation.KEEP_AS_V2_RELATION_TAG).isEmpty =>
      Some(DeltaRelation.fromV2Relation(d, dsv2, dsv2.options))
    case _ => None
  }
}
```

**Why Immediate Fallback?**
* **Fallback condition**: `DataSourceV2Relation` has NO `KEEP_AS_V2_RELATION_TAG` tag
* For batch read operations, this tag is **NOT set** → always fallback
* The tag is only set for specific operations (e.g., MERGE INTO target table) that need V2
* **Root cause**: Delta doesn't implement `SupportsRead` interface for batch reads
* Must use V1 execution path via `HadoopFsRelation`
* Fallback happens in **Analysis phase**, not Optimization

**Result after Analysis:**
```
SubqueryAlias [catalog, schema, table]
└─ LogicalRelation(
     relation: HadoopFsRelation(
       location: TahoeLogFileIndex,        // Delta's file index (unprepared)
       partitionSchema: StructType(...),
       dataSchema: StructType(...),
       bucketSpec: None,
       fileFormat: DeltaParquetFileFormat, // Delta's Parquet format
       options: Map(...)
     ),
     output: [attributes...],
     catalogTable: Some(CatalogTable),
     isStreaming: false
   )
```

**For Path 2 (Path-based):**

Similar flow through [ResolveDataSource](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/DataSourceV2Strategy.scala) rule:
* `UnresolvedDataSource("delta", ...)` → `DataSourceV2Relation(DeltaTableV2)`
* Then same immediate fallback through `DeltaAnalysis` rule
* Final result: `LogicalRelation(HadoopFsRelation)` (no catalog table reference)

#### 2.2 Optimization Phase

The V1 `LogicalRelation` goes through standard Spark optimizations plus Delta-specific rules:

```
Optimizer batches:
  └─> PushDownPredicate
      └─> Push filters to HadoopFsRelation
  └─> ColumnPruning
      └─> Prune unused columns
  └─> RangePartitionIdRewrite (Delta injected)
      └─> For advanced range partitioning features
  └─> OptimizeConditionalIncrementMetric (Delta injected)
      └─> Optimize Delta internal metrics
  └─> ... (other V1 optimizations)
```

**Delta Optimizer Rules**:

**RangePartitionIdRewrite**: 
- Handles `RangePartitionId` expressions
- Samples data to compute partition boundaries
- Rarely used in typical batch reads

**OptimizeConditionalIncrementMetric**:
- Simplifies Delta metric collection expressions
- Removes metrics with constant false conditions
- Optimizes metrics with constant true conditions
- Reduces overhead in metric-heavy queries

**Key point**: By the time Optimizer runs, the plan is already V1. No DSv2-specific optimizations apply.

#### 2.3 Pre-CBO Phase

**CRITICAL PHASE** for Delta read performance:

```
PreCBO Rules (before Cost-Based Optimization):
  └─> PrepareDeltaScan (Delta injected) ⭐⭐⭐
      ├─> Snapshot Isolation: Pin Delta Log version
      ├─> Data Skipping: Use statistics to prune files
      ├─> Partition Pruning: Filter by partition values
      ├─> Generated Column Optimization: Generate extra filters
      └─> Create PreparedDeltaFileIndex
  
  └─> ConstantFolding (Delta injected)
      └─> Clean up constants introduced by PrepareDeltaScan
  
  └─> PrepareDeltaSharingScan (Optional, if Delta Sharing enabled)
      └─> Handle Delta Sharing reads
```

**PrepareDeltaScan** is the most critical rule for batch read performance. It transforms the file index from containing all files to only those that need to be scanned:

```scala
// Before PrepareDeltaScan
LogicalRelation(HadoopFsRelation(
  location: TahoeLogFileIndex  // All files in the table
))

// After PrepareDeltaScan
LogicalRelation(HadoopFsRelation(
  location: PreparedDeltaFileIndex  // Only files matching filters!
))
```

**How PrepareDeltaScan works**:

1. **Snapshot Isolation**: Ensures all reads use the same Delta Log snapshot
   ```scala
   val scanGenerator = getDeltaScanGenerator(fileIndex)
   // Returns: Snapshot pinned on first access for this query
   ```

2. **Extract Filters**: Gets partition and data filters from the plan
   ```scala
   case DeltaTableScan(filters, fileIndex, limit, delta) =>
     val partitionFilters = filters.filter(isPartitionFilter)
     val dataFilters = filters.filterNot(isPartitionFilter)
   ```

3. **Data Skipping**: Uses statistics to prune files
   ```scala
   val deltaScan = scanGenerator.filesForScan(filters)
   // Uses min/max/null_count stats from AddFile actions
   // Skips files that provably don't match filters
   ```

4. **Generated Column Optimization**: Generates additional partition filters
   ```scala
   val generatedFilters = GeneratedColumn.generatePartitionFilters(
     spark, snapshot, filters, delta
   )
   // Example: date = '2024-01-01' generates year = 2024, month = 1
   ```

5. **Create PreparedDeltaFileIndex**: Wraps the pruned file list
   ```scala
   val preparedIndex = PreparedDeltaFileIndex(
     spark, deltaLog, path, catalogTable, deltaScan, version
   )
   // deltaScan.files contains only matching files after data skipping
   ```

#### 2.4 Cost-Based Optimization (CBO)

After PrepareDeltaScan, CBO has accurate statistics:

```
CBO receives:
  ✅ Accurate file count (after pruning)
  ✅ Accurate data size (after pruning)
  ✅ Row count estimates
  ✅ Column statistics

CBO makes better decisions:
  - Join order optimization
  - Broadcast vs. shuffle join
  - Aggregation strategy
```

### 3. Physical Planning & Execution

#### 3.1 Physical Planning

[Spark Planner](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlanner.scala) converts logical plan to physical plan:

```scala
// FileSourceStrategy (Spark's V1 strategy)
LogicalRelation(fsRelation: HadoopFsRelation, ...) =>
  FileSourceScanExec(
    relation = fsRelation,
    output = [...],
    requiredSchema = StructType(...),
    partitionFilters = [...],
    dataFilters = [...],
    tableIdentifier = Some(...)
  )
```

**Physical Plan Structure:**
```
*(1) Project [columns...]
+- *(1) Filter [predicates...]
   +- *(1) ColumnarToRow
      +- FileScan parquet delta_table [columns...]
         ├─ Location: TahoeLogFileIndex [N files, X GB]
         │           ^^^^^^^^^^^^^^^^^ Display name (may be simplified)
         │           [N files, X GB] Actual numbers from PreparedDeltaFileIndex ⭐
         ├─ PartitionFilters: [...]
         ├─ PushedFilters: [...]
         ├─ ReadSchema: struct<...>
         └─ Format: DeltaParquetFileFormat
```

**Note**: The actual object is `PreparedDeltaFileIndex` (which extends `TahoeFileIndex`). The file count and size shown are from PreparedDeltaFileIndex.

**Delta Physical Planning Strategy:**

**PreprocessTableWithDVsStrategy** (Delta injected):
- Handles deletion vectors in physical planning
- For files with deletion vectors, injects row-level filtering
- Adds `__skip_row` column to filter deleted rows

```scala
// Physical plan transformation for deletion vectors:
FileSourceScanExec(files with DVs)
  └─> FileSourceScanExec with __skip_row column
      └─> Filter(__skip_row = false)
```

#### 3.2 Execution

[`FileSourceScanExec.doExecute()`](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/FileSourceScanExec.scala#L519) executes the scan:

**Step 1: File Listing** (PreparedDeltaFileIndex)
```scala
// PreparedDeltaFileIndex.matchingFiles()
// Returns the pre-computed pruned file list
val files = preparedScan.files  // Pruned file list
```

**Step 2: Data Skipping** (Already done in PrepareDeltaScan)
```scala
// Statistics-based pruning already happened in PreCBO phase
// Files that don't match predicates are already excluded
```

**Step 3: Deletion Vector Handling**
```scala
// DeletionVectorDescriptor.scala
if (file.deletionVector != null) {
  val dv = DeletionVectorDescriptor.fromJson(file.deletionVector)
  // Apply row-level filter during Parquet read
  // Rows marked in DV are filtered out
}
```

**Step 4: Parquet Scan**
```scala
// FileSourceScanExec.createNonBucketedReadRDD()
createNonBucketedReadRDD(
  readFile,            // Function to read one file
  selectedPartitions,  // Partitions after pruning
  relation
)

// For each file:
DeltaParquetFileFormat.buildReaderWithPartitionValues(...)
// Returns: Iterator[InternalRow] with deletion vectors applied
```

**Key Delta Components**:

1. **TahoeLogFileIndex** (Pre-PrepareDeltaScan): 
   - Reads Delta Log to get list of all files
   - Contains ALL files in the table
   - Initial file index before optimization
   
2. **PreparedDeltaFileIndex** (Post-PrepareDeltaScan):
   - Contains only pruned files after data skipping
   - Handles partition pruning
   - Manages file statistics for data skipping
   - **This is what actually executes** during query execution

3. **DeltaParquetFileFormat**: 
   - Delta's Parquet format wrapper
   - Adds deletion vector support
   - Column mapping handling
   - Statistics collection

---

## Time Travel Queries

```sql
-- Query historical version
SELECT * FROM table VERSION AS OF 10
SELECT * FROM table TIMESTAMP AS OF '2024-01-01'
```

**Planning flow**:
```
Parser: DeltaSqlParser recognizes time travel syntax
  └─> Creates TimeTravel(UnresolvedRelation, version/timestamp)
  ↓
PreprocessTimeTravel: Resolves table inside TimeTravel node
  └─> TimeTravel(DataSourceV2Relation(DeltaTableV2), version/timestamp)
  ↓
DeltaAnalysis: V1 fallback
  └─> LogicalRelation with TahoeLogFileIndex(version=10)
  ↓
PrepareDeltaScan: Uses specified version
  └─> Reads files from that specific version only
  ↓
Execution: Reads historical files
```

**Key difference**: For time travel, `TahoeLogFileIndex.isTimeTravelQuery = true`, so it uses the specified version instead of latest.

### CDC Queries (Change Data Capture)

Delta supports CDC through table-valued functions:

```sql
-- Get all changes between versions
SELECT * FROM table_changes('my_table', 0, 10)

-- Get changes by path
SELECT * FROM table_changes_by_path('/path/to/table', 0, 10)
```

**Planning flow**:
```
Parser: Recognizes table_changes() function
  └─> Creates CDCNameBased/CDCPathBased logical plan
  ↓
Resolution: Registers as table-valued function
  └─> Creates TableChanges node
  ↓
DeltaAnalysis: Converts to CDC relation
  └─> TableChanges → LogicalRelation
  ↓
CDCReader: Reads Delta Log for change data
  └─> Scans AddFile/RemoveFile actions between versions
  └─> Generates CDC rows with _change_type
  ↓
Execution: Returns CDC data
```

**Output includes**:
- All original columns
- `_change_type`: insert, update_preimage, update_postimage, delete
- `_commit_version`: Version where change occurred
- `_commit_timestamp`: Timestamp of change

---

## Summary: Is Delta Batch Read DSv2?

### Answer: **NO - Temporary DSv2 Resolution, Immediate V1 Fallback**

**DSv2 Usage (Temporary, < 1 Analyzer iteration):**
* ✅ Table resolution through `TableCatalog` interface
* ✅ `DeltaTableV2` implements DSv2 `Table` interface
* ✅ `DataSourceV2Relation` created in ResolveRelations rule

**V1 Fallback (ALWAYS happens for batch reads in same Resolution batch):**
* ⚠️ `DeltaAnalysis` rule checks for `KEEP_AS_V2_RELATION_TAG` tag
* ⚠️ Batch read: NO tag → fallback to V1 immediately
* ⚠️ Happens in ANALYSIS phase (customResolutionRules), not Optimization
* ⚠️ `DataSourceV2Relation` never leaves the Analyzer
* ⚠️ Final analyzed plan is V1 `LogicalRelation(HadoopFsRelation)`

**Why No DSv2 Read Support?**

Delta explicitly chooses V1 path for batch reads through the **tag mechanism**:

```scala
// DeltaRelation.scala L1371
val KEEP_AS_V2_RELATION_TAG = new TreeNodeTag[Unit]("__keep_as_v2_relation")

// For batch reads: NO tag is set → fallback to V1
// For MERGE INTO target: Tag IS set → keep as V2 (DeltaAnalysis.scala L607)
case r: DataSourceV2Relation => r.setTagValue(DeltaRelation.KEEP_AS_V2_RELATION_TAG, ())
```

**Root cause**: Delta doesn't implement DSv2 read interfaces:
* ❌ No `SupportsRead` trait
* ❌ No `ScanBuilder` implementation
* ❌ No `Scan` implementation
* ❌ No `Batch` implementation

```scala
// DeltaTableV2.scala - Capabilities
override def capabilities() = Set(
  ACCEPT_ANY_SCHEMA,
  BATCH_READ,        // Claims this capability
  V1_BATCH_WRITE,    // But uses V1 for execution!
  OVERWRITE_BY_FILTER,
  TRUNCATE,
  OVERWRITE_DYNAMIC
)
// Note: Claiming BATCH_READ is misleading - actual reads use V1 path
```

**Fallback Timing:**
```
Resolution Batch (FixedPoint strategy):
  Iteration 1:
    UnresolvedRelation
    └─> ResolveRelations rule
        └─> DataSourceV2Relation(DeltaTableV2)
  
  Iteration 2:
    DataSourceV2Relation
    └─> DeltaAnalysis rule (customResolutionRules)
        └─> LogicalRelation(HadoopFsRelation)  ← V1 from here on
  
  Iteration 3:
    Convergence: No more changes, exit FixedPoint
```

**Execution Path:**
* V1 `FileSourceScanExec` (not DSv2 `BatchScanExec`)
* V1 `HadoopFsRelation` (not DSv2 `Scan`)
* Delta-specific V1 components:
  * `TahoeLogFileIndex` → `PreparedDeltaFileIndex` (file listing via Delta Log)
  * `DeltaParquetFileFormat` (deletion vectors, column mapping)

---

## Key Files Reference

### Spark Core

- **SparkSessionExtensions**: `/spark/sql/core/src/main/scala/org/apache/spark/sql/SparkSessionExtensions.scala`
  - Extension injection API

- **DataFrameReader**: `/spark/sql/core/src/main/scala/org/apache/spark/sql/DataFrameReader.scala`
  - table(): L232 (creates UnresolvedRelation)
  - load(): L105 (creates UnresolvedDataSource)

- **QueryExecution**: `/spark/sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala`
  - analyzed: L149 (lazy property that triggers analysis)
  - assertAnalyzed(): L265 (calls analyzer.executeAndCheck)

- **Analyzer**: `/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala`
  - Resolution batch definition: L133-584
  - extendedResolutionRules: Uses from BaseSessionStateBuilder

- **RelationResolution**: `/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/RelationResolution.scala`
  - ResolveRelations rule: L73-233
  - createRelation(): Creates DataSourceV2Relation

- **FileSourceScanExec**: `/spark/sql/core/src/main/scala/org/apache/spark/sql/execution/FileSourceScanExec.scala`
  - doExecute(): L519 (creates file scan RDD)

### Delta Lake - Core

- **AbstractDeltaSparkSessionExtension**: `/delta/spark/src/main/scala/io/delta/sql/AbstractDeltaSparkSessionExtension.scala`
  - apply(): L48-143 (injects all Delta rules)
  - Parser injection: L49
  - Resolution rules: L52-64
  - Check rules: L68-72
  - Optimizer rules: L77-81
  - PreCBO rules: L100-105
  - Planner strategy: L108

- **DeltaSqlParser**: `/delta/spark/src/main/scala/io/delta/sql/parser/DeltaSqlParser.scala`
  - Parses Delta SQL syntax (time travel, commands, TVFs)

- **ResolveDeltaPathTable**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/ResolveDeltaPathTable.scala`
  - apply(): L33 (resolves path-based table references)

- **PreprocessTimeTravel**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/PreprocessTimeTravel.scala`
  - apply(): L41 (resolves time travel queries)

- **DeltaAnalysis**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/DeltaAnalysis.scala`
  - apply(): L87 (resolution rule that does V1 fallback)
  - FallbackToV1DeltaRelation pattern: L491

- **FallbackToV1DeltaRelation**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/FallbackToV1Relations.scala`
  - unapply(): L28-32 (pattern matcher for fallback)

- **DeltaTableV2**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/catalog/DeltaTableV2.scala`
  - Class definition: L73
  - capabilities(): L127 (declares BATCH_READ but doesn't implement SupportsRead)
  - toBaseRelation: L305 (creates V1 HadoopFsRelation)

- **DeltaCatalog**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/catalog/DeltaCatalog.scala`
  - loadTable(): L113 (loads DeltaTableV2 from catalog)

- **DeltaSparkSessionExtension**: `/delta/spark-unified/src/main/scala/io/delta/sql/DeltaSparkSessionExtension.scala`
  - Injects DeltaAnalysis into customResolutionRules

### Delta Lake - File Index

- **TahoeFileIndex**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/files/TahoeFileIndex.scala`
  - Base class for Delta file indices
  - toString(): L177 (returns "Delta[version=X, path]")

- **TahoeLogFileIndex**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/files/TahoeFileIndex.scala`
  - matchingFiles(): Reads Delta Log for file listing
  - Contains ALL files in the table (before PrepareDeltaScan)

- **TahoeFileIndexWithSnapshotDescriptor**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/files/TahoeFileIndex.scala`
  - Base class for file indices with specific snapshot

- **PreparedDeltaFileIndex**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/stats/PrepareDeltaScan.scala`
  - L343-419
  - matchingFiles(): L356 (returns pre-computed pruned file list)
  - inputFiles: L402 (used by explain)
  - sizeInBytes: L409 (used by explain and CBO)

### Delta Lake - Optimization

- **PrepareDeltaScan**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/stats/PrepareDeltaScan.scala`
  - apply(): L141 (main entry point)
  - getDeltaScanGenerator(): L77 (snapshot isolation)
  - filesForScan(): L114 (file pruning and data skipping)
  - getPreparedIndex(): L94 (creates PreparedDeltaFileIndex)

- **RangePartitionIdRewrite**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/optimizer/RangePartitionIdRewrite.scala`
  - apply(): L43 (rewrites range partition expressions)

- **OptimizeConditionalIncrementMetric**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/metric/IncrementMetric.scala`
  - apply(): L183 (optimizes metric expressions)

### Delta Lake - Special Features

- **DeltaParquetFileFormat**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/DeltaParquetFileFormat.scala`
  - buildReaderWithPartitionValues(): Adds deletion vector support
  - Handles column mapping

- **DeltaTableValueFunctions**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/DeltaTableValueFunctions.scala`
  - Defines table_changes() and table_changes_by_path() functions

- **CDCReader**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/commands/cdc/CDCReader.scala`
  - Reads change data capture rows

- **PreprocessTableWithDVsStrategy**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/PreprocessTableWithDVsStrategy.scala`
  - Physical planning for deletion vectors

- **CheckUnresolvedRelationTimeTravel**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/CheckUnresolvedRelationTimeTravel.scala`
  - Validation for time travel queries

- **DeltaUnsupportedOperationsCheck**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/DeltaUnsupportedOperationsCheck.scala`
  - Validation for unsupported operations
