# Life of a query of Delta

[Xin Huang](mailto:xin.huang@databricks.com)  
This doc tries to capture key pieces of how planning works for Delta connector today, keep evolving for more operations 

## Batch Write (Append Operations)

TL;DR

```
User API:
  df.write.format("delta").mode("append").save("/path/to/table")
  df.write.mode("overwrite").saveAsTable("table")
  df.writeTo("table").append()
  └─> Creates write command logical plan
  
Analysis Phase:
  1. DataFrameWriter API determines write command type:
     V1: InsertIntoStatement, CreateTableAsSelectStatement
     V2: AppendData, OverwriteByExpression, OverwritePartitionsDynamic
  
  2. Resolution:
     - Table resolution (same as read)
     - Schema validation
     - ResolveInsertInto, PreprocessTableInsertion
  
  3. DeltaAnalysis (customResolutionRules):
     V2 Commands (AppendData, OverwriteByExpression) 
     └─> Pattern: AppendDelta/OverwriteDelta
         └─> Keep as V2 (set KEEP_AS_V2_RELATION_TAG for target)
         └─> Transform to Delta-specific write plans
  
Command Execution:
  Execute V2 write command (e.g., AppendData)
  └─> DeltaTableV2.newWriteBuilder()
      └─> WriteIntoDeltaBuilder.build()
          └─> Returns V1WriteCommand wrapping WriteIntoDelta
  
  └─> WriteIntoDelta.run()
      ├─> Start OptimisticTransaction
      ├─> Write data files (Parquet)
      │   └─> TransactionalWrite.writeFiles()
      ├─> Collect AddFile actions
      └─> Transaction.commit()
          ├─> Write Delta Log JSON
          ├─> Create checkpoint (if needed)
          └─> Atomically commit
  
Result:
  - Data files written to table path
  - Delta Log updated with new version
  - Transaction committed or retried on conflict
```

### 1. Entry Point

Delta batch write (append operations) has multiple entry points through DataFrameWriter API:

**Note**: This document covers Append operations only. For DML operations (UPDATE, DELETE, MERGE), see `delta-dml-operations.md`.

**API 1: format().save() (V1 API)**
```scala
// User code
df.write
  .format("delta")
  .mode("append")  // or "overwrite", "ignore", "error"
  .save("/path/to/delta/table")

// Implementation: DataFrameWriter.scala
def save(path: String): Unit = {
  val dataSource = DataSource(
    sparkSession,
    className = source,  // "delta"
    options = extraOptions,
    userSpecifiedSchema = None
  )
  dataSource.planForWriting(mode, Dataset.ofRows(sparkSession, df.logicalPlan))
}
```

**API 2: saveAsTable() (V1 API)**
```scala
// User code
df.write
  .mode("overwrite")
  .saveAsTable("catalog.schema.table")

// Creates InsertIntoStatement or CreateTableAsSelectStatement
```

**API 3: writeTo() (V2 API)**
```scala
// User code - Append
df.writeTo("table").append()

// User code - Overwrite with condition
df.writeTo("table").overwrite($"date" === "2024-01-01")

// User code - Overwrite partitions dynamically
df.writeTo("table").overwritePartitions()

// Implementation: DataFrameWriterV2.scala
def append(): Unit = {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  val append = AppendData.byName(
    sparkSession.sessionState.analyzer.ResolveTables(table),
    df.logicalPlan
  )
  runCommand(append)
}
```

### 2. Query Planning

#### 2.1 Analysis Phase - Create Write Command

The write API creates different logical plan nodes based on API and mode:

**V1 Write Commands** (format().save(), saveAsTable()):
```scala
// InsertIntoStatement for existing tables
case InsertIntoStatement(
    table: LogicalPlan,           // UnresolvedRelation
    partitionSpec: Map[String, Option[String]],
    userSpecifiedCols: Seq[String],
    query: LogicalPlan,           // The DataFrame to write
    overwrite: Boolean,           // Based on mode
    ifPartitionNotExists: Boolean
)

// CreateTableAsSelectStatement for new tables
case CreateTableAsSelectStatement(
    tableName: Seq[String],
    query: LogicalPlan,
    properties: Map[String, String],
    provider: Option[String],     // "delta"
    options: Map[String, String],
    location: Option[String],
    ifNotExists: Boolean
)
```

**V2 Write Commands** (writeTo() API):
```scala
// AppendData - for append mode
case class AppendData(
    table: LogicalPlan,           // UnresolvedRelation or ResolvedTable
    query: LogicalPlan,           // The DataFrame to write
    writeOptions: Map[String, String],
    isByName: Boolean             // true for byName(), false for byPosition()
) extends V2WriteCommand

// OverwriteByExpression - for overwrite with filter
case class OverwriteByExpression(
    table: LogicalPlan,
    deleteExpr: Expression,       // Filter condition
    query: LogicalPlan,
    writeOptions: Map[String, String],
    isByName: Boolean
) extends V2WriteCommand

// OverwritePartitionsDynamic - for overwritePartitions()
case class OverwritePartitionsDynamic(
    table: LogicalPlan,
    query: LogicalPlan,
    writeOptions: Map[String, String],
    isByName: Boolean
) extends V2WriteCommand
```

#### 2.2 Analysis Phase - Resolution

**Standard Spark Resolution Rules**:

1. **ResolveRelations**: Resolves table references
   ```
   UnresolvedRelation("table")
     └─> DeltaCatalog.loadTable()
         └─> ResolvedTable(DeltaTableV2)
   ```

2. **ResolveInsertInto**: Resolves InsertIntoStatement
   ```scala
   // Validates target table exists
   // Resolves partition specifications
   ```

3. **PreprocessTableInsertion**: Schema validation and alignment
   ```scala
   // Checks schema compatibility
   // Adds necessary casts
   // Reorders columns if needed
   ```

**Delta Custom Resolution Rules**:

**DeltaAnalysis** (customResolutionRules) handles V2 write commands:

```scala
// DeltaAnalysis.scala
class DeltaAnalysis(session: SparkSession) extends Rule[LogicalPlan] {
  
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    
    // Handle AppendData for Delta tables
    case a @ AppendData(r: ResolvedTable, query, _, isByName)
        if r.table.isInstanceOf[DeltaTableV2] =>
      // Keep as V2 - Set tag to prevent fallback
      r.setTagValue(DeltaRelation.KEEP_AS_V2_RELATION_TAG, ())
      
      // Perform Delta-specific schema evolution, type casting
      val projection = if (isByName) {
        resolveQueryColumnsByName(query, r.output, deltaTable, writeOptions)
      } else {
        resolveQueryColumnsByOrdinal(query, r.output, deltaTable, writeOptions)
      }
      a.copy(query = projection)
    
    // Handle OverwriteByExpression for Delta tables
    case o @ OverwriteByExpression(r: ResolvedTable, deleteExpr, query, _, isByName)
        if r.table.isInstanceOf[DeltaTableV2] =>
      r.setTagValue(DeltaRelation.KEEP_AS_V2_RELATION_TAG, ())
      // Similar schema handling
      o.copy(query = resolvedQuery)
    
    // Handle OverwritePartitionsDynamic for Delta tables
    case o @ OverwritePartitionsDynamic(r: ResolvedTable, query, _, isByName)
        if r.table.isInstanceOf[DeltaTableV2] =>
      r.setTagValue(DeltaRelation.KEEP_AS_V2_RELATION_TAG, ())
      o.copy(query = resolvedQuery)
  }
}
```

**Key Difference from Batch Read**:
- **Batch Read**: NO tag → fallback to V1
- **Batch Write**: Tag IS set → **keep as V2**

### 3. Command Execution

After analysis, the V2 write command is executed:

#### 3.1 V2 Write Builder

**DeltaTableV2.newWriteBuilder()** creates a write builder:

```scala
// DeltaTableV2.scala
override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
  new WriteIntoDeltaBuilder(
    this,                          // DeltaTableV2 instance
    info.options,                  // Write options
    conf.useNullsForMissingDefaultColumnValues
  )
}
```

**WriteIntoDeltaBuilder** determines the write operation:

```scala
// DeltaTableV2.scala
private class WriteIntoDeltaBuilder(
    table: DeltaTableV2,
    writeOptions: Map[String, String],
    useNullsForMissing: Boolean
) extends WriteBuilder with SupportsOverwrite with SupportsTruncate {
  
  private var forceOverwrite = false
  private var overwriteByFilter: Option[Expression] = None
  
  // For AppendData
  override def build(): Write = {
    V1Write(
      WriteIntoDelta(
        deltaLog = table.deltaLog,
        mode = SaveMode.Append,
        options = DeltaOptions(writeOptions, conf),
        partitionColumns = table.partitionColumns,
        configuration = table.properties,
        data = <query plan>
      )
    )
  }
  
  // For OverwriteByExpression
  override def overwrite(filter: Expression): WriteBuilder = {
    this.overwriteByFilter = Some(filter)
    this.forceOverwrite = true
    this
  }
  
  // For OverwritePartitionsDynamic
  override def overwriteDynamicPartitions(): WriteBuilder = {
    this.forceOverwrite = true
    this
  }
}
```

#### 3.2 WriteIntoDelta Command

**WriteIntoDelta** is the main Delta write command:

```scala
// WriteIntoDelta.scala
case class WriteIntoDelta(
    deltaLog: DeltaLog,
    mode: SaveMode,                  // Append, Overwrite, etc.
    options: DeltaOptions,
    partitionColumns: Seq[String],
    configuration: Map[String, String],
    data: LogicalPlan                // Query to write
) extends DeltaCommand with V1WriteCommand {
  
  override def run(sparkSession: SparkSession): Seq[Row] = {
    deltaLog.withNewTransaction { txn =>
      // Start optimistic transaction
      val actions = write(txn, sparkSession)
      
      // Commit transaction
      val operation = DeltaOperations.Write(
        mode,
        Option(partitionColumns),
        options.replaceWhere,
        options.userMetadata
      )
      txn.commit(actions, operation)
    }
    Seq.empty
  }
}
```

#### 3.3 Optimistic Transaction

**OptimisticTransaction** manages the write transaction:

```scala
// OptimisticTransaction.scala
class OptimisticTransaction(
    override val deltaLog: DeltaLog,
    override val snapshot: Snapshot
) extends OptimisticTransactionImpl {
  
  // Transaction lifecycle:
  // 1. Read current state (snapshot)
  // 2. Perform operation (write, update, delete, etc.)
  // 3. Collect actions (AddFile, RemoveFile, etc.)
  // 4. Commit atomically
  
  def commit(actions: Seq[Action], op: DeltaOperations.Operation): Long = {
    // Prepare commit
    val preparedActions = prepareCommit(actions, op)
    
    // Attempt commit
    val (version, postCommitSnapshot, actionsToCommit) = try {
      doCommit(preparedActions, op)
    } catch {
      case e: DeltaConcurrentModificationException =>
        // Conflict detected - retry or fail
        if (canRetry) {
          // Update to latest snapshot and retry
          retry
        } else {
          throw e
        }
    }
    
    // Post commit hooks
    postCommit(version, postCommitSnapshot)
    
    version
  }
  
  protected def doCommit(
      actions: Seq[Action],
      op: DeltaOperations.Operation
  ): (Long, Snapshot, Seq[Action]) = {
    val version = snapshot.version + 1
    
    // Write Delta Log JSON file
    val commitFile = deltaLog.logPath.resolve(s"$version.json")
    writeActionsToJson(actions, commitFile)
    
    // Create checkpoint if needed
    if (shouldCheckpoint(version)) {
      deltaLog.checkpoint()
    }
    
    (version, deltaLog.getSnapshotAt(version), actions)
  }
}
```

### 4. File Writing

#### 4.1 TransactionalWrite

**TransactionalWrite** handles the actual file writing:

```scala
// TransactionalWrite.scala
trait TransactionalWrite {
  
  def writeFiles(
      data: Dataset[_],
      writeOptions: Option[DeltaOptions],
      additionalConstraints: Seq[Constraint]
  ): Seq[AddFile] = {
    
    // 1. Prepare data for writing
    val queryExecution = data.queryExecution
    val outputPath = deltaLog.dataPath
    
    // 2. Determine partitioning
    val partitioningColumns = partitionColumns.map(c => data.col(c))
    
    // 3. Write files using FileFormatWriter
    val addedFiles = sparkSession.sparkContext.runJob(queryExecution.toRdd, 
      (taskContext: TaskContext, iterator: Iterator[InternalRow]) => {
        // Write one or more files per task
        val writer = new DeltaTaskWriter(outputPath, partitioningColumns)
        while (iterator.hasNext) {
          writer.write(iterator.next())
        }
        writer.close()  // Returns AddFile actions
      }
    ).flatten
    
    // 4. Collect statistics
    addedFiles.map { addFile =>
      addFile.copy(stats = computeStats(addFile))
    }
  }
}
```

#### 4.2 File Writing Process

**Per-Task File Writing**:
```
Each Spark task:
  1. Creates output file(s) in table path
     └─> File naming: part-00000-{uuid}.snappy.parquet
  
  2. Writes rows using ParquetOutputWriter
     └─> Optionally partitioned by partition columns
  
  3. Collects file metadata:
     - File path
     - Size (bytes)
     - Partition values
     - Row count
     - Column statistics (min, max, null_count)
  
  4. Returns AddFile action(s)
```

**File Layout**:
```
/path/to/table/
├─ _delta_log/
│  ├─ 00000000000000000000.json    # Initial version
│  ├─ 00000000000000000001.json    # After first write
│  ├─ ...
│  └─ 00000000000000000010.checkpoint.parquet
├─ part-00000-xxx.snappy.parquet   # Data files
├─ part-00001-xxx.snappy.parquet
└─ date=2024-01-01/                # Partitioned data
   ├─ part-00000-yyy.snappy.parquet
   └─ part-00001-yyy.snappy.parquet
```

### 5. Transaction Commit

#### 5.1 Delta Log Actions

The transaction collects actions to commit:

**AddFile Action**:
```scala
case class AddFile(
    path: String,                      // Relative path to data file
    partitionValues: Map[String, String], // Partition column values
    size: Long,                        // File size in bytes
    modificationTime: Long,            // File modification timestamp
    dataChange: Boolean,               // Whether this changes table data
    stats: String,                     // JSON statistics (min, max, null_count)
    tags: Map[String, String],         // Optional file tags
    deletionVector: DeletionVectorDescriptor, // Optional deletion vector
    baseRowId: Option[Long],           // Row tracking (if enabled)
    defaultRowCommitVersion: Option[Long]
) extends Action
```

**Other Actions**:
```scala
// Remove files (for Overwrite)
case class RemoveFile(
    path: String,
    deletionTimestamp: Option[Long],
    dataChange: Boolean
) extends Action

// Update metadata (schema changes)
case class Metadata(
    schemaString: String,
    partitionColumns: Seq[String],
    configuration: Map[String, String],
    ...
) extends Action

// Protocol version
case class Protocol(
    minReaderVersion: Int,
    minWriterVersion: Int,
    readerFeatures: Set[String],
    writerFeatures: Set[String]
) extends Action
```

#### 5.2 Commit Process

**Atomic Commit**:
```
1. Prepare commit file:
   - File name: {version}.json
   - Contents: JSON lines of actions
   - Example:
     {"add":{"path":"part-00000.parquet","size":1024,...}}
     {"add":{"path":"part-00001.parquet","size":2048,...}}

2. Write commit file atomically:
   - Write to temp file
   - Atomic rename to final name
   - File system guarantees atomicity

3. Conflict detection:
   - If version file already exists → concurrent write detected
   - Read conflicting commit
   - Check if conflict is resolvable
   - Retry with new version if possible

4. Post-commit:
   - Update cached snapshot
   - Trigger checkpoint if needed
   - Run post-commit hooks
```

**Checkpointing**:
```
Every N commits (default 10):
  1. Read all Delta Log JSON files from 0 to current version
  2. Aggregate state (current AddFiles, Metadata, Protocol)
  3. Write checkpoint Parquet file:
     - {version}.checkpoint.parquet
  4. Allows readers to skip reading individual JSON files
```

### 6. Write Modes

**SaveMode.Append**:
```
- Adds new data files
- No existing files removed
- Actions: AddFile only
```

**SaveMode.Overwrite (full table)**:
```
- Removes all existing data files
- Adds new data files
- Actions: RemoveFile (all existing) + AddFile (new)
```

**SaveMode.Overwrite (with replaceWhere)**:
```scala
df.write
  .format("delta")
  .mode("overwrite")
  .option("replaceWhere", "date >= '2024-01-01'")
  .save("/path/to/table")

// Only removes files matching condition
// Actions: RemoveFile (matching) + AddFile (new)
```

**Dynamic Partition Overwrite**:
```scala
df.writeTo("table").overwritePartitions()

// Identifies partitions in new data
// Removes only those partitions
// Adds new data files
// Actions: RemoveFile (affected partitions) + AddFile (new)
```

---

## Summary: Delta Batch Write DSv2 Path

### Answer: **Hybrid - V2 Commands, V1 Execution**

**DSv2 Usage (Retained through execution)**:
* ✅ V2 write commands (AppendData, OverwriteByExpression, etc.)
* ✅ `DeltaTableV2` implements `SupportsWrite`
* ✅ `KEEP_AS_V2_RELATION_TAG` prevents fallback
* ✅ V2 WriteBuilder (WriteIntoDeltaBuilder)

**V1 Execution (Wrapped in V2)**:
* ⚠️ Returns `V1Write` wrapping `WriteIntoDelta` command
* ⚠️ Actual execution uses Delta-specific command pattern
* ⚠️ Not pure DSv2 write interfaces (no `DataWriter`, `DataWriterFactory`)

**Why Hybrid?**

```scala
// DeltaTableV2.scala
override def capabilities() = Set(
  ACCEPT_ANY_SCHEMA,
  BATCH_READ,
  V1_BATCH_WRITE,      // ← Declares V1 write support!
  OVERWRITE_BY_FILTER,
  TRUNCATE,
  OVERWRITE_DYNAMIC
)
```

**Write Flow**:
```
V2 Command (AppendData)
  └─> DeltaTableV2.newWriteBuilder()
      └─> WriteIntoDeltaBuilder
          └─> Returns V1Write wrapping WriteIntoDelta
              └─> OptimisticTransaction
                  ├─> TransactionalWrite (file writing)
                  └─> Commit to Delta Log
```

**Key Difference from Batch Read**:
- **Read**: Immediate V1 fallback in Analysis
- **Write**: Stays as V2 command, wrapped V1 execution

---

## Key Files Reference

### Spark Core - Write API

- **DataFrameWriter**: `/spark/sql/core/src/main/scala/org/apache/spark/sql/classic/DataFrameWriter.scala`
  - save(): Creates write commands
  - saveAsTable(): Creates InsertIntoStatement

- **DataFrameWriterV2**: `/spark/sql/core/src/main/scala/org/apache/spark/sql/classic/DataFrameWriterV2.scala`
  - append(): Creates AppendData
  - overwrite(): Creates OverwriteByExpression
  - overwritePartitions(): Creates OverwritePartitionsDynamic

- **V2 Write Commands**: `/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/v2Commands.scala`
  - AppendData: L106
  - OverwriteByExpression: L139
  - OverwritePartitionsDynamic: L184

### Delta Lake - Write Implementation

- **DeltaTableV2**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/catalog/DeltaTableV2.scala`
  - newWriteBuilder(): L271 (creates WriteIntoDeltaBuilder)
  - WriteIntoDeltaBuilder: L567 (inner class)

- **WriteIntoDelta**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/commands/WriteIntoDelta.scala`
  - Main write command: L80
  - run(): Executes write with transaction

- **OptimisticTransaction**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/OptimisticTransaction.scala`
  - Transaction management
  - commit(): Atomic commit with conflict detection

- **TransactionalWrite**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/commands/WriteIntoDelta.scala`
  - writeFiles(): File writing logic
  - Handles partitioning, statistics, file creation

- **DeltaOperations**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/DeltaOperations.scala`
  - Operation definitions: L44
  - Used for commit metadata and history

- **Actions**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/actions/actions.scala`
  - AddFile, RemoveFile, Metadata, Protocol, etc.
  - Serialized to Delta Log JSON

### Delta Lake - Analysis

- **DeltaAnalysis**: `/delta/spark/src/main/scala/org/apache/spark/sql/delta/DeltaAnalysis.scala`
  - Handles V2 write commands
  - Schema evolution and validation
  - Sets KEEP_AS_V2_RELATION_TAG


