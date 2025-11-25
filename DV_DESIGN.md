# Deletion Vector Support in DSv2 Connector - Design Doc

## Problem Statement

DSv2 connector needs to support Deletion Vectors (DV) to correctly filter out deleted rows when reading Delta tables. Currently:

- **DSv1**: Uses `PreprocessTableWithDVs` analyzer rule to inject `__delta_internal_is_row_deleted` column
- **DSv2**: Does not use analyzer rules (by design), so DV filtering is not applied
- **Result**: DSv2 returns more rows than DSv1 for tables with DVs

## Design Principles

1. **Self-contained**: DSv2 should not rely on Spark analyzer rules
2. **Minimal changes**: Reuse existing `DeltaParquetFileFormatBase` infrastructure  
3. **Transparent**: DV handling should be invisible to users (internal column not exposed)
4. **Correct by default**: Always enable DV support if table protocol allows it

## Analysis: Why Step 1 (Detection) is Actually NOT Needed

### Key Insight from DeltaParquetFileFormatBase

```scala
// Line 342-344 in DeltaParquetFileFormat.scala
} else {
  KeepAllRowsFilter  // ← Used when file has NO DV metadata
}
```

**This means:**
- If a file has DV metadata → Apply DV filtering
- If a file has NO DV metadata → Use `KeepAllRowsFilter` (keep all rows)
- **Mixed files work correctly!** No need to detect at batch level

### Proposed Solution: Simplified Approach

**Always add `__delta_internal_is_row_deleted` column if table supports DVs**, regardless of whether current files have DVs.

### Architecture

```
SparkBatch.createReaderFactory()
    ↓
1. Check if table protocol supports DV feature
    ↓
2. If yes, add __delta_internal_is_row_deleted to read schema
    ↓
3. Pass augmented schema to DeltaParquetFileFormatV2
    ↓
4. DeltaParquetFileFormatBase handles each file:
    - File with DV → Apply DropMarkedRowsFilter
    - File without DV → Use KeepAllRowsFilter
    ↓
5. Wrap reader to filter rows + remove internal column
```

### Implementation Steps

#### Step 1: Check Protocol Support (Not File-Level Detection!)

In `SparkBatch.createReaderFactory()`:
```java
// Check if table PROTOCOL supports DV feature (not per-file)
Protocol protocol = ((SnapshotImpl) initialSnapshot).getProtocol();
boolean tableSupportsDV = protocol.getReaderFeatures().contains("deletionVectors");
```

**Rationale:**
- More robust: Works even if current files don't have DVs
- Future-proof: New files with DVs will work correctly
- Simpler: No need to iterate through all partitionedFiles
- Correct: DVs can be added later to any file

#### Step 2: Augment Read Schema

If table supports DVs, add internal column to `readDataSchema`:
```java
StructType augmentedSchema = tableSupportsDV 
    ? addInternalDVColumn(readDataSchema)
    : readDataSchema;
    
private StructType addInternalDVColumn(StructType schema) {
  StructField dvColumn = DataTypes.createStructField(
      "__delta_internal_is_row_deleted",
      DataTypes.ByteType,
      false);  // not nullable
  return schema.add(dvColumn);
}
```

#### Step 3: Filter Output

Wrap the reader to:
- Filter out rows where `__delta_internal_is_row_deleted != 0`
- Project out the internal column from output

#### Step 4: Use Metadata Row Index (Optional Optimization)

Enable `useMetadataRowIndex` optimization for better performance with predicate pushdown.

## Why Analyzer Rule Doesn't Work for DSv2

### V1 vs V2 Architecture Difference

**V1 (LogicalRelation)**:
- Uses `LogicalRelation` with mutable `HadoopFsRelation`
- Has `FileFormat` concept that can be replaced
- Schema can be modified via relation
- `PreprocessTableWithDVs` works by:
  1. Pattern match `LogicalRelation` 
  2. Replace `FileFormat` with `DeltaParquetFileFormat`
  3. Add `__delta_internal_is_row_deleted` to dataSchema
  4. Inject Filter + Project nodes

**V2 (DataSourceV2Relation)**:
- Uses `DataSourceV2Relation` (immutable)
- Schema comes from `Table.schema()` (fixed before analyzer)
- No `FileFormat` concept, uses `Batch`/`PartitionReaderFactory`
- **Cannot modify schema in analyzer phase!**

### Why V1's Rule Cannot Be Adapted

| V1 Dependency | DSv2 Equivalent | Can Reuse? |
|---------------|-----------------|------------|
| `LogicalRelation` | `DataSourceV2Relation` | ❌ Different types |
| `HadoopFsRelation` | No equivalent (uses `Table`) | ❌ |
| `TahoeFileIndex` | No equivalent (metadata in `Snapshot`) | ❌ |
| `DeltaParquetFileFormat` | `DeltaParquetFileFormatV2` | ✅ Can reuse! |
| Modify dataSchema | Schema from `Table.schema()` | ❌ Already fixed |

**Conclusion**: V1's analyzer rule is **deeply coupled to V1 APIs** and cannot work with DSv2.

## Why This Design is Superior

### Comparison with Alternatives

| Approach | Pros | Cons | Feasible? | Decision |
|----------|------|------|-----------|----------|
| **SparkBatch-level (Proposed)** | ✅ Works with DSv2<br>✅ Self-contained<br>✅ Reuses DeltaParquetFileFormatBase | ⚠️ Adds column even if no DVs | ✅ Yes | **CHOSEN** |
| **Table.schema() level** | ✅ Simple | ❌ Exposes internal column to users<br>❌ Breaks API contract | ⚠️ Possible but bad UX | Rejected |
| **V1 Analyzer Rule** | ✅ Proven (DSv1) | ❌ Only works with `LogicalRelation`<br>❌ Incompatible with DSv2 | ❌ No | **Not Possible** |
| **New DSv2 Analyzer Rule** | ✅ Familiar pattern | ❌ Cannot modify schema (from `Table.schema()`)<br>❌ No FileFormat in DSv2 | ❌ No | **Not Possible** |
| **ColumnVectorWithFilter (Iceberg)** | ✅ Zero-copy<br>✅ Pure DSv2 | ❌ High complexity<br>❌ New implementation | ✅ Yes | Future work |

### Key Advantages

1. **Reuses 100% of existing DeltaParquetFileFormatBase logic**
   - No need to reimplement DV filtering
   - No risk of semantic divergence from DSv1
   
2. **Self-contained within DSv2 connector**
   - No Spark analyzer rules
   - No global state
   - Clean separation of concerns

3. **Handles all edge cases automatically**
   - Mixed files (some with DVs, some without)
   - Files that get DVs added later
   - Checkpoint with updated DVs

4. **Minimal overhead**
   - One extra byte column per row (filtered before output)
   - DV filtering happens in Parquet reader (efficient)

## Implementation Plan

### What's Already Done ✅

1. ✅ Add DV metadata to `PartitionedFile` in `SparkScan.buildMetadataMap()`
2. ✅ `DeltaParquetFileFormatV2` created and integrated in `SparkBatch`
3. ✅ Metadata passed through `otherConstantMetadataColumnValues`

### What Needs to Be Done ⬜

1. ⬜ Check protocol for DV support in `SparkBatch.createReaderFactory()`
2. ⬜ Augment read schema with `__delta_internal_is_row_deleted` column
3. ⬜ Wrap reader to filter deleted rows and project out internal column
4. ⬜ Remove debug logging from `SparkScan`
5. ⬜ Add/update tests

## Detailed Implementation

### Code Changes Required

#### 1. SparkBatch.java

```java
@Override
public PartitionReaderFactory createReaderFactory() {
    // Check if table protocol supports deletion vectors
    Protocol protocol = ((SnapshotImpl) initialSnapshot).getProtocol();
    Metadata metadata = ((SnapshotImpl) initialSnapshot).getMetadata();
    boolean hasDVSupport = deletionVectorsReadable(protocol, metadata);
    
    // Augment schema if DVs are supported
    StructType augmentedReadSchema = hasDVSupport 
        ? addDVColumn(readDataSchema) 
        : readDataSchema;
    StructType augmentedDataSchema = hasDVSupport
        ? addDVColumn(dataSchema)
        : dataSchema;
    
    boolean enableVectorizedReader =
        ParquetUtils.isBatchReadSupportedForSchema(sqlConf, augmentedReadSchema);
        
    // ... create file format and reader ...
    
    Function1<PartitionedFile, Iterator<InternalRow>> readFunc =
        fileFormat.buildReaderWithPartitionValues(
            SparkSession.active(),
            augmentedDataSchema,      // ← Use augmented schema
            partitionSchema,
            augmentedReadSchema,      // ← Use augmented schema
            JavaConverters.asScalaBuffer(Arrays.asList(dataFilters)).toSeq(),
            optionsWithBatch,
            hadoopConf);
    
    // Wrap reader to filter + project if DVs supported
    if (hasDVSupport) {
        readFunc = wrapReaderForDV(readFunc, readDataSchema);
    }
    
    return new SparkReaderFactory(readFunc, enableVectorizedReader);
}

private boolean deletionVectorsReadable(Protocol protocol, Metadata metadata) {
    // Check if protocol has deletionVectors reader feature
    scala.collection.Set<String> readerFeatures = protocol.readerFeatures();
    return readerFeatures != null && 
           readerFeatures.contains("deletionVectors") &&
           "parquet".equals(metadata.format().provider());
}

private StructType addDVColumn(StructType schema) {
    StructField dvColumn = DataTypes.createStructField(
        "__delta_internal_is_row_deleted",
        DataTypes.ByteType,
        false);  // not nullable
    return schema.add(dvColumn);
}

private Function1<PartitionedFile, Iterator<InternalRow>> wrapReaderForDV(
        Function1<PartitionedFile, Iterator<InternalRow>> baseReader,
        StructType originalSchema) {
    return (PartitionedFile pf) -> {
        Iterator<InternalRow> iter = baseReader.apply(pf);
        return new DVFilteringIterator(iter, originalSchema);
    };
}
```

#### 2. DVFilteringIterator (New Helper Class)

```java
private static class DVFilteringIterator extends AbstractIterator<InternalRow> {
    private final Iterator<InternalRow> baseIterator;
    private final StructType outputSchema;
    private final int numOutputFields;
    
    public DVFilteringIterator(Iterator<InternalRow> baseIterator, StructType outputSchema) {
        this.baseIterator = baseIterator;
        this.outputSchema = outputSchema;
        this.numOutputFields = outputSchema.fields().length;
    }
    
    @Override
    protected InternalRow computeNext() {
        while (baseIterator.hasNext()) {
            InternalRow row = baseIterator.next();
            
            // Check __delta_internal_is_row_deleted column (last field)
            int dvColumnIndex = row.numFields() - 1;
            byte isDeleted = row.getByte(dvColumnIndex);
            
            if (isDeleted == 0) {
                // Row not deleted, project out the DV column
                return projectRow(row);
            }
            // Row is deleted, skip it
        }
        return endOfData();
    }
    
    private InternalRow projectRow(InternalRow row) {
        // Remove last column (__delta_internal_is_row_deleted)
        Object[] values = new Object[numOutputFields];
        for (int i = 0; i < numOutputFields; i++) {
            values[i] = row.get(i, outputSchema.fields()[i].dataType());
        }
        return InternalRow.fromSeq(
            JavaConverters.asScalaIterator(Arrays.asList(values).iterator()).toSeq());
    }
}
```

#### 3. Test Updates

Remove from unsupported list in `SparkGoldenTableTest.java`:
```java
// Remove these from unsupportedTables:
// "dv-partitioned-with-checkpoint",
// "dv-with-columnmapping"
```

## Open Questions

1. **Performance**: Should we use `useMetadataRowIndex` optimization?
   - Pro: Better performance with predicate pushdown
   - Con: Requires `_metadata.row_index` column support
   - **Decision**: Start without it, add as optimization later

2. **Schema exposure**: Should internal column be visible in `Scan.readSchema()`?
   - **Decision**: No, handle internally in `SparkBatch`, not exposed in API

3. **Vectorized vs Row-based**: How to handle both reader modes?
   - **Decision**: `DVFilteringIterator` works with `InternalRow`, handles both

## References

- DSv1 implementation: `PreprocessTableWithDVs.scala`
- DV filtering logic: `DeltaParquetFileFormatBase.scala` (lines 321-345)
- Iceberg approach: Mentioned in roadmap doc (zero-copy filtering with ColumnVectorWithFilter)
- Protocol check: `DeletionVectorUtils.deletionVectorsReadable()` in DSv1

