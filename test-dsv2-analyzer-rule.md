# Testing: Can Analyzer Rule Work for DSv2?

## Key Findings

### 1. V1 vs V2 Logical Plan Nodes

**V1 (LogicalRelation)**:
```scala
// PreprocessTableWithDVs.scala line 61
def unapply(a: LogicalRelation): Option[LogicalPlan] = a match {
  case scan @ LogicalRelationWithTable(
    relation @ HadoopFsRelation(
      index: TahoeFileIndex, _, _, _, format: DeltaParquetFileFormat, _), _) =>
    dvEnabledScanFor(scan, relation, format, index)
  case _ => None
}
```

**V2 (DataSourceV2Relation)**:
```scala
// FallbackToV1Relations.scala line 28
def unapply(dsv2: DataSourceV2Relation): Option[LogicalRelation] = dsv2.table match {
  case d: DeltaTableV2 if dsv2.getTagValue(DeltaRelation.KEEP_AS_V2_RELATION_TAG).isEmpty =>
    Some(DeltaRelation.fromV2Relation(d, dsv2, dsv2.options))
  case _ => None
}
```

### 2. V1 Rule Dependencies (All V1-specific!)

- `LogicalRelation` - V1 logical plan node
- `HadoopFsRelation` - V1 file relation
- `TahoeFileIndex` - V1 file index
- `DeltaParquetFileFormat` - V1 file format
- **Result**: Cannot reuse V1 rule for DSv2

### 3. Could We Create a DSv2 Analyzer Rule?

**Theoretical Approach**:
```scala
object PreprocessDSv2TableWithDVs {
  def unapply(relation: DataSourceV2Relation): Option[LogicalPlan] = relation.table match {
    case sparkTable: SparkTable => // kernel-spark's DSv2 table
      // Problem: How to inject column into DataSourceV2Relation?
      // DataSourceV2Relation is immutable and doesn't have FileFormat concept
      ???
  }
}
```

**Blockers**:
1. **Schema is immutable**: `DataSourceV2Relation` schema comes from `Table.schema()`, can't be modified by analyzer rule
2. **No FileFormat concept**: DSv2 uses `Batch` and `PartitionReaderFactory`, not `FileFormat`
3. **Column injection is harder**: Can't just add a field like V1's `HadoopFsRelation`

### 4. Why Analyzer Rule Doesn't Fit DSv2

| Aspect | V1 (LogicalRelation) | V2 (DataSourceV2Relation) | Can Rule Work? |
|--------|---------------------|---------------------------|----------------|
| **Schema Modification** | Mutable via `HadoopFsRelation` | Immutable from `Table.schema()` | ❌ |
| **File Format** | `FileFormat` interface | No file format, uses `Batch` | ❌ |
| **Reader Control** | Replace `FileFormat` | Replace `PartitionReaderFactory` | ⚠️ Maybe |
| **Column Injection** | Add to `dataSchema` | Add to... where? | ❌ |

### 5. Alternative: Modify Schema at Table Level

**Could Work**:
```scala
// In SparkTable.schema()
override def schema(): StructType = {
  if (hasDeletionVectors) {
    baseSchema.add(DV_COLUMN)  // Add DV column to schema
  } else {
    baseSchema
  }
}
```

**Problem**: This exposes internal column to user!
- User queries would see `__delta_internal_is_row_deleted` column
- Violates "transparent" design principle

### 6. The Real Issue

**Analyzer rules work by transforming LogicalPlan nodes**. For DSv2:
- Schema must be determined **before** analyzer runs (from `Table.schema()`)
- Can't inject columns dynamically in analyzer phase
- **This is a fundamental design difference between V1 and V2**

### 7. Conclusion

**Analyzer Rule approach is NOT viable for DSv2** because:

1. ❌ Cannot match `DataSourceV2Relation` same way as `LogicalRelation`
2. ❌ Cannot modify schema (comes from `Table.schema()`)
3. ❌ Cannot inject `FileFormat` (DSv2 doesn't use this concept)
4. ❌ Would expose internal columns to users

**The original design (handle in `SparkBatch`) is the correct approach**:
- ✅ Schema modification happens at reader factory level
- ✅ Internal column never exposed to users
- ✅ Follows DSv2 design patterns
- ✅ Self-contained within connector

## Verification Test

Let's verify this by checking Spark's DataSourceV2Relation structure:

```scala
// From Spark source (spark/sql/core/.../v2/DataSourceV2Relation.scala)
case class DataSourceV2Relation(
    table: Table,
    output: Seq[AttributeReference],
    catalog: Option[CatalogPlugin],
    identifier: Option[Identifier],
    options: CaseInsensitiveStringMap)
  extends LeafNode with NamedRelation {
  
  // Schema comes from table.schema(), immutable
  override lazy val schema: StructType = table.schema()
  
  // No FileFormat concept
  // No way to inject columns post-creation
}
```

**Key observation**: `schema` is derived from `table.schema()` and is immutable.
**Implication**: We MUST handle DV columns at the `Table` or `Batch` level, not in analyzer rules.

