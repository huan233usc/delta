# Deletion Vector Support - Implementation Summary (Option 2)

## Approach: Table.schema() Level

实现了 **Option 2**: 在 `SparkTable.schema()` 中添加 DV 列。

## 实现的组件

### 1. SparkTable.java - Schema 级别添加 DV 列

**文件**: `kernel-spark/src/main/java/io/delta/kernel/spark/catalog/SparkTable.java`

**修改**:
```java
// 在构造函数中检查 protocol 并添加 DV 列到 schema
StructType baseSchema = SchemaUtils.convertKernelSchemaToSparkSchema(initialSnapshot.getSchema());
this.schema = addDVColumnIfNeeded(baseSchema, initialSnapshot);

// 新增方法
private StructType addDVColumnIfNeeded(StructType baseSchema, Snapshot snapshot)
private boolean deletionVectorsReadable(Snapshot snapshot)

// 在构建 dataSchema 时排除 DV 列
for (StructField field : schema.fields()) {
  if (!partColNames.contains(field.name()) && 
      !"__delta_internal_is_row_deleted".equals(field.name())) {
    dataFields.add(field);
  }
}
```

**作用**:
- 如果表的 protocol 支持 DV feature，自动添加 `__delta_internal_is_row_deleted` 列到 schema
- 但不添加到 `dataSchema`（只在整体 schema 中）

### 2. SparkBatch.java - 过滤 DV 行并移除列

**文件**: `kernel-spark/src/main/java/io/delta/kernel/spark/read/SparkBatch.java`

**修改**:
```java
// 在 createReaderFactory() 中
boolean hasDVColumn = hasDVColumn(readDataSchema);

if (hasDVColumn) {
  readFunc = wrapReaderForDV(readFunc, readDataSchema);
}

// 新增类和方法
private boolean hasDVColumn(StructType schema)
private Function1<...> wrapReaderForDV(...)
private static class DVFilteringIterator extends AbstractIterator<InternalRow>
```

**作用**:
- 检测 schema 中是否有 DV 列
- 如果有，包装 reader 来：
  1. 过滤掉 `__delta_internal_is_row_deleted != 0` 的行
  2. 从输出中移除 DV 列

### 3. SparkScan.java - 保留 DV metadata 传递

**文件**: `kernel-spark/src/main/java/io/delta/kernel/spark/read/SparkScan.java`

**保持不变** (之前已经实现):
- `buildMetadataMap()` 已经正确提取和传递 DV descriptor
- 通过 `otherConstantMetadataColumnValues` 传递给 `DeltaParquetFileFormatBase`

## 工作流程

```
1. SparkTable.schema()
   ↓
   检查 protocol.readerFeatures().contains("deletionVectors")
   ↓
   是 → 添加 __delta_internal_is_row_deleted 到 schema
   否 → 使用原始 schema

2. SparkScanBuilder/SparkScan
   ↓
   使用 schema (可能包含 DV 列)
   ↓
   SparkBatch.createReaderFactory()

3. DeltaParquetFileFormatV2.buildReaderWithPartitionValues()
   ↓
   从 PartitionedFile.otherConstantMetadataColumnValues 读取 DV metadata
   ↓
   DeltaParquetFileFormatBase 填充 __delta_internal_is_row_deleted 列
   ↓
   返回包含 DV 列的 rows

4. DVFilteringIterator (SparkBatch中)
   ↓
   过滤 isDeleted == 0 的行
   ↓
   投影掉 DV 列
   ↓
   返回用户数据（不含 DV 列）
```

## 关键特性

### ✅ 优点

1. **简单直接**: 在 table level 处理，逻辑清晰
2. **复用现有代码**: 100% 复用 `DeltaParquetFileFormatBase` 的 DV 过滤逻辑
3. **自包含**: 不需要 analyzer rules
4. **DV 列对用户透明**: 在最终输出前移除

### ⚠️ 潜在问题

1. **DV 列在 schema() 中可见**:
   - `Table.schema()` 返回包含 DV 列的 schema
   - 用户调用 `spark.table("dsv2.delta.path").schema` 会看到这个列
   - 但查询结果中不会出现（被 DVFilteringIterator 移除）

2. **与 Spark 的约定不一致**:
   - Spark 期望 `Table.schema()` 返回的是用户可见的列
   - 我们返回了内部列

## 测试方法

### 手动测试

```scala
val spark = SparkSession.builder()
  .config("spark.sql.catalog.dsv2", "io.delta.kernel.spark.catalog.DeltaCatalog")
  .getOrCreate()

val tablePath = "path/to/dv-partitioned-with-checkpoint"

// 测试1: 检查 schema
val table = spark.table(s"dsv2.delta.$tablePath")
table.printSchema()  // 应该包含 __delta_internal_is_row_deleted

// 测试2: 检查查询结果
val df = spark.sql(s"SELECT * FROM `dsv2`.`delta`.`$tablePath`")
df.columns  // 应该 NOT 包含 __delta_internal_is_row_deleted
df.count()  // 应该 = 35 (与 DSv1 一致)

// 测试3: 对比 DSv1 和 DSv2
val df1 = spark.sql(s"SELECT * FROM `spark_catalog`.`delta`.`$tablePath`")
val df2 = spark.sql(s"SELECT * FROM `dsv2`.`delta`.`$tablePath`")
assert(df1.count() == df2.count())
```

### Golden Table 测试

```bash
cd /Users/xin.huang/oss/delta
./build/sbt "sparkV2/testOnly io.delta.kernel.spark.read.SparkGoldenTableTest"
```

应该通过:
- `dv-partitioned-with-checkpoint`
- `dv-with-columnmapping`

## 已知限制

1. **Schema 暴露问题**: DV 列在 `Table.schema()` 中可见
2. **SELECT * 可能有问题**: 如果 Spark 基于 `Table.schema()` 生成列列表
3. **列数不匹配**: `Table.schema().fields().length` vs 实际返回的列数

## 下一步

如果 Option 2 的"schema 暴露问题"不可接受，需要：

### Option 3: Batch Level (推荐的最终方案)

在 `SparkBatch.createReaderFactory()` 中：
1. 检查 protocol 是否支持 DV
2. 动态添加 DV 列到 `dataSchema` 和 `readDataSchema`
3. 传递给 `DeltaParquetFileFormatV2`
4. 包装 reader 过滤并移除 DV 列

优点:
- DV 列完全不出现在 `Table.schema()` 中
- 只在 reader factory 创建时临时添加
- 对用户完全透明

## 测试状态

- ✅ 代码实现完成
- ✅ 编译通过
- ⬜ 手动测试待执行
- ⬜ Golden table 测试待执行


