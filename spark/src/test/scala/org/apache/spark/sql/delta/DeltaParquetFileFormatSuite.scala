/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.DataFrameUtils
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.sql.{DataFrame, Dataset, QueryTest}
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.test.SharedSparkSession

trait DeltaParquetFileFormatSuiteBase
    extends QueryTest
    with SharedSparkSession
    with DeletionVectorsTestUtils
    with DeltaSQLCommandTest {
  import testImplicits._

  /** Helper method to run the test with vectorized and non-vectorized Parquet readers */
  protected def testWithBothParquetReaders(name: String)(f: => Any): Unit = {
    for {
      enableVectorizedParquetReader <- BOOLEAN_DOMAIN
      readColumnarBatchAsRows <- BOOLEAN_DOMAIN
      // don't run for the combination (vectorizedReader=false, readColumnarBathAsRows = false)
      // as the non-vectorized reader always generates and returns rows, unlike the vectorized
      // reader which internally generates columnar batches but can returns either columnar batches
      // or rows from the columnar batch depending upon the config.
      if enableVectorizedParquetReader || readColumnarBatchAsRows
    } {
      test(s"$name, with vectorized Parquet reader=$enableVectorizedParquetReader, " +
        s"with readColumnarBatchAsRows=$readColumnarBatchAsRows") {
        // Set the max code gen fields to 0 to force the vectorized Parquet reader generate rows
        // from columnar batches.
        val codeGenMaxFields = if (readColumnarBatchAsRows) "0" else "100"
        withSQLConf(
          "spark.sql.parquet.enableVectorizedReader" -> enableVectorizedParquetReader.toString,
          "spark.sql.codegen.maxFields" -> codeGenMaxFields) {
          f
        }
      }
    }
  }

  /** Helper method to generate a table with single Parquet file with multiple rowgroups */
  protected def generateData(tablePath: String): Unit = {
    // This is to generate a Parquet file with two row groups
    hadoopConf().set("parquet.block.size", (1024 * 50).toString)

    // Keep the number of partitions to 1 to generate a single Parquet data file
    val df = Seq.range(0, 20000).toDF().repartition(1)
    df.write.format("delta").mode("append").save(tablePath)

    // Set DFS block size to be less than Parquet rowgroup size, to allow
    // the file split logic to kick-in, but gets turned off due to the
    // disabling of file splitting in DeltaParquetFileFormat when DVs are present.
    hadoopConf().set("dfs.block.size", (1024 * 20).toString)
  }

  protected def assertParquetHasMultipleRowGroups(filePath: Path): Unit = {
    val parquetMetadata = ParquetFileReader.readFooter(
      hadoopConf,
      filePath,
      ParquetMetadataConverter.NO_FILTER)
    assert(parquetMetadata.getBlocks.size() > 1)
  }

  protected def hadoopConf(): Configuration = {
    // scalastyle:off hadoopconfiguration
    // This is to generate a Parquet file with two row groups
    spark.sparkContext.hadoopConfiguration
    // scalastyle:on hadoopconfiguration
  }

  lazy val dvStore: DeletionVectorStore = DeletionVectorStore.createInstance(hadoopConf)
}

/**
 * Result from creating the file format and associated test objects.
 *
 * @param fileIndex The file index for the Delta table
 * @param deltaParquetFormat The DeltaParquetFileFormatBase instance (V1 or V2)
 * @param readingSchema The schema to use for reading
 */
case class FileFormatTestContext(
    fileIndex: FileIndex,
    deltaParquetFormat: DeltaParquetFileFormatBase,
    readingSchema: org.apache.spark.sql.types.StructType)

/**
 * Abstract base suite for DeltaParquetFileFormat tests.
 * Subclasses implement createFileFormatTestContext to provide V1 or V2 implementations.
 */
abstract class DeltaParquetFileFormatBasicSuiteBase extends DeltaParquetFileFormatSuiteBase {
  import testImplicits._

  /** Whether this suite supports deletion vectors tests */
  protected def supportsDeletionVectors: Boolean = true

  /**
   * Create the test context including file index, file format, and reading schema.
   *
   * @param tablePath Path to the test table
   * @param readIsRowDeletedCol Whether to include IS_ROW_DELETED column
   * @param readRowIndexCol Whether to include ROW_INDEX column
   * @param enableDVs Whether deletion vectors are enabled
   */
  def createFileFormatTestContext(
      tablePath: String,
      readIsRowDeletedCol: Boolean,
      readRowIndexCol: Boolean,
      enableDVs: Boolean): FileFormatTestContext

  /**
   * Prepare the table for DV tests by removing specific rows.
   * Override in subclasses to customize DV creation behavior.
   */
  protected def prepareTableForDVTest(tablePath: String): Unit = {
    val deltaLog = DeltaLog.forTable(spark, tablePath)
    val addFile = deltaLog.snapshot.allFiles.collect()(0)
    removeRowsFromFile(deltaLog, addFile, Seq(0, 200, 300, 756, 10352, 19999))
  }

  /**
   * Get the file path to verify for multiple row groups.
   */
  protected def getFilePathForRowGroupVerification(tablePath: String): Path = {
    val deltaLog = DeltaLog.forTable(spark, tablePath)
    val addFile = deltaLog.snapshot.allFiles.collect()(0)
    addFile.absolutePath(deltaLog)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key, "false")
  }

  // Read with deletion vectors has separate code paths based on vectorized Parquet
  // reader is enabled or not. Test both the combinations
  for {
    readIsRowDeletedCol <- BOOLEAN_DOMAIN
    readRowIndexCol <- BOOLEAN_DOMAIN
    enableDVs <- BOOLEAN_DOMAIN
    if (enableDVs && readIsRowDeletedCol) || !enableDVs
  } {
    // Skip DV tests for suites that don't support them
    val shouldSkip = enableDVs && !supportsDeletionVectors

    testWithBothParquetReaders(
      s"isDeletionVectorsEnabled=$enableDVs, read DV metadata columns: " +
        s"with isRowDeletedCol=$readIsRowDeletedCol, " +
        s"with rowIndexCol=$readRowIndexCol") {
      if (shouldSkip) {
        cancel("Deletion vectors not supported in this implementation")
      }

      withSQLConf(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey ->
          enableDVs.toString) {
        withTempDir { tempDir =>
          val tablePath = tempDir.toString

          // Generate a table with one parquet file containing multiple row groups.
          generateData(tablePath)

          if (enableDVs) {
            prepareTableForDVTest(tablePath)
          }

          val addFilePath = getFilePathForRowGroupVerification(tablePath)
          assertParquetHasMultipleRowGroups(addFilePath)

          // Create file format using implementation-specific method
          val FileFormatTestContext(fileIndex, deltaParquetFormat, readingSchema) =
            createFileFormatTestContext(tablePath, readIsRowDeletedCol, readRowIndexCol, enableDVs)

          val relation = HadoopFsRelation(
            fileIndex,
            fileIndex.partitionSchema,
            readingSchema,
            bucketSpec = None,
            deltaParquetFormat,
            options = Map.empty)(spark)
          val plan = LogicalRelation(relation)

          if (readIsRowDeletedCol) {
            val (deletedColumnValue, notDeletedColumnValue) = (1, 0)
            if (enableDVs) {
              // Select some rows that are deleted and some rows not deleted
              // Deleted row `value`: 0, 200, 300, 756, 10352, 19999
              // Not deleted row `value`: 7, 900
              checkDatasetUnorderly(
                DataFrameUtils.ofRows(spark, plan)
                  .filter("value in (0, 7, 200, 300, 756, 900, 10352, 19999)")
                  .select("value", DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME)
                  .as[(Int, Int)],
                (0, deletedColumnValue),
                (7, notDeletedColumnValue),
                (200, deletedColumnValue),
                (300, deletedColumnValue),
                (756, deletedColumnValue),
                (900, notDeletedColumnValue),
                (10352, deletedColumnValue),
                (19999, deletedColumnValue))
            } else {
              checkDatasetUnorderly(
                DataFrameUtils.ofRows(spark, plan)
                  .filter("value in (0, 7, 200, 300, 756, 900, 10352, 19999)")
                  .select("value", DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME)
                  .as[(Int, Int)],
                (0, notDeletedColumnValue),
                (7, notDeletedColumnValue),
                (200, notDeletedColumnValue),
                (300, notDeletedColumnValue),
                (756, notDeletedColumnValue),
                (900, notDeletedColumnValue),
                (10352, notDeletedColumnValue),
                (19999, notDeletedColumnValue))
            }
          }

          if (readRowIndexCol) {
            def rowIndexes(df: DataFrame): Set[Long] = {
              val colIndex = if (readIsRowDeletedCol) 2 else 1
              df.collect().map(_.getLong(colIndex)).toSet
            }

            val df = DataFrameUtils.ofRows(spark, plan)
            assert(rowIndexes(df) === Seq.range(0, 20000).toSet)

            assert(
              rowIndexes(
                df.filter("value in (0, 7, 200, 300, 756, 900, 10352, 19999)")) ===
                Seq(0, 7, 200, 300, 756, 900, 10352, 19999).toSet)
          }
        }
      }
    }
  }
}

/**
 * V1 implementation of DeltaParquetFileFormat tests using DeltaParquetFileFormat.
 */
class DeltaParquetFileFormatSuite extends DeltaParquetFileFormatBasicSuiteBase {

  override def createFileFormatTestContext(
      tablePath: String,
      readIsRowDeletedCol: Boolean,
      readRowIndexCol: Boolean,
      enableDVs: Boolean): FileFormatTestContext = {
    val deltaLog = DeltaLog.forTable(spark, tablePath)
    val metadata = deltaLog.snapshot.metadata

    // Add additional field that has the deleted row flag to existing data schema
    var readingSchema = metadata.schema
    if (readIsRowDeletedCol) {
      readingSchema = readingSchema.add(DeltaParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD)
    }
    if (readRowIndexCol) {
      readingSchema = readingSchema.add(DeltaParquetFileFormat.ROW_INDEX_STRUCT_FIELD)
    }

    val deltaParquetFormat = new DeltaParquetFileFormat(
      deltaLog.snapshot.protocol,
      metadata,
      nullableRowTrackingConstantFields = false,
      nullableRowTrackingGeneratedFields = false,
      optimizationsEnabled = false,
      if (enableDVs) Some(tablePath) else None)

    val fileIndex = TahoeLogFileIndex(spark, deltaLog, None)

    FileFormatTestContext(fileIndex, deltaParquetFormat, readingSchema)
  }
}

class DeltaParquetFileFormatWithPredicatePushdownSuite extends DeltaParquetFileFormatSuiteBase {
  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    enableDeletionVectorsForAllSupportedOperations(spark)
    spark.conf.set(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key, "true")
  }

  for {
    rowIndexFilterType <- Seq(RowIndexFilterType.IF_CONTAINED, RowIndexFilterType.IF_NOT_CONTAINED)
  } testWithBothParquetReaders("read DV metadata columns: " +
      s"with rowIndexFilterType=$rowIndexFilterType") {
    withTempDir { tempDir =>
      val tablePath = tempDir.toString

      // Generate a table with one parquet file containing multiple row groups.
      generateData(tablePath)

      val deltaLog = DeltaLog.forTable(spark, tempDir)
      val metadata = deltaLog.update().metadata

      // Add additional field that has the deleted row flag to existing data schema
      val readingSchema = metadata.schema.add(DeltaParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD)

      // Fetch the only file in the DeltaLog snapshot
      val addFile = deltaLog.update().allFiles.collect()(0)
      removeRowsFromFile(deltaLog, addFile, Seq(0, 200, 300, 756, 10352, 19999))

      val addFilePath = addFile.absolutePath(deltaLog)
      assertParquetHasMultipleRowGroups(addFilePath)

      val deltaParquetFormat = new DeltaParquetFileFormat(
        deltaLog.update().protocol,
        metadata,
        nullableRowTrackingConstantFields = false,
        nullableRowTrackingGeneratedFields = false,
        optimizationsEnabled = true,
        Some(tablePath))

      val fileIndex = TahoeLogFileIndex(spark, deltaLog)

      val relation = HadoopFsRelation(
        fileIndex,
        fileIndex.partitionSchema,
        readingSchema,
        bucketSpec = None,
        deltaParquetFormat,
        options = Map.empty)(spark)

      val plan = LogicalRelation(relation)
      val planWithMetadataCol =
        plan.copy(output = plan.output :+ deltaParquetFormat.createFileMetadataCol())
      val (deletedColumnValue, notDeletedColumnValue) = (1, 0)

      // Select some rows that are deleted and some rows not deleted
      // Deleted row `value`: 0, 200, 300, 756, 10352, 19999
      // Not deleted row `value`: 7, 900
      checkDatasetUnorderly(
        DataFrameUtils.ofRows(spark, planWithMetadataCol)
          .filter("value in (0, 7, 200, 300, 756, 900, 10352, 19999)")
          .select("value", DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME)
          .as[(Int, Int)],
        (0, deletedColumnValue),
        (7, notDeletedColumnValue),
        (200, deletedColumnValue),
        (300, deletedColumnValue),
        (756, deletedColumnValue),
        (900, notDeletedColumnValue),
        (10352, deletedColumnValue),
        (19999, deletedColumnValue))
    }
  }
}
