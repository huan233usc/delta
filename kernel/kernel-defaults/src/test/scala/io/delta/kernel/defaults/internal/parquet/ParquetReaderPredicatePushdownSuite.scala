/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.internal.parquet

import java.math.BigDecimal
import java.nio.file.Files
import java.sql.Date
import java.util.Optional

import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel.defaults.utils.{ExpressionTestUtils, TestRow}
import io.delta.kernel.expressions._
import io.delta.kernel.expressions.Literal.{ofBinary, ofBoolean, ofDate, ofDouble, ofFloat, ofInt, ofLong, ofNull, ofString}
import io.delta.kernel.internal.util.InternalUtils.daysSinceEpoch
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.types.{DecimalType, IntegerType, StructType}

import org.apache.spark.sql.{types => sparktypes, Row}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ParquetReaderPredicatePushdownSuite extends AnyFunSuite
    with BeforeAndAfterAll with ParquetSuiteBase with VectorTestUtils with ExpressionTestUtils {

  //////////////////////////////////////////////////////////////////////////////////
  // Test data generation and helper methods
  //////////////////////////////////////////////////////////////////////////////////

  var testParquetTable: String = ""

  override def beforeAll(): Unit = {
    super.beforeAll()

    testParquetTable = Files.createTempDirectory("tempDir").toString

    // Generate a test Parquet file with 20 row groups. Each row group has 100 rows.
    // Parquet-mr checks whether the current row group has reached the limit or for every 100 rows.
    // We set the `parquet.block.size` to very low, so for every 100 rows, it will create a
    // new row group.
    val rows = Seq.range(0, 20).flatMap(i => generateRowsGroup(i))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), testTableSchema)
    withSQLConf("parquet.block.size" -> 1.toString) {
      df.repartition(1)
        .orderBy("rowId")
        .write
        .format("delta")
        .mode("append")
        .save(testParquetTable)
    }

    // Create a separate test table with decimal columns for testing decimal filter pushdown
    // Build 5 row groups (100 rows each) so we can validate row-group pruning semantics.
    testDecimalTable = Files.createTempDirectory("decimalTestDir").toString

    val decimalTestSchema = sparktypes.StructType(Seq(
      sparktypes.StructField("id", sparktypes.IntegerType),
      sparktypes.StructField("decimal_int32", sparktypes.DecimalType(9, 2)), // stored as INT32
      sparktypes.StructField("decimal_int64", sparktypes.DecimalType(18, 4)), // stored as INT64
      sparktypes.StructField(
        "decimal_binary",
        sparktypes.DecimalType(25, 5)
      ) // stored as BINARY/FIXED_LEN_BYTE_ARRAY
    ))

    def decimalGroupValueInt32(groupIdx: Int): java.math.BigDecimal = groupIdx match {
      case 0 => null
      case 1 => BigDecimal.valueOf(234.56)
      case 2 => BigDecimal.valueOf(345.67)
      case 3 => BigDecimal.valueOf(456.78)
      case 4 => BigDecimal.valueOf(567.89)
    }
    def decimalGroupValueInt64(groupIdx: Int): java.math.BigDecimal = groupIdx match {
      case 0 => BigDecimal.valueOf(12345.6789)
      case 1 => BigDecimal.valueOf(23456.7890)
      case 2 => BigDecimal.valueOf(34567.8901)
      case 3 => BigDecimal.valueOf(45678.9012)
      case 4 => BigDecimal.valueOf(56789.0123)
    }
    def decimalGroupValueBinary(groupIdx: Int): java.math.BigDecimal = groupIdx match {
      case 0 => BigDecimal.valueOf(123456789.12345)
      case 1 => BigDecimal.valueOf(234567890.23456)
      case 2 => BigDecimal.valueOf(345678901.34567)
      case 3 => BigDecimal.valueOf(456789012.45678)
      case 4 => BigDecimal.valueOf(567890123.56789)
    }

    val decimalRows = Seq.range(0, 5).flatMap { groupIdx =>
      Seq.range(groupIdx * 100, (groupIdx + 1) * 100).map { _ =>
        Row(
          groupIdx + 1,
          decimalGroupValueInt32(groupIdx),
          decimalGroupValueInt64(groupIdx),
          decimalGroupValueBinary(groupIdx))
      }
    }

    val decimalDf = spark.createDataFrame(
      spark.sparkContext.parallelize(decimalRows),
      decimalTestSchema)
    withSQLConf("parquet.block.size" -> 1.toString) {
      decimalDf.repartition(1).write.format("delta").mode("overwrite").save(testDecimalTable)
    }
  }

  // test table schema
  val testTableSchema: sparktypes.StructType = {
    // These are the only supported column types in Parquet filter push down
    def allTypesSchema(): Array[sparktypes.StructField] = {
      Seq(
        sparktypes.StructField("byteCol", sparktypes.ByteType),
        sparktypes.StructField("shortCol", sparktypes.ShortType),
        sparktypes.StructField("intCol", sparktypes.IntegerType),
        sparktypes.StructField("longCol", sparktypes.LongType),
        sparktypes.StructField("floatCol", sparktypes.FloatType),
        sparktypes.StructField("doubleCol", sparktypes.DoubleType),
        sparktypes.StructField("stringCol", sparktypes.StringType),
        // column with values that are truncated in stats
        sparktypes.StructField("truncatedStringCol", sparktypes.StringType),
        sparktypes.StructField("binaryCol", sparktypes.BinaryType),
        sparktypes.StructField("truncatedBinaryCol", sparktypes.BinaryType),
        sparktypes.StructField("booleanCol", sparktypes.BooleanType),
        sparktypes.StructField("dateCol", sparktypes.DateType)).toArray
    }

    // supported data type columns as top level columns
    new sparktypes.StructType(allTypesSchema())
      // supported data type columns as nested columns
      .add("nested", sparktypes.StructType(allTypesSchema()))
      // row id to help with the test results verification
      .add("rowId", sparktypes.IntegerType)
  }

  private def generateRowsGroup(rowGroupIdx: Int): Seq[Row] = {
    def values(rowId: Int): Seq[Any] = {
      // One of the columns in each row group is all nulls or all non-nulls depending on
      // the [[rowGroupIdx]]. This helps to verify the test results for `is null` and
      // `is not null` pushdown
      Seq(
        // byteCol
        if (rowGroupIdx == 0) null /* all nulls */
        else if (rowGroupIdx == 11) rowId.byteValue() /* all non-nulls */
        else (if (rowId % 72 != 0) rowId.byteValue() else null), /* mix of nulls and non-nulls */

        // shortCol
        if (rowGroupIdx == 1) null
        else if (rowGroupIdx == 10) rowId.shortValue()
        else (if (rowId % 56 != 0) rowId.shortValue() else null),

        // intCol
        if (rowGroupIdx == 2) null
        else if (rowGroupIdx == 9) rowId
        else (if (rowId % 23 != 0) rowId else null),

        // longCol
        if (rowGroupIdx == 3) null
        else if (rowGroupIdx == 8) (rowId + 1).longValue()
        else (if (rowId % 25 != 0) (rowId + 1).longValue() else null),

        // floatCol
        if (rowGroupIdx == 4) null
        else if (rowGroupIdx == 7) (rowId + 0.125).floatValue()
        else (if (rowId % 28 != 0) (rowId + 0.125).floatValue() else null),

        // doubleCol
        if (rowGroupIdx == 5) null
        else if (rowGroupIdx == 6) (rowId + 0.000001).doubleValue()
        else (if (rowId % 54 != 0) (rowId + 0.000001).doubleValue() else null),

        // stringCol
        if (rowGroupIdx == 6) null
        else if (rowGroupIdx == 5) "%05d".format(rowId)
        else (if (rowId % 57 != 0) "%05d".format(rowId) else null),

        // truncatedStringCol - stats will be truncated as the value is too long
        if (rowGroupIdx == 7) null
        else if (rowGroupIdx == 4) "%050d".format(rowId)
        else (if (rowId % 57 != 0) "%050d".format(rowId) else null),

        // binaryCol
        if (rowGroupIdx == 8) null
        else if (rowGroupIdx == 3) "%06d".format(rowId).getBytes
        else (if (rowId % 59 != 0) "%06d".format(rowId).getBytes else null),

        // truncatedBinaryCol - stats will be truncated as the value is too long
        if (rowGroupIdx == 9) null
        else if (rowGroupIdx == 2) "%060d".format(rowId).getBytes
        else (if (rowId % 59 != 0) "%060d".format(rowId).getBytes else null),

        // booleanCol
        if (rowGroupIdx == 10) null
        else if (rowGroupIdx == 1) rowId % 2 == 0
        // alternative between true and false for each row group
        else (if (rowId % 29 != 0) rowGroupIdx % 2 == 0 else null),

        // dateCol
        if (rowGroupIdx == 11) null
        else if (rowGroupIdx == 0) new Date(rowId * 86400000L)
        else (if (rowId % 61 != 0) new Date(rowId * 86400000L) else null))
    }

    Seq.range(rowGroupIdx * 100, (rowGroupIdx + 1) * 100).map { rowId =>
      Row.fromSeq(
        values(rowId) ++ // top-level column values
          Seq(
            Row.fromSeq(values(rowId)), // nested column values
            rowId // row id to help with the test results verification
          ))
    }
  }

  def generateExpData(rowGroupIndexes: Seq[Int]): Seq[TestRow] = {
    spark.createDataFrame(
      spark.sparkContext.parallelize(rowGroupIndexes.flatMap(i => generateRowsGroup(i))),
      testTableSchema)
      .collect
      .map(TestRow(_))
  }

  private def readUsingKernel(tablePath: String, predicate: Predicate): Seq[TestRow] = {
    val readSchema: StructType = tableSchema(testParquetTable)
    readParquetFilesUsingKernel(tablePath, readSchema, Optional.of(predicate))
  }

  private def assertConvertedFilterIsEmpty(predicate: Predicate, tablePath: String): Unit = {
    val parquetFileSchema =
      parquetFiles(tablePath).map(_.getPath).map(footer(_)).head.getFileMetaData.getSchema

    assert(
      !ParquetFilterUtils.toParquetFilter(parquetFileSchema, predicate).isPresent,
      "Predicate should not be converted to Parquet filter")
  }

  //////////////////////////////////////////////////////////////////////////////////
  // End-2-end tests
  //////////////////////////////////////////////////////////////////////////////////

  Seq(
    // filter on int type column
    (
      eq(col("intCol"), ofInt(20)), // top-level column
      eq(col("nested", "intCol"), ofInt(20)), // nested column
      Seq(0) // expected row groups
    ),
    // filter on long type column
    (
      gt(col("longCol"), ofLong(1600)),
      gt(col("nested", "longCol"), ofLong(1600)),
      Seq(16, 17, 18, 19) // expected row groups
    ),
    // filter on float type column
    (
      lt(col("floatCol"), ofFloat(1000.0f)),
      lt(col("nested", "floatCol"), ofFloat(1000.0f)),
      Seq(0, 1, 2, 3, 5, 6, 7, 8, 9) // expected row groups - row group 4 has all nulls
    ),
    // filter on double type column
    (
      gt(col("doubleCol"), ofDouble(1000.0)),
      gt(col("nested", "doubleCol"), ofDouble(1000.0)),
      Seq(10, 11, 12, 13, 14, 15, 16, 17, 18, 19) // expected row groups
    ),
    // filter on boolean type column
    (
      eq(col("booleanCol"), ofBoolean(true)),
      eq(col("nested", "booleanCol"), ofBoolean(true)),
      // expected row groups
      // 1 has mix of true/false (included), 10 has all nulls (not included)
      Seq(0, 1, 2, 4, 6, 8, 12, 14, 16, 18)),
    // filter on date type column
    (
      lte(
        col("dateCol"),
        ofDate(
          daysSinceEpoch(new Date(500 * 86400000L /* millis in a day */ )))),
      lte(
        col("nested", "dateCol"),
        ofDate(
          daysSinceEpoch(new Date(500 * 86400000L /* millis in a day */ )))),
      Seq(0, 1, 2, 3, 4, 5) // expected row groups
    ),
    // filter on string type column
    (
      eq(col("stringCol"), ofString("%05d".format(300))),
      eq(col("nested", "stringCol"), ofString("%05d".format(300))),
      Seq(3) // expected row groups
    ),
    // filter on binary type column
    (
      gte(col("binaryCol"), ofBinary("%06d".format(1700).getBytes)),
      gte(col("nested", "binaryCol"), ofBinary("%06d".format(1700).getBytes)),
      Seq(17, 18, 19) // expected row groups
    ),
    // filter on truncated stats string type column
    (
      gte(col("truncatedStringCol"), ofString("%050d".format(300))),
      gte(col("nested", "truncatedStringCol"), ofString("%050d".format(300))),
      // expected row groups
      Seq(3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19) // 7 has all nulls
    ),
    // filter on truncated stats binary type column
    (
      lte(col("truncatedBinaryCol"), ofBinary("%060d".format(600).getBytes)),
      lte(col("nested", "truncatedBinaryCol"), ofBinary("%060d".format(600).getBytes)),
      Seq(0, 1, 2, 3, 4, 5, 6) // expected row groups
    )).foreach {
    // boolean, int32, data, int64, float, double, binary, string
    // Test table has 20 row groups, each with 100 rows.
    case (predicateTopLevelCol, predicateNestedCol, expRowGroups) =>
      Seq(predicateTopLevelCol, predicateNestedCol).foreach { predicate =>
        test(s"filter pushdown: $predicate") {
          val actualData = readUsingKernel(testParquetTable, predicate)
          val expOutputRowCount = expRowGroups.length * 100 // 100 rows per row group
          assert(actualData.size === expOutputRowCount, s"predicate: $predicate")
          checkAnswer(actualData, generateExpData(expRowGroups))
        }
      }
  }

  // IS NULL and IS NOT NULL tests
  Seq(
    // (columnName, row groups with all nulls, row groups with all non-nulls)
    ("byteCol", Seq(0), Seq(11)), // int type column
    ("shortCol", Seq(1), Seq(10)), // short type column
    ("intCol", Seq(2), Seq(9)), // int type column
    ("longCol", Seq(3), Seq(8)), // long type column
    ("floatCol", Seq(4), Seq(7)), // float type column
    ("doubleCol", Seq(5), Seq(6)), // double type column
    ("stringCol", Seq(6), Seq(5)), // string type column
    ("truncatedStringCol", Seq(7), Seq(4)), // truncatedStringCol type column
    ("binaryCol", Seq(8), Seq(3)), // binary type column
    ("truncatedBinaryCol", Seq(9), Seq(2)), // truncatedBinaryCol type column
    ("booleanCol", Seq(10), Seq(1)), // boolean type column
    ("dateCol", Seq(11), Seq(0)) // date type column
  ).foreach {
    // Test table has 20 row groups, each with 100 rows.
    case (colName, allNullsRowGroups, allNonNullsRowGroups) =>
      // Test predicate on both top-level and nested columns
      Seq(col(colName), col("nested", colName)).foreach { column =>
        val isNullFilter = isNull(column)
        test(s"filter pushdown: $isNullFilter") {
          val actualData = readUsingKernel(testParquetTable, isNullFilter)
          val expOutputRowCount = 100 * (20 - 1) // 100 rows per row group

          // we get everything expect the rowgroup that has all non-nulls
          val expRowGroups = (0 until 20).filter(!allNonNullsRowGroups.contains(_))
          assert(actualData.size === expOutputRowCount, s"predicate: $isNullFilter")
          checkAnswer(actualData, generateExpData(expRowGroups))

          // not (col is null) should return all row groups exception the one with all nulls
          assertNot(isNullFilter, (0 until 20).filter(!allNullsRowGroups.contains(_)))
        }

        val isNotNullFilter = isNotNull(column)
        test(s"filter pushdown: $isNotNullFilter") {
          val actualData = readUsingKernel(testParquetTable, isNotNullFilter)
          val expOutputRowCount = 100 * (20 - 1) // 100 rows per row group

          // we get everything expect the rowgroup that has all nulls
          val expRowGroups = (0 until 20).filter(!allNullsRowGroups.contains(_))
          assert(actualData.size === expOutputRowCount, s"predicate: $isNotNullFilter")
          checkAnswer(actualData, generateExpData(expRowGroups))

          // not (col is not null) should return all row groups exception the one with all non-nulls
          assertNot(isNotNullFilter, (0 until 20).filter(!allNonNullsRowGroups.contains(_)))
        }
      }
  }

  test("for a column that doesn't exist in the table") {
    val testPredicate = predicate("=", col("nonExistentCol"), ofInt(20))
    assertConvertedFilterIsEmpty(testPredicate, testParquetTable)

    val actData = readUsingKernel(testParquetTable, testPredicate)
    // contains all the data in the table as the predicate is not pushed down
    checkAnswer(actData, generateExpData(Seq.range(0, 20)))
  }

  test("literal and column are swapped") {
    val testPredicate = predicate("=", ofInt(20), col("intCol"))
    val actData = readUsingKernel(testParquetTable, testPredicate)
    checkAnswer(actData, generateExpData(Seq(0)))
  }

  test("comparator literal value is null") {
    val testPredicate = predicate("=", col("intCol"), ofNull(IntegerType.INTEGER))
    assertConvertedFilterIsEmpty(testPredicate, testParquetTable)

    val actData = readUsingKernel(testParquetTable, testPredicate)
    // contains all the data in the table as the predicate is not pushed down
    checkAnswer(actData, generateExpData(Seq.range(0, 20)))
  }

  test("comparator that compare column and column") {
    val testPredicate = predicate("=", col("intCol"), col("longCol"))
    assertConvertedFilterIsEmpty(testPredicate, testParquetTable)

    val actData = readUsingKernel(testParquetTable, testPredicate)
    // contains all the data in the table as the predicate is not pushed down
    checkAnswer(actData, generateExpData(Seq.range(0, 20)))
  }

  test("comparator that compare literal and literal") {
    val testPredicate = predicate("=", ofInt(20), ofInt(20))
    assertConvertedFilterIsEmpty(testPredicate, testParquetTable)

    val actData = readUsingKernel(testParquetTable, testPredicate)
    // contains all the data in the table as the predicate is not pushed down
    checkAnswer(actData, generateExpData(Seq.range(0, 20)))
  }

  test("OR support") {
    val predicate = or(
      eq(col("intCol"), ofInt(20)),
      eq(col("longCol"), ofLong(1600)))
    val actData = readUsingKernel(testParquetTable, predicate)
    checkAnswer(actData, generateExpData(Seq(0, 15)))
  }

  test("one end of the OR is not convertible") {
    val predicate = or(
      eq(col("intCol"), ofInt(1599)),
      eq(col("nonExistentCol"), ofInt(1600)))
    assertConvertedFilterIsEmpty(predicate, testParquetTable)

    val actData = readUsingKernel(testParquetTable, predicate)
    // contains all the data in the table as the predicate is not pushed down
    checkAnswer(actData, generateExpData(Seq.range(0, 20)))
  }

  test("AND support") {
    val predicate = and(
      eq(col("intCol"), ofInt(1599)),
      eq(col("longCol"), ofLong(1600)))
    val actData = readUsingKernel(testParquetTable, predicate)
    checkAnswer(actData, generateExpData(Seq(15)))
  }

  test("one end of the AND is not convertible") {
    val predicate = and(
      eq(col("intCol"), ofInt(1599)),
      eq(col("nonExistentCol"), ofInt(1600)))
    val actData = readUsingKernel(testParquetTable, predicate)
    checkAnswer(actData, generateExpData(Seq(15)))
  }

  test("not support on gt") {
    val predicate = not(gt(col("intCol"), ofInt(950)))
    val actData = readUsingKernel(testParquetTable, predicate)

    // rowgroups until 9 could have values <= 950
    // rowgroup 2 has all nulls, so it won't be included in the result
    val expRowGroups = Seq(0, 1, 3, 4, 5, 6, 7, 8, 9)
    val expOutputRowCount = expRowGroups.length * 100 // 100 rows per row group
    assert(actData.size === expOutputRowCount, s"predicate: $predicate")

    checkAnswer(actData, generateExpData(expRowGroups))
  }

  test("not support on equality") {
    val predicate = not(eq(col("longCol"), ofLong(768)))
    val actData = readUsingKernel(testParquetTable, predicate)
    // rowgroup 3 has all nulls, so it will be included in the results as
    // Parquet equality filter is not null safe
    // every other group has value that is not 768
    checkAnswer(actData, generateExpData(Seq.range(0, 20)))
  }

  test("doesn't work on the repeated columns") {
    val testTable = goldenTablePath("parquet-all-types")
    val readSchema = tableSchema(testTable)

    val predicate = eq(col("array_of_prims"), ofInt(20))
    assertConvertedFilterIsEmpty(predicate, testTable)

    val actResult = readParquetFilesUsingKernel(testTable, readSchema, Optional.of(predicate))
    val expResult = readParquetFilesUsingSpark(testTable, readSchema)

    checkAnswer(actResult, expResult)
  }

  /** Test the `not(predicate)` returns expected rowgroups */
  private def assertNot(predicate: Predicate, expRowGroups: Seq[Int]): Unit = {
    val notPredicate = not(predicate)
    val actualData = readUsingKernel(testParquetTable, notPredicate)
    val expOutputRowCount = expRowGroups.length * 100 // 100 rows per row group
    assert(actualData.size === expOutputRowCount, s"predicate: $notPredicate")
    checkAnswer(actualData, generateExpData(expRowGroups))
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Decimal type filter pushdown tests
  //////////////////////////////////////////////////////////////////////////////////

  var testDecimalTable: String = ""

  private def readDecimalTable(predicate: Predicate): Seq[TestRow] = {
    val readSchema: StructType = tableSchema(testDecimalTable)
    readParquetFilesUsingKernel(testDecimalTable, readSchema, Optional.of(predicate))
  }

  test("decimal filter pushdown - INT32 stored decimal equality (row-group prune)") {
    val decimalPredicate =
      eq(col("decimal_int32"), Literal.ofDecimal(new BigDecimal("234.56"), 9, 2))
    val actualData = readDecimalTable(decimalPredicate)
    // Only group 1 has this value; 1 group * 100 rows
    assert(actualData.size === 100)
  }

  test("decimal filter pushdown - INT32 stored decimal greater than (row-group prune)") {
    val decimalPredicate =
      gt(col("decimal_int32"), Literal.ofDecimal(new BigDecimal("300.00"), 9, 2))
    val actualData = readDecimalTable(decimalPredicate)
    // Groups 2,3,4 qualify => 3 groups * 100 rows
    assert(actualData.size === 300)
  }

  test("decimal filter pushdown - INT64 stored decimal less than (row-group prune)") {
    val decimalPredicate =
      lt(col("decimal_int64"), Literal.ofDecimal(new BigDecimal("40000.0000"), 18, 4))
    val actualData = readDecimalTable(decimalPredicate)
    // Groups 0,1,2 qualify => 3 groups * 100 rows
    assert(actualData.size === 300)
  }

  test("decimal filter pushdown - BINARY stored decimal greater than or equal (row-group prune)") {
    val decimalPredicate =
      gte(col("decimal_binary"), Literal.ofDecimal(new BigDecimal("300000000.00000"), 25, 5))
    val actualData = readDecimalTable(decimalPredicate)
    // Groups 2,3,4 qualify => 3 groups * 100 rows
    assert(actualData.size === 300)
  }

  test("decimal filter pushdown - IS NULL on decimal column") {
    // Build 3 row groups (100 rows each):
    //  - group 0: all nulls
    //  - group 1: all non-nulls
    //  - group 2: mixed nulls/non-nulls
    val decimalNullTestSchema = sparktypes.StructType(Seq(
      sparktypes.StructField("id", sparktypes.IntegerType),
      sparktypes.StructField("decimal_col", sparktypes.DecimalType(10, 2))))

    val rows = Seq.range(0, 3).flatMap { groupIdx =>
      Seq.range(groupIdx * 100, (groupIdx + 1) * 100).map { rowId =>
        val v = groupIdx match {
          case 0 => null
          case 1 => BigDecimal.valueOf(123.45)
          case 2 => if (rowId % 2 == 0) null else BigDecimal.valueOf(234.56)
        }
        Row(rowId, v)
      }
    }

    val tempNullTable = Files.createTempDirectory("decimalNullTestDir").toString
    val nullDf = spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      decimalNullTestSchema)
    withSQLConf("parquet.block.size" -> 1.toString) {
      nullDf.repartition(1).write.format("delta").mode("overwrite").save(tempNullTable)
    }

    val predicate = isNull(col("decimal_col"))
    val readSchema: StructType = tableSchema(tempNullTable)
    val actualData = readParquetFilesUsingKernel(tempNullTable, readSchema, Optional.of(predicate))

    // Expect row groups 0 (all nulls) and 2 (mixed) to be kept: 2 groups * 100 rows
    assert(actualData.size === 200)
  }

  //////////////////////////////////////////////////////////////////////////////////
  // IN expression filter pushdown tests
  //////////////////////////////////////////////////////////////////////////////////

  test("IN expression filter pushdown - integer values (row-group prune)") {
    val inPredicate = predicate("IN", col("intCol"), ofInt(20), ofInt(120), ofInt(920))
    val actualData = readUsingKernel(testParquetTable, inPredicate)
    // Each chosen value resides in a different row group -> 3 groups * 100 rows
    assert(actualData.size === 300)
  }

  test("IN expression filter pushdown - string values (row-group prune)") {
    val inPredicate = predicate(
      "IN",
      col("stringCol"),
      ofString("%05d".format(100)),
      ofString("%05d".format(300)),
      ofString("%05d".format(500)))
    val actualData = readUsingKernel(testParquetTable, inPredicate)

    // 3 matching row groups -> 3 * 100 rows
    assert(actualData.size === 300)
  }

  test("IN expression filter pushdown - decimal values (row-group prune)") {
    val inPredicate = predicate(
      "IN",
      col("decimal_int32"),
      Literal.ofDecimal(new BigDecimal("123.45"), 9, 2),
      Literal.ofDecimal(new BigDecimal("345.67"), 9, 2))
    val actualData = readDecimalTable(inPredicate)
    // With our decimal table layout: group0 null, group1=234.56, group2=345.67, group3=456.78, group4=567.89
    // Only 345.67 matches -> 1 row group * 100 rows
    assert(actualData.size === 100)
  }

  test("IN expression filter pushdown - boolean values (row-group prune)") {
    val inPredicate = predicate("IN", col("booleanCol"), ofBoolean(true))
    val actualData = readUsingKernel(testParquetTable, inPredicate)
    // Should include multiple row groups; at least one full group (not 0 or 10) -> size multiple of 100
    assert(actualData.nonEmpty)
    assert(actualData.size % 100 === 0)
  }

  test("IN expression filter pushdown - mixed with null values (row-group prune)") {
    val inPredicate = predicate(
      "IN",
      col("intCol"),
      ofInt(20),
      ofNull(IntegerType.INTEGER),
      ofInt(120))
    val actualData = readUsingKernel(testParquetTable, inPredicate)
    // Matches row groups for 20 and 120 -> 2 groups * 100 rows
    assert(actualData.size === 200)
  }

  test("IN expression filter pushdown - single value (equivalent to equality, row-group prune)") {
    val inPredicate = predicate("IN", col("intCol"), ofInt(42))
    val eqPredicate = eq(col("intCol"), ofInt(42))

    val inResult = readUsingKernel(testParquetTable, inPredicate)
    val eqResult = readUsingKernel(testParquetTable, eqPredicate)

    // Both should prune down to the same row group
    assert(inResult.size === eqResult.size)
  }

  test("IN expression filter pushdown - empty values list") {
    // This tests the edge case where IN has only the column but no values
    // It should result in an unsupported filter (not pushed down)
    val inPredicate = predicate("IN", col("intCol"))
    assertConvertedFilterIsEmpty(inPredicate, testParquetTable)

    val actualData = readUsingKernel(testParquetTable, inPredicate)
    // Should return all data since filter is not pushed down
    assert(actualData.size === 2000) // 20 row groups * 100 rows each
  }

  test("IN expression filter pushdown - non-existent column") {
    val inPredicate = predicate("IN", col("nonExistentCol"), ofInt(1), ofInt(2))
    assertConvertedFilterIsEmpty(inPredicate, testParquetTable)

    val actualData = readUsingKernel(testParquetTable, inPredicate)
    // Should return all data since filter is not pushed down
    assert(actualData.size === 2000)
  }
}
