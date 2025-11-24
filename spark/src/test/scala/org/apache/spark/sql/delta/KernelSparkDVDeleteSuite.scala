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

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.QueryTest

/**
 * Test suite for DV-based DELETE operations using kernel-spark.
 *
 * This suite tests the full DV (Deletion Vector) flow:
 * 1. Create table with DV enabled
 * 2. Insert data
 * 3. Execute DELETE with DV (not COW)
 * 4. Verify data correctness
 * 5. Verify DV metadata in Delta log
 */
class KernelSparkDVDeleteSuite extends QueryTest with DeltaSQLCommandTest {

  import testImplicits._

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  test("kernel-spark DV DELETE - basic") {
    withTable("test_dv_delete") {
      cleanupTable("test_dv_delete")

      // Step 1: Create table with DV enabled
      spark.sql("""
        CREATE TABLE test_dv_delete (
          id BIGINT,
          name STRING,
          value DOUBLE
        ) USING delta
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
      """)

      // Step 2: Insert initial data
      Seq((1L, "Alice", 100.0), (2L, "Bob", 200.0), (3L, "Charlie", 300.0), (4L, "David", 400.0))
        .toDF("id", "name", "value")
        .write
        .format("delta")
        .mode("append")
        .insertInto("test_dv_delete")

      // Verify initial data
      checkAnswer(
        spark.table("test_dv_delete").orderBy("id"),
        Seq(
          Row(1L, "Alice", 100.0),
          Row(2L, "Bob", 200.0),
          Row(3L, "Charlie", 300.0),
          Row(4L, "David", 400.0)
        )
      )

      // Step 3: Execute DELETE using DV
      spark.sql("DELETE FROM test_dv_delete WHERE id = 2")

      // Step 4: Verify data after DELETE
      checkAnswer(
        spark.table("test_dv_delete").orderBy("id"),
        Seq(
          Row(1L, "Alice", 100.0),
          Row(3L, "Charlie", 300.0),
          Row(4L, "David", 400.0)
        )
      )

      // Step 5: Verify DV metadata in Delta log
      val deltaLog = DeltaLog.forTable(spark, "test_dv_delete")
      val snapshot = deltaLog.snapshot
      val addFilesWithDV = snapshot.allFiles.collect().filter(_.deletionVector != null)

      // Should have at least one file with DV
      assert(
        addFilesWithDV.nonEmpty,
        s"Expected at least one file with DV, but found none. " +
          s"Total files: ${snapshot.allFiles.count()}"
      )

      // Verify DV cardinality
      val totalDeletedRows = addFilesWithDV.map(_.deletionVector.cardinality).sum
      assert(
        totalDeletedRows == 1,
        s"Expected 1 deleted row in DV, but found $totalDeletedRows"
      )
    }
  }

  test("kernel-spark DV DELETE - multiple rows") {
    withTable("test_dv_delete_multi") {
      cleanupTable("test_dv_delete_multi")

      // Create table with DV enabled
      spark.sql("""
        CREATE TABLE test_dv_delete_multi (
          id BIGINT,
          category STRING,
          value DOUBLE
        ) USING delta
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
      """)

      // Insert data
      (1 to 100).map(i => (i.toLong, if (i % 2 == 0) "even" else "odd", i.toDouble))
        .toDF("id", "category", "value")
        .write
        .format("delta")
        .mode("append")
        .insertInto("test_dv_delete_multi")

      // Verify initial count
      assert(spark.table("test_dv_delete_multi").count() == 100)

      // Delete all even numbers
      spark.sql("DELETE FROM test_dv_delete_multi WHERE category = 'even'")

      // Verify data after DELETE
      val remaining = spark.table("test_dv_delete_multi").collect()
      assert(remaining.length == 50, s"Expected 50 rows, but found ${remaining.length}")
      assert(
        remaining.forall(_.getString(1) == "odd"),
        "All remaining rows should have category='odd'"
      )

      // Verify DV metadata
      val deltaLog = DeltaLog.forTable(spark, "test_dv_delete_multi")
      val snapshot = deltaLog.snapshot
      val addFilesWithDV = snapshot.allFiles.collect().filter(_.deletionVector != null)

      assert(addFilesWithDV.nonEmpty, "Expected files with DV")

      val totalDeletedRows = addFilesWithDV.map(_.deletionVector.cardinality).sum
      assert(
        totalDeletedRows == 50,
        s"Expected 50 deleted rows in DV, but found $totalDeletedRows"
      )
    }
  }

  test("kernel-spark DV DELETE - no matching rows") {
    withTable("test_dv_delete_nomatch") {
      cleanupTable("test_dv_delete_nomatch")

      // Create table with DV enabled
      spark.sql("""
        CREATE TABLE test_dv_delete_nomatch (
          id BIGINT,
          name STRING
        ) USING delta
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
      """)

      // Insert data
      Seq((1L, "Alice"), (2L, "Bob"), (3L, "Charlie"))
        .toDF("id", "name")
        .write
        .format("delta")
        .mode("append")
        .insertInto("test_dv_delete_nomatch")

      // Delete non-existent row
      spark.sql("DELETE FROM test_dv_delete_nomatch WHERE id = 999")

      // Verify all data still exists
      assert(spark.table("test_dv_delete_nomatch").count() == 3)

      // Verify no DV was created
      val deltaLog = DeltaLog.forTable(spark, "test_dv_delete_nomatch")
      val snapshot = deltaLog.snapshot
      val addFilesWithDV = snapshot.allFiles.collect().filter(_.deletionVector != null)

      // Should have no files with DV (or DV with 0 cardinality)
      val totalDeletedRows = addFilesWithDV.map(_.deletionVector.cardinality).sum
      assert(
        totalDeletedRows == 0,
        s"Expected 0 deleted rows, but found $totalDeletedRows"
      )
    }
  }

  test("kernel-spark DV DELETE - incremental DV updates") {
    withTable("test_dv_incremental") {
      cleanupTable("test_dv_incremental")

      // Create table with DV enabled
      spark.sql("""
        CREATE TABLE test_dv_incremental (
          id BIGINT,
          value DOUBLE
        ) USING delta
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
      """)

      // Insert data
      (1 to 10).map(i => (i.toLong, i.toDouble))
        .toDF("id", "value")
        .write
        .format("delta")
        .mode("append")
        .insertInto("test_dv_incremental")

      // First DELETE
      spark.sql("DELETE FROM test_dv_incremental WHERE id = 1")
      assert(spark.table("test_dv_incremental").count() == 9)

      // Second DELETE (should merge with existing DV)
      spark.sql("DELETE FROM test_dv_incremental WHERE id = 2")
      assert(spark.table("test_dv_incremental").count() == 8)

      // Third DELETE
      spark.sql("DELETE FROM test_dv_incremental WHERE id = 3")
      assert(spark.table("test_dv_incremental").count() == 7)

      // Verify DV metadata
      val deltaLog = DeltaLog.forTable(spark, "test_dv_incremental")
      val snapshot = deltaLog.snapshot
      val addFilesWithDV = snapshot.allFiles.collect().filter(_.deletionVector != null)

      assert(addFilesWithDV.nonEmpty, "Expected files with DV")

      val totalDeletedRows = addFilesWithDV.map(_.deletionVector.cardinality).sum
      assert(
        totalDeletedRows == 3,
        s"Expected 3 deleted rows (merged DV), but found $totalDeletedRows"
      )
    }
  }

  /**
   * Helper method to clean up table directory before creating a new table.
   */
  private def cleanupTable(tableName: String): Unit = {
    try {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      val warehouseDir = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
      val tablePath = s"$warehouseDir/$tableName"
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val path = new org.apache.hadoop.fs.Path(tablePath)
      if (fs.exists(path)) {
        fs.delete(path, true)
      }
    } catch {
      case _: Exception => // Ignore cleanup errors
    }
  }
}

