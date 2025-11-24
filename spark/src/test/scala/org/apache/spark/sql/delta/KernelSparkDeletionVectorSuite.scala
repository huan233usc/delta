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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

/**
 * Test suite for Deletion Vector support in kernel-spark.
 * 
 * Deletion Vectors (DVs) are a Delta Lake feature that allows efficient row-level deletes
 * without rewriting entire Parquet files. Instead of Copy-on-Write (COW), DVs maintain
 * a bitmap of deleted row positions.
 */
class KernelSparkDeletionVectorSuite extends QueryTest 
  with SharedSparkSession 
  with DeltaSQLCommandTest {
  import testImplicits._

  test("kernel-spark DELETE with Deletion Vectors - basic") {
    withTable("test_dv") {
      // Create table with DV enabled
      spark.sql("""
        CREATE TABLE test_dv (
          id BIGINT,
          name STRING,
          value DOUBLE
        ) USING delta
        TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'true'
        )
      """)

      // Insert initial data
      Seq((1L, "Alice", 100.0), (2L, "Bob", 200.0), (3L, "Charlie", 300.0))
        .toDF("id", "name", "value")
        .write
        .format("delta")
        .mode("append")
        .insertInto("test_dv")

      // Verify initial data
      checkAnswer(
        spark.table("test_dv").orderBy("id"),
        Seq((1L, "Alice", 100.0), (2L, "Bob", 200.0), (3L, "Charlie", 300.0))
          .toDF("id", "name", "value")
      )

      // Delete one row - should use DV instead of COW
      spark.sql("DELETE FROM test_dv WHERE id = 2")

      // Verify deletion
      checkAnswer(
        spark.table("test_dv").orderBy("id"),
        Seq((1L, "Alice", 100.0), (3L, "Charlie", 300.0))
          .toDF("id", "name", "value")
      )

      // Verify that DV was used (no new AddFile, only DV metadata)
      val tableId = TableIdentifier("test_dv")
      val deltaLog = DeltaLog.forTable(spark, tableId)
      val lastVersion = deltaLog.update().version
      val lastCommit = deltaLog.getChanges(lastVersion).next()._2
      
      // Check that we have RemoveFile with DV descriptor
      val removeFiles = lastCommit.collect { case r: actions.RemoveFile => r }
      assert(removeFiles.nonEmpty, "Should have RemoveFile actions")
      
      // Check that we have AddFile with DV descriptor
      val addFiles = lastCommit.collect { case a: actions.AddFile => a }
      assert(addFiles.nonEmpty, "Should have AddFile with DV")
      assert(addFiles.exists(_.deletionVector != null), "AddFile should have deletion vector")
    }
  }

  test("kernel-spark DELETE with Deletion Vectors - multiple deletes") {
    withTable("test_dv_multi") {
      // Create table with DV enabled
      spark.sql("""
        CREATE TABLE test_dv_multi (
          id BIGINT,
          category STRING,
          amount DOUBLE
        ) USING delta
        TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'true'
        )
      """)

      // Insert test data
      (1 to 100).map(i => (i.toLong, if (i % 2 == 0) "even" else "odd", i.toDouble))
        .toDF("id", "category", "amount")
        .write
        .format("delta")
        .mode("append")
        .insertInto("test_dv_multi")

      // Verify initial count
      assert(spark.table("test_dv_multi").count() == 100)

      // Delete all even numbers
      spark.sql("DELETE FROM test_dv_multi WHERE category = 'even'")

      // Verify deletion
      assert(spark.table("test_dv_multi").count() == 50)
      assert(spark.table("test_dv_multi").filter("category = 'odd'").count() == 50)

      // Delete specific odd numbers
      spark.sql("DELETE FROM test_dv_multi WHERE id IN (1, 3, 5, 7, 9)")

      // Verify final count
      assert(spark.table("test_dv_multi").count() == 45)
    }
  }

  test("kernel-spark UPDATE with Deletion Vectors") {
    withTable("test_dv_update") {
      // Create table with DV enabled
      spark.sql("""
        CREATE TABLE test_dv_update (
          id BIGINT,
          status STRING,
          score INT
        ) USING delta
        TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'true'
        )
      """)

      // Insert test data
      Seq((1L, "pending", 10), (2L, "pending", 20), (3L, "active", 30))
        .toDF("id", "status", "score")
        .write
        .format("delta")
        .mode("append")
        .insertInto("test_dv_update")

      // Update with DV - should mark old rows as deleted and insert new rows
      spark.sql("UPDATE test_dv_update SET status = 'active', score = score + 100 WHERE status = 'pending'")

      // Verify update
      checkAnswer(
        spark.table("test_dv_update").orderBy("id"),
        Seq((1L, "active", 110), (2L, "active", 120), (3L, "active", 30))
          .toDF("id", "status", "score")
      )
    }
  }

  test("kernel-spark READ table with existing Deletion Vectors") {
    withTable("test_dv_read") {
      // Create table with DV enabled using V1 writer
      spark.sql("""
        CREATE TABLE test_dv_read (
          id BIGINT,
          data STRING
        ) USING delta
        TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'true'
        )
      """)

      // Insert and delete using V1 (to create DVs)
      Seq((1L, "a"), (2L, "b"), (3L, "c"), (4L, "d"), (5L, "e"))
        .toDF("id", "data")
        .write
        .format("delta")
        .mode("append")
        .saveAsTable("test_dv_read")

      spark.sql("DELETE FROM test_dv_read WHERE id IN (2, 4)")

      // Now read using kernel-spark (V2)
      // Should correctly apply DVs and return only non-deleted rows
      val result = spark.table("test_dv_read").orderBy("id").collect()
      assert(result.length == 3)
      assert(result.map(_.getLong(0)).toSeq == Seq(1L, 3L, 5L))
    }
  }

  test("kernel-spark DV with filters - data skipping") {
    withTable("test_dv_filter") {
      // Create partitioned table with DV
      spark.sql("""
        CREATE TABLE test_dv_filter (
          id BIGINT,
          partition_col STRING,
          value INT
        ) USING delta
        PARTITIONED BY (partition_col)
        TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'true'
        )
      """)

      // Insert data across partitions
      (1 to 100).map(i => (i.toLong, if (i <= 50) "p1" else "p2", i))
        .toDF("id", "partition_col", "value")
        .write
        .format("delta")
        .mode("append")
        .insertInto("test_dv_filter")

      // Delete some rows in p1
      spark.sql("DELETE FROM test_dv_filter WHERE partition_col = 'p1' AND id % 10 = 0")

      // Query with partition filter - should skip p2 entirely
      val result = spark.table("test_dv_filter")
        .filter("partition_col = 'p1'")
        .count()

      // p1 had 50 rows, deleted 5 (10, 20, 30, 40, 50)
      assert(result == 45)
    }
  }
}

