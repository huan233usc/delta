/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

/**
 * Test suite for kernel-spark (V2 connector) write operations.
 */
class KernelSparkWriteSuite extends QueryTest with DeltaSQLCommandTest {

  test("kernel-spark V2 batch write - simple append") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath

      // Create table using V1 (to ensure table exists)
      spark.range(5).write.format("delta").save(path)

      // Read current data (should have 5 rows)
      checkAnswer(
        spark.read.format("delta").load(path),
        (0 until 5).map(i => Row(i.toLong))
      )

      // Append using V2 write
      // Note: DeltaCatalog should return SparkTable which implements SupportsWrite
      spark.range(5, 10).write.format("delta").mode("append").save(path)

      // Read all data (should have 10 rows)
      checkAnswer(
        spark.read.format("delta").load(path),
        (0 until 10).map(i => Row(i.toLong))
      )
    }
  }

  test("kernel-spark V2 batch write - append to existing table") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      
      // Create table with initial data
      spark.range(5).toDF("id").write.format("delta").save(path)
      
      // Append more data using DataFrame API
      spark.range(5, 10).toDF("id").write.format("delta").mode("append").save(path)
      
      // Verify all data
      val readDf = spark.read.format("delta").load(path)
      checkAnswer(readDf, spark.range(10).toDF("id"))
      
      // Verify transaction log
      val deltaLog = DeltaLog.forTable(spark, path)
      assert(deltaLog.update().version === 1)
    }
  }

  test("kernel-spark V2 batch write - empty dataframe") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath

      // Create table
      spark.range(5).write.format("delta").save(path)

      // Append empty dataframe
      spark.range(0).write.format("delta").mode("append").save(path)

      // Should still have original 5 rows
      checkAnswer(
        spark.read.format("delta").load(path),
        (0 until 5).map(i => Row(i.toLong))
      )
    }
  }

  test("kernel-spark V2 batch write - multiple columns") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath

      // Create table with multiple columns
      val df1 = spark.createDataFrame(Seq(
        (1, "Alice", 30),
        (2, "Bob", 25)
      )).toDF("id", "name", "age")

      df1.write.format("delta").save(path)

      // Append more data
      val df2 = spark.createDataFrame(Seq(
        (3, "Charlie", 35),
        (4, "David", 28)
      )).toDF("id", "name", "age")

      df2.write.format("delta").mode("append").save(path)

      // Verify all data
      checkAnswer(
        spark.read.format("delta").load(path).orderBy("id"),
        Seq(
          Row(1, "Alice", 30),
          Row(2, "Bob", 25),
          Row(3, "Charlie", 35),
          Row(4, "David", 28)
        )
      )
    }
  }

  test("kernel-spark V2 batch write - large dataset") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath

      // Create table
      spark.range(100).write.format("delta").save(path)

      // Append large dataset
      spark.range(100, 1000).write.format("delta").mode("append").save(path)

      // Verify row count
      val count = spark.read.format("delta").load(path).count()
      assert(count === 1000, s"Expected 1000 rows but got $count")

      // Verify some sample data
      val result = spark.read.format("delta").load(path)
        .filter("id = 500")
        .collect()
      
      assert(result.length === 1, "Should find exactly one row with id=500")
      assert(result(0).getLong(0) === 500L, "Row should have id=500")
    }
  }

  test("kernel-spark V2 batch write - verify transaction log") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath

      // Initial write
      spark.range(5).write.format("delta").save(path)

      // Append
      spark.range(5, 10).write.format("delta").mode("append").save(path)

      // Check Delta log
      val deltaLog = DeltaLog.forTable(spark, path)
      val snapshot = deltaLog.snapshot

      // Should have version 1 (0 = initial, 1 = append)
      assert(snapshot.version === 1, s"Expected version 1 but got ${snapshot.version}")

      // Verify file count
      val addFiles = snapshot.allFiles.collect()
      assert(addFiles.length >= 1, "Should have at least one data file")

      // Verify total record count matches
      val totalRecords = addFiles.map(_.numLogicalRecords.getOrElse(0L)).sum
      assert(totalRecords === 10, s"Expected 10 records but got $totalRecords")
    }
  }

  test("kernel-spark V2 batch write with time travel read") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath

      // Version 0: Initial write
      spark.range(5).write.format("delta").save(path)

      // Version 1: Append
      spark.range(5, 10).write.format("delta").mode("append").save(path)

      // Version 2: Append more
      spark.range(10, 15).write.format("delta").mode("append").save(path)

      // Read current version (should have 15 rows)
      assert(spark.read.format("delta").load(path).count() === 15)

      // Time travel to version 0 (should have 5 rows)
      assert(
        spark.read.format("delta").option("versionAsOf", "0").load(path).count() === 5
      )

      // Time travel to version 1 (should have 10 rows)
      assert(
        spark.read.format("delta").option("versionAsOf", "1").load(path).count() === 10
      )
    }
  }
}

