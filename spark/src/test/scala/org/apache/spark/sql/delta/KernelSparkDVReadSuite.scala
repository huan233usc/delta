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
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

/**
 * Test suite for reading tables with Deletion Vectors using kernel-spark.
 * 
 * This tests that kernel-spark can correctly read tables that have DVs,
 * filtering out deleted rows.
 */
class KernelSparkDVReadSuite extends QueryTest with DeltaSQLCommandTest {
  import testImplicits._

  test("kernel-spark READ table with Deletion Vectors") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      
      // Create table with DV enabled using V1 writer
      sql(s"""
        CREATE TABLE delta.`$path` (
          id BIGINT,
          name STRING,
          value DOUBLE
        ) USING delta
        TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'true'
        )
      """)

      // Insert data using V1
      Seq((1L, "Alice", 100.0), (2L, "Bob", 200.0), (3L, "Charlie", 300.0), (4L, "David", 400.0))
        .toDF("id", "name", "value")
        .write
        .format("delta")
        .mode("append")
        .save(path)

      // Delete using V1 (creates DV)
      sql(s"DELETE FROM delta.`$path` WHERE id IN (2, 4)")

      // Read using kernel-spark (V2)
      // Should correctly apply DVs and return only non-deleted rows
      val result = spark.read.format("delta").load(path).orderBy("id")
      
      checkAnswer(
        result,
        Seq((1L, "Alice", 100.0), (3L, "Charlie", 300.0)).toDF("id", "name", "value")
      )
    }
  }

  test("kernel-spark READ partitioned table with Deletion Vectors") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      
      // Create partitioned table with DV
      sql(s"""
        CREATE TABLE delta.`$path` (
          id BIGINT,
          category STRING,
          value INT
        ) USING delta
        PARTITIONED BY (category)
        TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'true'
        )
      """)

      // Insert data
      (1 to 100).map(i => (i.toLong, if (i % 2 == 0) "even" else "odd", i))
        .toDF("id", "category", "value")
        .write
        .format("delta")
        .mode("append")
        .save(path)

      // Delete some rows in each partition
      sql(s"DELETE FROM delta.`$path` WHERE id % 10 = 0")

      // Read with partition filter
      val result = spark.read.format("delta").load(path)
        .filter("category = 'even'")
        .count()

      // even partition: 50 rows, deleted 5 (10, 20, 30, 40, 50)
      assert(result == 45)
    }
  }

  test("kernel-spark READ with filters on DV table") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      
      sql(s"""
        CREATE TABLE delta.`$path` (
          id BIGINT,
          status STRING,
          score INT
        ) USING delta
        TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'true'
        )
      """)

      // Insert data
      (1 to 50).map(i => (i.toLong, if (i <= 25) "active" else "inactive", i * 10))
        .toDF("id", "status", "score")
        .write
        .format("delta")
        .mode("append")
        .save(path)

      // Delete some active rows
      sql(s"DELETE FROM delta.`$path` WHERE status = 'active' AND id % 5 = 0")

      // Read with filter - should apply both DV and data filter
      val result = spark.read.format("delta").load(path)
        .filter("status = 'active' AND score > 100")
        .count()

      // active: id 1-25, deleted: 5, 10, 15, 20, 25
      // remaining active: 1-4, 6-9, 11-14, 16-19, 21-24 (20 rows)
      // filter score > 100: id >= 11, so 11-14, 16-19, 21-24 (12 rows)
      assert(result == 12)
    }
  }
}

