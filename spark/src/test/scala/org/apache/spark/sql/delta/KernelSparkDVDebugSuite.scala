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
 * Debug suite to check if DV is being created correctly.
 */
class KernelSparkDVDebugSuite extends QueryTest with DeltaSQLCommandTest {

  import testImplicits._

  test("kernel-spark DV DEBUG - check if DV is created") {
    withTable("test_dv_debug") {
      cleanupTable("test_dv_debug")

      // Create table with DV enabled
      spark.sql("""
        CREATE TABLE test_dv_debug (
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
        .insertInto("test_dv_debug")

      println("=== BEFORE DELETE ===")
      val beforeCount = spark.table("test_dv_debug").count()
      println(s"Row count: $beforeCount")
      
      // Get the actual table path
      val tablePath = spark.sessionState.catalog.getTableMetadata(
        org.apache.spark.sql.catalyst.TableIdentifier("test_dv_debug")).location.toString
      println(s"Table path: $tablePath")
      
      val deltaLogBefore = DeltaLog.forTable(spark, tablePath)
      val snapshotBefore = deltaLogBefore.snapshot
      println(s"Version before: ${snapshotBefore.version}")
      println(s"Files before: ${snapshotBefore.allFiles.count()}")

      // Execute DELETE
      println("\n=== EXECUTING DELETE ===")
      spark.sql("DELETE FROM test_dv_debug WHERE id = 2")

      println("\n=== AFTER DELETE ===")
      val deltaLogAfter = DeltaLog.forTable(spark, tablePath)
      val snapshotAfter = deltaLogAfter.snapshot
      println(s"Version after: ${snapshotAfter.version}")
      println(s"Files after: ${snapshotAfter.allFiles.count()}")
      
      val allFiles = snapshotAfter.allFiles.collect()
      println(s"\nTotal files: ${allFiles.length}")
      
      allFiles.foreach { addFile =>
        println(s"\nFile: ${addFile.path}")
        println(s"  Size: ${addFile.size}")
        println(s"  DV: ${addFile.deletionVector}")
        if (addFile.deletionVector != null) {
          println(s"  DV Cardinality: ${addFile.deletionVector.cardinality}")
          println(s"  DV Storage Type: ${addFile.deletionVector.storageType}")
        }
      }

      val afterCount = spark.table("test_dv_debug").count()
      println(s"\nRow count after DELETE: $afterCount")
      
      val rows = spark.table("test_dv_debug").collect()
      println(s"Actual rows: ${rows.mkString(", ")}")
    }
  }

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

