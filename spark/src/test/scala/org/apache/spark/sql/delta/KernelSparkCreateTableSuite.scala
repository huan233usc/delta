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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

/**
 * Test suite for kernel-spark CREATE TABLE operations.
 *
 * Tests:
 * - CREATE TABLE with schema
 * - CREATE TABLE with partition columns
 * - CREATE TABLE with table properties
 * - CREATE TABLE IF NOT EXISTS
 */
class KernelSparkCreateTableSuite
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  import testImplicits._

  test("kernel-spark CREATE TABLE - simple table") {
    withTable("test_create") {
      // Explicitly clean up any leftover table directory from previous test runs
      val tablePath = new java.io.File(
        spark.sessionState.catalog.defaultTablePath(
          spark.sessionState.sqlParser.parseTableIdentifier("test_create")).toString)
      if (tablePath.exists()) {
        org.apache.commons.io.FileUtils.deleteDirectory(tablePath)
      }
      
      spark.sql("""
        CREATE TABLE test_create (
          id BIGINT,
          name STRING,
          value DOUBLE
        ) USING delta
      """)

      // Verify table exists
      assert(spark.catalog.tableExists("test_create"))

      // Verify schema
      val schema = spark.table("test_create").schema
      assert(schema.fieldNames === Array("id", "name", "value"))

      // Verify can write and read
      Seq((1L, "Alice", 100.0), (2L, "Bob", 200.0))
        .toDF("id", "name", "value")
        .write
        .format("delta")
        .mode("append")
        .insertInto("test_create")

      checkAnswer(
        spark.table("test_create"),
        Seq((1L, "Alice", 100.0), (2L, "Bob", 200.0)).toDF("id", "name", "value")
      )
    }
  }

  ignore("kernel-spark CREATE TABLE - with partition (TODO: dynamic partitioning support)") {
    withTable("test_create_partition") {
      spark.sql("""
        CREATE TABLE test_create_partition (
          id BIGINT,
          name STRING,
          value DOUBLE,
          partition_col STRING
        ) USING delta
        PARTITIONED BY (partition_col)
      """)

      // Verify table exists
      assert(spark.catalog.tableExists("test_create_partition"))

      // Verify can write and read
      Seq((1L, "Alice", 100.0, "A"), (2L, "Bob", 200.0, "B"))
        .toDF("id", "name", "value", "partition_col")
        .write
        .format("delta")
        .mode("append")
        .insertInto("test_create_partition")

      checkAnswer(
        spark.table("test_create_partition"),
        Seq((1L, "Alice", 100.0, "A"), (2L, "Bob", 200.0, "B"))
          .toDF("id", "name", "value", "partition_col")
      )
    }
  }

  test("kernel-spark CREATE TABLE - with table properties") {
    withTable("test_create_props") {
      // Explicitly clean up any leftover table directory from previous test runs
      val tablePath = new java.io.File(
        spark.sessionState.catalog.defaultTablePath(
          spark.sessionState.sqlParser.parseTableIdentifier("test_create_props")).toString)
      if (tablePath.exists()) {
        org.apache.commons.io.FileUtils.deleteDirectory(tablePath)
      }
      
      spark.sql("""
        CREATE TABLE test_create_props (
          id BIGINT,
          value DOUBLE
        ) USING delta
        TBLPROPERTIES (
          'delta.minReaderVersion' = '1',
          'delta.minWriterVersion' = '2',
          'custom.property' = 'test-value'
        )
      """)

      // Verify table exists
      assert(spark.catalog.tableExists("test_create_props"))

      // Verify can write and read
      Seq((1L, 100.0), (2L, 200.0))
        .toDF("id", "value")
        .write
        .format("delta")
        .mode("append")
        .insertInto("test_create_props")

      checkAnswer(
        spark.table("test_create_props"),
        Seq((1L, 100.0), (2L, 200.0)).toDF("id", "value")
      )
    }
  }

  test("kernel-spark CREATE TABLE IF NOT EXISTS") {
    withTable("test_create_if_not_exists") {
      // Explicitly clean up any leftover table directory from previous test runs
      val tablePath = new java.io.File(
        spark.sessionState.catalog.defaultTablePath(
          spark.sessionState.sqlParser.parseTableIdentifier("test_create_if_not_exists")).toString)
      if (tablePath.exists()) {
        org.apache.commons.io.FileUtils.deleteDirectory(tablePath)
      }
      
      // First create
      spark.sql("""
        CREATE TABLE IF NOT EXISTS test_create_if_not_exists (
          id BIGINT,
          value DOUBLE
        ) USING delta
      """)

      // Insert data
      Seq((1L, 100.0)).toDF("id", "value")
        .write.format("delta").mode("append").insertInto("test_create_if_not_exists")

      // Second create should not fail
      spark.sql("""
        CREATE TABLE IF NOT EXISTS test_create_if_not_exists (
          id BIGINT,
          value DOUBLE
        ) USING delta
      """)

      // Data should still be there
      checkAnswer(
        spark.table("test_create_if_not_exists"),
        Seq((1L, 100.0)).toDF("id", "value")
      )
    }
  }
}

