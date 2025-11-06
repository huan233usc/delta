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

import org.apache.spark.sql.{QueryTest, SparkSession}
import org.scalatest.BeforeAndAfterAll

/**
 * Integration test suite for Delta Lake with Unity Catalog master branch.
 * 
 * This test suite:
 * 1. Starts an embedded Unity Catalog server before all tests
 * 2. Configures Spark to use the Unity Catalog connector
 * 3. Runs tests to verify Delta Lake and Unity Catalog integration
 * 4. Stops the server after all tests complete
 */
class UnityCatalogIntegrationSuite
  extends QueryTest
  with BeforeAndAfterAll
  with UnityCatalogIntegrationTestUtils {
  
  protected var _spark: SparkSession = _
  override def spark: SparkSession = _spark
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    // Start the embedded Unity Catalog server
    startUCServer()
    
    // Create test catalog and schema
    createUCCatalog(UC_CATALOG)
    createUCSchema(UC_CATALOG, UC_SCHEMA)
    
    // Create a shared Spark session for all tests
    _spark = createUCSparkSession()
    
    println(s"Unity Catalog test environment ready at ${getUCServerUrl()}")
  }
  
  override def afterAll(): Unit = {
    try {
      // Stop Spark session
      if (_spark != null) {
        _spark.stop()
      }
      // Stop the Unity Catalog server
      stopUCServer()
    } finally {
      super.afterAll()
    }
  }
  
  test("Unity Catalog connector should be available") {
    // Debug: Print Spark configuration
    println("=== Spark Configuration ===")
    spark.conf.getAll.filter(_._1.contains("catalog")).foreach { case (k, v) =>
      println(s"$k = $v")
    }
    
    // Verify that the Unity Catalog is accessible
    // Note: UCSingleCatalog might not appear in SHOW CATALOGS, but should be usable
    val catalogs = spark.sql("SHOW CATALOGS").collect()
    println(s"=== Available Catalogs ===")
    val catalogNames = catalogs.map(_.getString(0))
    catalogNames.foreach(c => println(s"  - $c"))
    
    // Try to list schemas in the Unity catalog to verify it's accessible
    try {
      val schemas = spark.sql(s"SHOW SCHEMAS IN $UC_CATALOG").collect()
      println(s"=== Schemas in $UC_CATALOG ===")
      schemas.foreach(s => println(s"  - ${s.getString(0)}"))
      
      // If we can list schemas, the catalog is working
      assert(schemas.nonEmpty, s"Unity Catalog '$UC_CATALOG' should have at least one schema")
    } catch {
      case e: Exception =>
        fail(s"Unity Catalog '$UC_CATALOG' is not accessible: ${e.getMessage}")
    }
  }
  
  test("Create and query Delta table in Unity Catalog") {
    val tableName = s"$UC_CATALOG.$UC_SCHEMA.test_table"
    val tableLocation = java.nio.file.Files.createTempDirectory("uc-delta-test").toString + "/test_table"
    println(s"Creating table: $tableName at location: $tableLocation")
    
    try {
      // Drop table if exists
      try {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      } catch {
        case _: Exception => // Ignore
      }
      
      // Create an external Delta table with explicit location (Unity Catalog requires this)
      spark.sql(s"""
        CREATE TABLE $tableName (
          id INT,
          name STRING
        ) USING DELTA
        LOCATION '$tableLocation'
      """)
      
      // Insert data
      spark.sql(s"""
        INSERT INTO $tableName VALUES 
        (1, 'Alice'),
        (2, 'Bob')
      """)
      
      // Query the table
      val result = spark.sql(s"SELECT * FROM $tableName ORDER BY id").collect()
      
      assert(result.length == 2)
      assert(result(0).getInt(0) == 1)
      assert(result(0).getString(1) == "Alice")
      assert(result(1).getInt(0) == 2)
      assert(result(1).getString(1) == "Bob")
    } finally {
      // Clean up
      try {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      } catch {
        case _: Exception => // Ignore cleanup errors
      }
    }
  }
  
  test("UPDATE and DELETE operations on Delta table in Unity Catalog") {
    val tableName = s"$UC_CATALOG.$UC_SCHEMA.test_update_delete"
    val tableLocation = java.nio.file.Files.createTempDirectory("uc-delta-test").toString + "/test_update_delete"
    
    try {
      // Create and populate external table with explicit location
      spark.sql(s"""
        CREATE TABLE $tableName (
          id INT,
          value STRING
        ) USING DELTA
        LOCATION '$tableLocation'
      """)
      
      spark.sql(s"""
        INSERT INTO $tableName VALUES 
        (1, 'original1'),
        (2, 'original2'),
        (3, 'original3')
      """)
      
      // UPDATE operation
      spark.sql(s"UPDATE $tableName SET value = 'updated' WHERE id = 2")
      
      val afterUpdate = spark.sql(s"SELECT value FROM $tableName WHERE id = 2").collect()
      assert(afterUpdate(0).getString(0) == "updated")
      
      // DELETE operation
      spark.sql(s"DELETE FROM $tableName WHERE id = 3")
      
      val afterDelete = spark.sql(s"SELECT COUNT(*) FROM $tableName").collect()
      assert(afterDelete(0).getLong(0) == 2)
    } finally {
      // Clean up
      try {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      } catch {
        case _: Exception => // Ignore cleanup errors
      }
    }
  }
  
  test("Verify Unity Catalog metadata for Delta table") {
    val tableName = s"$UC_CATALOG.$UC_SCHEMA.test_metadata"
    val tableLocation = java.nio.file.Files.createTempDirectory("uc-delta-test").toString + "/test_metadata"
    
    try {
      // Create external table with comment and explicit location
      spark.sql(s"""
        CREATE TABLE $tableName (
          id INT COMMENT 'ID column',
          data STRING COMMENT 'Data column'
        ) USING DELTA
        LOCATION '$tableLocation'
        COMMENT 'Test table for metadata verification'
      """)
      
      // Describe the table
      val description = spark.sql(s"DESCRIBE TABLE EXTENDED $tableName").collect()
      
      // Verify table exists and has correct format
      val formatInfo = description.filter(_.getString(0) == "Provider")
      assert(formatInfo.nonEmpty)
      assert(formatInfo(0).getString(1).toLowerCase.contains("delta"))
    } finally {
      // Clean up
      try {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      } catch {
        case _: Exception => // Ignore cleanup errors
      }
    }
  }
}
