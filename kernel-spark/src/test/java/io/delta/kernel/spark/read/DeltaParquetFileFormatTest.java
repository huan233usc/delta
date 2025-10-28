/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.spark.read;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for DeltaParquetFileFormat's Deletion Vector support.
 *
 * <p>These tests verify that DeltaParquetFileFormat correctly filters out deleted rows when reading
 * Delta tables with deletion vectors enabled.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DeltaParquetFileFormatTest {
  private SparkSession spark;

  @BeforeAll
  public void setUp() {
    spark =
        SparkSession.builder()
            .appName("DeltaParquetFileFormatDVTest")
            .master("local[2]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            // Register kernel-spark DSv2 catalog for testing
            .config("spark.sql.catalog.dsv2", "io.delta.kernel.spark.catalog.TestCatalog")
            .getOrCreate();
  }

  @AfterAll
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  @DisplayName("Test basic deletion vector filtering")
  public void testBasicDeletionVectorFiltering(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath() + "/dv_basic";

    // Create table with DV enabled
    spark.sql("DROP TABLE IF EXISTS test_dv_basic");
    spark
        .sql(
            "CREATE TABLE test_dv_basic (id LONG, value STRING) USING delta LOCATION '"
                + tablePath
                + "' "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
        .show();

    // Insert test data
    spark.sql("INSERT INTO test_dv_basic VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')");

    // Delete some rows
    spark.sql("DELETE FROM test_dv_basic WHERE id IN (2, 4)");

    // Read using kernel-spark (should filter deleted rows)
    Dataset<Row> result =
        spark.sql(String.format("SELECT * FROM dsv2.delta.`%s` ORDER BY id", tablePath));
    List<Row> rows = result.collectAsList();

    // Verify: should have 3 rows (1, 3, 5)
    assertEquals(3, rows.size(), "Expected 3 rows after deletion");
    assertEquals(1L, rows.get(0).getLong(0));
    assertEquals("a", rows.get(0).getString(1));
    assertEquals(3L, rows.get(1).getLong(0));
    assertEquals("c", rows.get(1).getString(1));
    assertEquals(5L, rows.get(2).getLong(0));
    assertEquals("e", rows.get(2).getString(1));

    spark.sql("DROP TABLE IF EXISTS test_dv_basic");
  }

  @Test
  @DisplayName("Test deletion vector with multiple deletes")
  public void testMultipleDeletions(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath() + "/dv_multiple";

    spark.sql("DROP TABLE IF EXISTS test_dv_multiple");
    spark
        .sql(
            "CREATE TABLE test_dv_multiple (id LONG, name STRING) USING delta LOCATION '"
                + tablePath
                + "' "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
        .show();

    // Insert data in batches
    spark.sql("INSERT INTO test_dv_multiple VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");
    spark.sql("INSERT INTO test_dv_multiple VALUES (4, 'David'), (5, 'Eve'), (6, 'Frank')");

    // Delete rows from different batches
    spark.sql("DELETE FROM test_dv_multiple WHERE id = 2");
    spark.sql("DELETE FROM test_dv_multiple WHERE id = 5");

    // Read and verify
    Dataset<Row> result =
        spark.sql(String.format("SELECT * FROM dsv2.delta.`%s` ORDER BY id", tablePath));
    List<Row> rows = result.collectAsList();

    assertEquals(4, rows.size(), "Expected 4 rows after deletions");
    assertEquals(1L, rows.get(0).getLong(0));
    assertEquals(3L, rows.get(1).getLong(0));
    assertEquals(4L, rows.get(2).getLong(0));
    assertEquals(6L, rows.get(3).getLong(0));

    spark.sql("DROP TABLE IF EXISTS test_dv_multiple");
  }

  @Test
  @DisplayName("Test deletion vector with vectorized reader")
  public void testDeletionVectorVectorized(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath() + "/dv_vectorized";

    // Enable vectorized reader
    spark.conf().set("spark.sql.parquet.enableVectorizedReader", "true");
    spark.conf().set("spark.sql.codegen.maxFields", "100");

    try {
      spark.sql("DROP TABLE IF EXISTS test_dv_vec");
      spark
          .sql(
              "CREATE TABLE test_dv_vec (id LONG, value INT) USING delta LOCATION '"
                  + tablePath
                  + "' "
                  + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
          .show();

      // Insert larger dataset to trigger vectorized reading
      StringBuilder insertQuery = new StringBuilder("INSERT INTO test_dv_vec VALUES ");
      for (int i = 1; i <= 100; i++) {
        insertQuery.append(String.format("(%d, %d)", i, i * 10));
        if (i < 100) insertQuery.append(", ");
      }
      spark.sql(insertQuery.toString());

      // Delete every 10th row
      spark.sql("DELETE FROM test_dv_vec WHERE id % 10 = 0");

      // Read and verify
      Dataset<Row> result =
          spark.sql(String.format("SELECT * FROM dsv2.delta.`%s` ORDER BY id", tablePath));
      List<Row> rows = result.collectAsList();

      assertEquals(90, rows.size(), "Expected 90 rows after deletion (deleted 10 rows)");

      // Verify no deleted rows are present
      for (Row row : rows) {
        long id = row.getLong(0);
        assertNotEquals(0, id % 10, "Deleted row with id=" + id + " should not be present");
      }

      spark.sql("DROP TABLE IF EXISTS test_dv_vec");
    } finally {
      // Reset config
      spark.conf().set("spark.sql.parquet.enableVectorizedReader", "true");
      spark.conf().set("spark.sql.codegen.maxFields", "100");
    }
  }

  @Test
  @DisplayName("Test table without deletion vectors")
  public void testTableWithoutDeletionVectors(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath() + "/no_dv";

    // Create table WITHOUT DV enabled
    spark.sql("DROP TABLE IF EXISTS test_no_dv");
    spark
        .sql(
            "CREATE TABLE test_no_dv (id LONG, value STRING) USING delta LOCATION '"
                + tablePath
                + "'")
        .show();

    // Insert and delete (will rewrite files instead of using DVs)
    spark.sql("INSERT INTO test_no_dv VALUES (1, 'a'), (2, 'b'), (3, 'c')");

    // Read using kernel-spark (should work fine even without DVs)
    Dataset<Row> result =
        spark.sql(String.format("SELECT * FROM dsv2.delta.`%s` ORDER BY id", tablePath));
    List<Row> rows = result.collectAsList();

    assertEquals(3, rows.size(), "Expected 3 rows");

    spark.sql("DROP TABLE IF EXISTS test_no_dv");
  }

  @Test
  @DisplayName("Test deletion vector with filter pushdown")
  public void testDeletionVectorWithFilterPushdown(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath() + "/dv_filter";

    spark.sql("DROP TABLE IF EXISTS test_dv_filter");
    spark
        .sql(
            "CREATE TABLE test_dv_filter (id LONG, category STRING, value INT) USING delta LOCATION '"
                + tablePath
                + "' "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
        .show();

    // Insert data
    spark.sql(
        "INSERT INTO test_dv_filter VALUES "
            + "(1, 'A', 100), (2, 'B', 200), (3, 'A', 300), "
            + "(4, 'B', 400), (5, 'A', 500), (6, 'B', 600)");

    // Delete some rows
    spark.sql("DELETE FROM test_dv_filter WHERE id IN (2, 5)");

    // Read with filter
    Dataset<Row> result =
        spark.sql(
            String.format(
                "SELECT * FROM dsv2.delta.`%s` WHERE category = 'A' ORDER BY id", tablePath));
    List<Row> rows = result.collectAsList();

    // Should have 2 rows (1 and 3), row 5 was deleted
    assertEquals(2, rows.size(), "Expected 2 rows in category A after deletion");
    assertEquals(1L, rows.get(0).getLong(0));
    assertEquals(3L, rows.get(1).getLong(0));

    spark.sql("DROP TABLE IF EXISTS test_dv_filter");
  }
}
