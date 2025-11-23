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

package org.apache.spark.sql.delta.catalog;

import io.delta.kernel.spark.table.SparkTable;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;

/**
 * A Spark catalog plugin for Delta Lake tables that implements the Spark DataSource V2 Catalog API.
 *
 * <p>To use this catalog, configure it in your Spark session:
 * <pre>{@code
 * // Scala example
 * val spark = SparkSession
 *   .builder()
 *   .appName("...")
 *   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
 *   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
 *   .getOrCreate()
 * }</pre>
 *
 * <h2>POC Architecture</h2>
 *
 * <p>This implementation always returns {@link SparkTable} (kernel-spark V2 connector)
 * instead of {@link DeltaTableV2} (delta-spark V1 connector).
 *
 * <p>Based on PR #5501 architecture but simplified for proof-of-concept:
 * no configuration checking, always use V2.
 */
public class DeltaCatalog extends AbstractDeltaCatalog {

  /**
   * Creates a catalog-based Delta table - POC: always return SparkTable.
   *
   * <p>This method is called by AbstractDeltaCatalog.loadTable() when a catalog table is found.
   *
   * @param ident Table identifier
   * @param catalogTable Catalog table metadata
   * @return SparkTable (V2 connector)
   */
  @Override
  public Table newDeltaCatalogBasedTable(Identifier ident, CatalogTable catalogTable) {
    // POC: Always use V2 (SparkTable)
    return new SparkTable(ident, catalogTable, new HashMap<>());
  }

  /**
   * Creates a path-based Delta table - POC: always return SparkTable.
   *
   * <p>This method is called by AbstractDeltaCatalog.loadTable() when no catalog table exists
   * but the identifier represents a path (e.g., delta.`/path/to/table`).
   *
   * @param ident Table identifier containing table path in ident.name()
   * @return SparkTable (V2 connector)
   */
  @Override
  public Table newDeltaPathTable(Identifier ident) {
    // POC: Always use V2 (SparkTable)
    // For path-based tables, Spark encodes the path in ident.name()
    return new SparkTable(ident, ident.name());
  }

  /**
   * Time Travel by timestamp - adds time travel options to SparkTable.
   *
   * <p>Called by Spark for SQL: {@code SELECT * FROM table TIMESTAMP AS OF '...'}
   *
   * @param ident the table identifier
   * @param timestamp the timestamp in microseconds since epoch
   * @return SparkTable with time travel options
   */
  @Override
  public Table loadTable(Identifier ident, long timestamp) {
    // First load the base table (will call our overridden methods above)
    Table table = loadTable(ident);

    // Add time travel options
    if (table instanceof SparkTable) {
      SparkTable sparkTable = (SparkTable) table;
      return sparkTable.withTimeTravelOptions(createTimestampOptions(timestamp));
    }

    // Fallback (shouldn't happen in POC)
    return super.loadTable(ident, timestamp);
  }

  /**
   * Time Travel by version - adds time travel options to SparkTable.
   *
   * <p>Called by Spark for SQL: {@code SELECT * FROM table VERSION AS OF 123}
   *
   * @param ident the table identifier
   * @param version the table version as a string
   * @return SparkTable with time travel options
   */
  @Override
  public Table loadTable(Identifier ident, String version) {
    // First load the base table (will call our overridden methods above)
    Table table = loadTable(ident);

    // Add time travel options
    if (table instanceof SparkTable) {
      SparkTable sparkTable = (SparkTable) table;
      return sparkTable.withTimeTravelOptions(createVersionOptions(version));
    }

    // Fallback (shouldn't happen in POC)
    return super.loadTable(ident, version);
  }

  /**
   * Creates options map for timestamp-based time travel.
   *
   * @param timestampMicros timestamp in microseconds since epoch (Spark's format)
   * @return options map with timestampAsOf in milliseconds (Delta's format)
   */
  private Map<String, String> createTimestampOptions(long timestampMicros) {
    Map<String, String> options = new HashMap<>();
    // Convert from Spark's microseconds to Delta's milliseconds
    long timestampMillis = timestampMicros / 1000;
    options.put("timestampAsOf", String.valueOf(timestampMillis));
    return options;
  }

  /**
   * Creates options map for version-based time travel.
   *
   * @param version the version string
   * @return options map with versionAsOf
   */
  private Map<String, String> createVersionOptions(String version) {
    Map<String, String> options = new HashMap<>();
    options.put("versionAsOf", version);
    return options;
  }
}
