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

import io.delta.kernel.TableManager;
import io.delta.kernel.Transaction;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.spark.table.SparkTable;
import io.delta.kernel.spark.utils.SchemaUtils;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.Transform;
import scala.collection.JavaConverters;

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

  /**
   * Creates a new Delta table using Kernel API.
   *
   * <p>This overrides the V1 implementation to use Kernel's CreateTableTransaction.
   *
   * @param ident the table identifier
   * @param schema the table schema
   * @param partitions the partition transforms
   * @param properties the table properties
   * @return the newly created SparkTable
   */
  @Override
  public Table createDeltaTableImpl(
      Identifier ident,
      org.apache.spark.sql.types.StructType schema,
      Transform[] partitions,
      java.util.Map<String, String> properties) {

    // Get table path from identifier
    String tablePath = getTablePath(ident, properties);

    // Debug: Check if table directory exists before we start
    try {
      java.io.File tableDir = new java.io.File(new java.net.URI(tablePath));
    } catch (Exception e) {
      System.out.println("DEBUG [DeltaCatalog.createDeltaTableImpl]: Error checking directory: " + e.getMessage());
    }

    // Convert Spark schema to Kernel schema
    StructType kernelSchema = SchemaUtils.convertSparkSchemaToKernelSchema(schema);

    // Convert partitions to partition column names
    List<String> partitionColumns = extractPartitionColumns(partitions);

    try {
      // Build Kernel create table transaction
      // Use empty options for Hadoop conf - properties are handled separately by Kernel
      scala.collection.immutable.Map<String, String> emptyScalaMap =
          scala.collection.immutable.Map$.MODULE$.empty();
      Configuration hadoopConf =
          SparkSession.active().sessionState().newHadoopConfWithOptions(emptyScalaMap);
      Engine engine = io.delta.kernel.defaults.engine.DefaultEngine.create(hadoopConf);

      CreateTableTransactionBuilder txnBuilder =
          TableManager.buildCreateTableTransaction(
              tablePath,
              kernelSchema,
              "kernel-spark" // engineInfo
          );

      // Filter out protocol-level properties that Kernel manages automatically
      // These include: delta.minReaderVersion, delta.minWriterVersion, etc.
      if (properties != null && !properties.isEmpty()) {
        Map<String, String> filteredProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          String key = entry.getKey();
          // Skip protocol-related properties - Kernel manages these automatically
          if (!key.equals("delta.minReaderVersion") && !key.equals("delta.minWriterVersion")) {
            filteredProperties.put(key, entry.getValue());
          }
        }
        if (!filteredProperties.isEmpty()) {
          txnBuilder.withTableProperties(filteredProperties);
        }
      }

      // Set partition columns if present
      if (!partitionColumns.isEmpty()) {
        List<io.delta.kernel.expressions.Column> kernelPartitionColumns = new ArrayList<>();
        for (String colName : partitionColumns) {
          kernelPartitionColumns.add(new io.delta.kernel.expressions.Column(colName));
        }
        DataLayoutSpec dataLayoutSpec = DataLayoutSpec.partitioned(kernelPartitionColumns);
        txnBuilder.withDataLayoutSpec(dataLayoutSpec);
      }

      // Build and commit the transaction
      // Debug: Check directory again before Kernel build
      try {
        java.io.File tableDir = new java.io.File(new java.net.URI(tablePath));
        System.out.println("DEBUG [Before txnBuilder.build()]: tableDir.exists()=" + tableDir.exists());
        if (tableDir.exists() && tableDir.isDirectory()) {
          System.out.println("DEBUG [Before txnBuilder.build()]: tableDir contents:");
          String[] files = tableDir.list();
          if (files != null) {
            for (String file : files) {
              System.out.println("  - " + file);
            }
          }
        }
      } catch (Exception e) {
        System.out.println("DEBUG [Before txnBuilder.build()]: Error: " + e.getMessage());
      }
      
      Transaction transaction = txnBuilder.build(engine);
      // Empty data actions for CREATE TABLE (no initial data)
      CloseableIterable<io.delta.kernel.data.Row> emptyActions =
          CloseableIterable.emptyIterable();
      transaction.commit(engine, emptyActions);
      System.out.println("DEBUG [After transaction.commit()]: Table created successfully, Delta log exists at " + tablePath);
      
      // Register table in HMS via SessionCatalog
      System.out.println("DEBUG [Before catalog().createTable()]: Registering table in HMS");
      try {
        // Build TableIdentifier
        org.apache.spark.sql.catalyst.TableIdentifier tableId =
            new org.apache.spark.sql.catalyst.TableIdentifier(
                ident.name(),
                scala.Option.apply(ident.namespace().length > 0 ? ident.namespace()[0] : null));
        
        // Build CatalogStorageFormat
        scala.collection.immutable.Map<String, String> emptyMap =
            scala.collection.immutable.Map$.MODULE$.empty();
        org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat storage =
            new org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat(
                scala.Option.apply(new java.net.URI(tablePath)), // locationUri
                scala.Option.empty(), // inputFormat
                scala.Option.empty(), // outputFormat
                scala.Option.empty(), // serde
                false, // compressed
                emptyMap); // properties
        
        // Convert partition columns to Scala immutable Seq
        scala.collection.immutable.Seq<String> partitionSeq;
        if (partitionColumns.isEmpty()) {
          partitionSeq = (scala.collection.immutable.Seq<String>) (Object) scala.collection.immutable.Nil$.MODULE$;
        } else {
          partitionSeq = scala.collection.JavaConverters.asScalaBuffer(partitionColumns).toList();
        }
        
        // Build CatalogTable (Spark 3.5 API)
        org.apache.spark.sql.catalyst.catalog.CatalogTable catalogTable =
            org.apache.spark.sql.catalyst.catalog.CatalogTable.apply(
                tableId, // identifier
                org.apache.spark.sql.catalyst.catalog.CatalogTableType.EXTERNAL(), // tableType
                storage, // storage
                schema, // schema
                scala.Option.apply("delta"), // provider
                partitionSeq, // partitionColumnNames
                scala.Option.empty(), // bucketSpec
                System.getProperty("user.name"), // owner
                System.currentTimeMillis(), // createTime
                System.currentTimeMillis(), // lastAccessTime
                org.apache.spark.package$.MODULE$.SPARK_VERSION(), // createVersion
                emptyMap, // properties
                scala.Option.empty(), // stats
                scala.Option.empty(), // viewText
                scala.Option.empty(), // comment
                (scala.collection.immutable.Seq<String>) (Object) scala.collection.immutable.Nil$.MODULE$, // unsupportedFeatures
                false, // tracksPartitionsInCatalog
                false, // schemaPreservesCase
                emptyMap, // ignoredProperties
                scala.Option.empty()); // catalogMetadataLocation
        
        // Register in SessionCatalog
        SparkSession.active().sessionState().catalog().createTable(
            catalogTable,
            false, // ignoreIfExists
            false); // validateLocation
        
        System.out.println("DEBUG [After catalog().createTable()]: Table registered in HMS");
      } catch (Exception e) {
        System.out.println("DEBUG [HMS registration error]: " + e.getMessage());
        e.printStackTrace(System.out);
        // Don't fail if HMS registration fails - we can still use path-based table
      }

      // The table is now created on the filesystem with Delta log
      // But it's not registered in HMS yet
      // Solution: Return a path-based SparkTable directly
      System.out.println("DEBUG [Creating path-based SparkTable]: tablePath=" + tablePath);
      
      // Create a path-based identifier for loading
      // This will make loadTable() treat it as a path-based table
      // Use "delta" as namespace to satisfy single-part namespace requirement
      Identifier pathIdent = Identifier.of(new String[]{"delta"}, tablePath);
      Table result = loadTable(pathIdent);
      
      System.out.println("DEBUG [After loadTable()]: Path-based table loaded successfully");
      return result;

    } catch (io.delta.kernel.exceptions.TableAlreadyExistsException e) {
      // Convert Kernel's TableAlreadyExistsException to Spark's TableAlreadyExistsException
      // Use uncheckedThrow to bypass Java's checked exception requirement
      // when overriding Scala methods that don't declare throws
      System.out.println("DEBUG [catch block]: Caught Kernel TableAlreadyExistsException: " + e.getMessage());
      e.printStackTrace(System.out);
      throw uncheckedThrow(
          new org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException(ident));
    } catch (Exception e) {
      System.out.println("DEBUG [catch block]: Caught generic exception: " + e.getClass().getName() + ": " + e.getMessage());
      e.printStackTrace(System.out);
      throw new RuntimeException("Failed to create Delta table: " + tablePath, e);
    }
  }

  /**
   * Utility method to throw checked exceptions as unchecked.
   * 
   * <p>This is a workaround for Java's checked exception requirement when overriding Scala methods.
   * The Scala method signature doesn't declare throws, but we need to throw checked exceptions
   * that Spark expects (like TableAlreadyExistsException). This method uses Java's type erasure
   * to throw checked exceptions without declaring them.
   */
  @SuppressWarnings("unchecked")
  private static <E extends Throwable> RuntimeException uncheckedThrow(Throwable e) throws E {
    throw (E) e;
  }

  /**
   * Gets the table path from the identifier and properties.
   *
   * <p>For catalog tables, uses the catalog's warehouse path + namespace + table name.
   * For path-based tables, extracts the path from the identifier.
   *
   * @param ident the table identifier
   * @param properties the table properties (may contain location)
   * @return the table path
   */
  private String getTablePath(Identifier ident, Map<String, String> properties) {
    // Check if location is provided in properties
    if (properties != null && properties.containsKey("location")) {
      return properties.get("location");
    }

    // For path-based tables (delta.`/path/to/table`)
    if (ident.namespace().length == 0 || isPathIdentifier(ident.name())) {
      return ident.name();
    }

    // For catalog tables, use catalog's table path resolution
    // Delegate to parent to construct the path from warehouse + namespace + table
    try {
      org.apache.spark.sql.catalyst.TableIdentifier sparkIdent =
          new org.apache.spark.sql.catalyst.TableIdentifier(
              ident.name(),
              scala.Option.apply(String.join(".", ident.namespace())));
      
      java.net.URI tableUri = 
          spark().sessionState().catalog().defaultTablePath(sparkIdent);
      
      if (tableUri != null) {
        return tableUri.toString();
      }
    } catch (Exception e) {
      // Fall back to simple path construction
    }

    // Fallback: construct path from namespace and table name
    String namespace = String.join("/", ident.namespace());
    return String.format("%s/%s/%s", defaultNamespace(), namespace, ident.name());
  }

  /**
   * Checks if the identifier name represents a path (starts with / or contains ://).
   */
  private boolean isPathIdentifier(String name) {
    return name != null && (name.startsWith("/") || name.contains("://"));
  }

  /**
   * Extracts partition column names from Spark Transform array.
   *
   * <p>Currently only supports identity transforms (PARTITIONED BY col1, col2, ...).
   * Other transforms (bucket, years, months, etc.) are not yet supported.
   *
   * @param partitions the partition transforms
   * @return list of partition column names
   */
  private List<String> extractPartitionColumns(Transform[] partitions) {
    if (partitions == null || partitions.length == 0) {
      return new ArrayList<>();
    }

    List<String> partitionColumns = new ArrayList<>();
    for (Transform transform : partitions) {
      if (transform instanceof IdentityTransform) {
        IdentityTransform identity = (IdentityTransform) transform;
        // Extract field reference name
        if (identity.reference() instanceof FieldReference) {
          FieldReference fieldRef = (FieldReference) identity.reference();
          String[] parts = fieldRef.fieldNames();
          partitionColumns.add(String.join(".", parts));
        }
      } else {
        throw new UnsupportedOperationException(
            "Partition transform not supported by Kernel: " + transform.describe());
      }
    }
    return partitionColumns;
  }

}
