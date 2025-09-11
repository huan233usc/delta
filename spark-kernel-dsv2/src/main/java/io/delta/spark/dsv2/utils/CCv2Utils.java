package io.delta.spark.dsv2.utils;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.spark.dsv2.table.SparkTableWithV1ConnectorFallback;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import io.delta.unity.UCCatalogManagedClient;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import scala.Option;
import scala.collection.JavaConverters;

/** Utilities to convert a DeltaTableV2 into a Kernel-backed V2 Table when possible. */
public class CCv2Utils {

  /** Prefix for Spark SQL catalog configurations. */
  private static final String SPARK_SQL_CATALOG_PREFIX = "spark.sql.catalog.";

  /** Connector class name for filtering relevant Unity Catalog catalogs. */
  private static final String UNITY_CATALOG_CONNECTOR_CLASS =
      "io.unitycatalog.spark.UCSingleCatalog";

  /** Suffix for the URI configuration of a catalog. */
  private static final String URI_SUFFIX = "uri";

  /** Suffix for the token configuration of a catalog. */
  private static final String TOKEN_SUFFIX = "token";

  public static Table convertToV2Connector(DeltaTableV2 table) {
    Option<CatalogTable> catalogTable = table.catalogTable();
    if (!catalogTable.isDefined()) {
      return table;
    }

    // Only handle Unity Catalog managed tables for now
    Option<String> ucTableId =
        catalogTable.get().storage().properties().get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
    if (ucTableId.isEmpty()) {
      return table;
    }

    try {
      // Build UC client from Spark conf
      String catalogName = getCurrentCatalogName();
      Optional<UCClient> ucClient = getUCClientForCatalog(catalogName);
      if (!ucClient.isPresent()) {
        return table;
      }

      // Base Hadoop conf and enrich with table storage properties
      Configuration baseHadoopConf = SparkSession.active().sessionState().newHadoopConf();
      java.util.Map<String, String> storageProps =
          JavaConverters.mapAsJavaMapConverter(catalogTable.get().storage().properties()).asJava();
      for (Map.Entry<String, String> e : storageProps.entrySet()) {
        baseHadoopConf.set(e.getKey(), e.getValue());
      }

      // Load snapshot through UC-managed client using Kernel DefaultEngine
      UCCatalogManagedClient ucCatalogManagedClient = new UCCatalogManagedClient(ucClient.get());
      SnapshotImpl snapshot =
          (SnapshotImpl)
              ucCatalogManagedClient.loadSnapshot(
                  DefaultEngine.create(baseHadoopConf),
                  ucTableId.get(),
                  catalogTable.get().location().toString(),
                  Optional.empty());

      // Build Identifier from table identifier
      Identifier ident = toIdentifier(table.getTableIdentifierIfExists());

      // Return Kernel-backed Spark V2 table with a V1 fallback
      return new SparkTableWithV1ConnectorFallback(ident, snapshot, baseHadoopConf, table);
    } catch (Exception e) {
      // In case of any failure, gracefully fall back to the original table
      System.err.println("convertToV2Connector failed: " + e.getMessage());
      return table;
    }
  }

  private static Identifier toIdentifier(Option<TableIdentifier> tableIdOpt) {
    if (tableIdOpt.isEmpty()) {
      // Fallback to a synthetic name when identifier is missing
      return Identifier.of(new String[] {}, "delta_kernel_table");
    }
    TableIdentifier id = tableIdOpt.get();
    String[] namespace =
        id.database().isDefined() ? new String[] {id.database().get()} : new String[] {};
    return Identifier.of(namespace, id.table());
  }

  /**
   * Gets the current catalog name by checking SparkSession configurations. It looks for Unity
   * Catalog configurations and returns the first matching catalog name.
   */
  private static String getCurrentCatalogName() {
    Map<String, String> allConfigs =
        JavaConverters.mapAsJavaMapConverter(SparkSession.active().conf().getAll()).asJava();

    // Look for configured Unity Catalog catalogs
    for (Map.Entry<String, String> entry : allConfigs.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();

      // Check if this is a catalog configuration for Unity Catalog
      if (key.startsWith(SPARK_SQL_CATALOG_PREFIX)
          && key.substring(SPARK_SQL_CATALOG_PREFIX.length()).indexOf('.') == -1
          && UNITY_CATALOG_CONNECTOR_CLASS.equals(value)) {

        // Extract catalog name from the key
        String catalogName = key.substring(SPARK_SQL_CATALOG_PREFIX.length());

        // Verify this catalog has both URI and token configured
        String uriKey = SPARK_SQL_CATALOG_PREFIX + catalogName + "." + URI_SUFFIX;
        String tokenKey = SPARK_SQL_CATALOG_PREFIX + catalogName + "." + TOKEN_SUFFIX;

        if (allConfigs.containsKey(uriKey) && allConfigs.containsKey(tokenKey)) {
          return catalogName;
        }
      }
    }

    // If no Unity Catalog is found, default to spark_catalog
    return "spark_catalog";
  }

  /**
   * Creates a UCClient for the specified catalog name following the pattern from
   * UCCommitCoordinatorBuilder.scala's buildForCatalog method.
   */
  private static Optional<UCClient> getUCClientForCatalog(String catalogName) {
    Map<String, String> allConfigs =
        JavaConverters.mapAsJavaMapConverter(SparkSession.active().conf().getAll()).asJava();

    // Find the catalog configuration
    String uriKey = SPARK_SQL_CATALOG_PREFIX + catalogName + "." + URI_SUFFIX;
    String tokenKey = SPARK_SQL_CATALOG_PREFIX + catalogName + "." + TOKEN_SUFFIX;
    String connectorKey = SPARK_SQL_CATALOG_PREFIX + catalogName;

    String connector = allConfigs.get(connectorKey);
    String uri = allConfigs.get(uriKey);
    String token = allConfigs.get(tokenKey);

    // Validate that this is a Unity Catalog connector
    if (connector == null || !UNITY_CATALOG_CONNECTOR_CLASS.equals(connector)) {
      return Optional.empty();
    }

    // Validate that both uri and token are present
    if (uri == null || token == null) {
      throw new IllegalArgumentException(
          String.format("Catalog %s does not have both uri and token configured", catalogName));
    }

    // Validate URI format
    try {
      new URI(uri);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format("Catalog %s has invalid URI: %s", catalogName, uri));
    }

    // Create UCClient using the same factory pattern as UCCommitCoordinatorBuilder
    return Optional.of(new UCTokenBasedRestClient(uri, token));
  }
}
