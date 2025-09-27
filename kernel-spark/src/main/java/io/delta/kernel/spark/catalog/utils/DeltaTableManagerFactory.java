package io.delta.kernel.spark.catalog.utils;

import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.util.Utils;
import scala.collection.JavaConverters;

/**
 * Factory for creating DeltaTableManager instances. Enhanced version supporting dynamic class
 * loading and custom implementations, following Iceberg's proven factory pattern.
 */
public class DeltaTableManagerFactory {

  // Built-in type mappings
  public static final String DELTA_TABLE_MANAGER_TYPE = "delta.table-manager.type";
  public static final String DELTA_TABLE_MANAGER_IMPL = "delta.table-manager.impl";

  public static final String UNITY_TYPE = "unity";
  public static final String PATH_TYPE = "path";

  // UC implementation in delta-unity module (will be moved there)
  private static final String UNITY_IMPL = "io.delta.unity.UnityCatalogTableManager";
  // Path implementation in delta-kernel-spark
  private static final String PATH_IMPL =
      "io.delta.kernel.spark.catalog.utils.PathBasedTableManager";

  /**
   * Creates the appropriate DeltaTableManager based on the catalog table configuration. This is the
   * main entry point for compatibility with existing code.
   *
   * @param catalogTable the catalog table to create a manager for
   * @return UnityCatalogTableManager for Unity Catalog tables, PathBasedTableManager otherwise
   */
  public static DeltaTableManager create(CatalogTable catalogTable) {
    Map<String, String> properties = extractPropertiesFromCatalogTable(catalogTable);

    // Check for custom implementation first
    String managerImpl = properties.get(DELTA_TABLE_MANAGER_IMPL);
    if (managerImpl == null) {
      // Fall back to built-in type detection
      String managerType = detectManagerType(catalogTable, properties);
      managerImpl = getImplForType(managerType);
    }

    return loadTableManager(managerImpl, catalogTable, properties);
  }

  /** Extract properties from CatalogTable for use in table manager configuration. */
  private static Map<String, String> extractPropertiesFromCatalogTable(CatalogTable catalogTable) {
    Map<String, String> properties = new HashMap<>();

    // Add catalog table properties
    properties.putAll(JavaConverters.mapAsJavaMap(catalogTable.properties()));

    // Add storage properties
    properties.putAll(JavaConverters.mapAsJavaMap(catalogTable.storage().properties()));

    // Add Spark configuration relevant to table managers
    SparkSession spark = SparkSession.active();
    properties.put("table.path", catalogTable.location().toString());

    // Extract catalog-specific configurations from Spark conf
    // This enables properties-based configuration as described in the design doc
    scala.collection.Map<String, String> sparkConf = spark.conf().getAll();
    JavaConverters.mapAsJavaMap(sparkConf).entrySet().stream()
        .filter(
            entry ->
                entry.getKey().startsWith("spark.sql.catalog.")
                    || entry.getKey().startsWith("delta.table-manager."))
        .forEach(entry -> properties.put(entry.getKey(), entry.getValue()));

    return properties;
  }

  /** Detect table manager type: UC, Path-based, or custom. */
  private static String detectManagerType(
      CatalogTable catalogTable, Map<String, String> properties) {
    // Explicit type override
    String explicitType = properties.get(DELTA_TABLE_MANAGER_TYPE);
    if (explicitType != null) {
      return explicitType;
    }

    // Auto-detection: UC vs Path-based
    if (isUnityCatalogTable(catalogTable)) {
      return UNITY_TYPE;
    } else {
      return PATH_TYPE;
    }
  }

  /** Get implementation class name for a given type. */
  private static String getImplForType(String managerType) {
    switch (managerType.toLowerCase()) {
      case UNITY_TYPE:
        return UNITY_IMPL;
      case PATH_TYPE:
        return PATH_IMPL;
      default:
        throw new UnsupportedOperationException(
            "Unknown table manager type: "
                + managerType
                + ". Supported types: unity, path. "
                + "For custom implementations, use "
                + DELTA_TABLE_MANAGER_IMPL
                + " property.");
    }
  }

  /**
   * Load table manager using dynamic class loading. Supports both built-in implementations and
   * custom ones.
   */
  public static DeltaTableManager loadTableManager(
      String impl, CatalogTable catalogTable, Map<String, String> properties) {

    if (impl == null) {
      throw new IllegalArgumentException(
          "Cannot initialize DeltaTableManager, impl class name is null");
    }

    try {
      // Use Spark's Utils.classForName for consistent class loading
      Class<?> clazz = Utils.classForName(impl);

      // Verify the class implements DeltaTableManager
      if (!DeltaTableManager.class.isAssignableFrom(clazz)) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot initialize DeltaTableManager, %s does not implement DeltaTableManager.",
                impl));
      }

      // Get no-arg constructor
      Constructor<?> ctor = clazz.getDeclaredConstructor();
      ctor.setAccessible(true);

      // Create instance
      DeltaTableManager manager = (DeltaTableManager) ctor.newInstance();

      // Initialize with table and properties
      manager.initialize(catalogTable.location().toString(), properties);
      return manager;

    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          String.format("Cannot find DeltaTableManager implementation: %s", impl), e);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format("DeltaTableManager implementation %s must have a no-arg constructor", impl),
          e);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to initialize DeltaTableManager %s: %s", impl, e.getMessage()), e);
    }
  }

  /**
   * Determines if the given catalog table is a Unity Catalog table.
   *
   * @param catalogTable the catalog table to check
   * @return true if this is a Unity Catalog table, false otherwise
   */
  private static boolean isUnityCatalogTable(CatalogTable catalogTable) {
    return catalogTable
        .storage()
        .properties()
        .get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY)
        .exists(
            ucId ->
                TableFeatures.isPropertiesManuallySupportingTableFeature(
                    JavaConverters.mapAsJavaMap(catalogTable.properties()),
                    TableFeatures.CATALOG_MANAGED_R_W_FEATURE_PREVIEW));
  }
}
