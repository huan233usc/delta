package io.delta.kernel.spark.catalog.utils;

import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import scala.collection.JavaConverters;

public class DeltaTableManagerFactory {

    /**
     * Creates the appropriate DeltaTableManager based on the catalog table configuration.
     * 
     * @param catalogTable the catalog table to create a manager for
     * @return UnityCatalogTableManager for Unity Catalog tables, PathBasedTableManager otherwise
     */
    public static DeltaTableManager create(CatalogTable catalogTable) {
        return isUnityCatalogTable(catalogTable) 
            ? UnityCatalogTableManager.create(catalogTable)
            : createPathBasedManager(catalogTable);
    }

    /**
     * Determines if the given catalog table is a Unity Catalog table.
     * 
     * @param catalogTable the catalog table to check
     * @return true if this is a Unity Catalog table, false otherwise
     */
    private static boolean isUnityCatalogTable(CatalogTable catalogTable) {
        return catalogTable.storage().properties()
            .get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY)
            .exists(ucId -> TableFeatures.isPropertiesManuallySupportingTableFeature(
                JavaConverters.mapAsJavaMap(catalogTable.properties()), 
                TableFeatures.CATALOG_MANAGED_R_W_FEATURE_PREVIEW));
    }
    
    /**
     * Creates a path-based table manager for non-Unity Catalog tables.
     * 
     * @param catalogTable the catalog table
     * @return a new PathBasedTableManager instance
     */
    private static PathBasedTableManager createPathBasedManager(CatalogTable catalogTable) {
        return new PathBasedTableManager(
            catalogTable.location().getPath(), 
            SparkSession.active().sessionState().newHadoopConf());
    }
}
