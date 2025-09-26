package io.delta.kernel.spark.catalog.utils;

import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import scala.Option;
import scala.collection.JavaConverters;

import java.util.Objects;

public class DeltaTableManagerFactory {

    enum CatalogType {
        UNITY,
        PATH_BASED
    }

    public static DeltaTableManager create(CatalogTable catalogTable) {
        CatalogType catalogType = getCatalogType(catalogTable);
        if (Objects.requireNonNull(catalogType) == CatalogType.UNITY) {
            return UnityCatalogTableManager.create(catalogTable);
        }
        return new PathBasedTableManager(catalogTable.location().getPath(), SparkSession.active().sessionState().newHadoopConf());

    }

    private static CatalogType getCatalogType(CatalogTable catalogTable) {
        if(isUnityCatalogTable(catalogTable)){
            return CatalogType.UNITY;
        }
        return CatalogType.PATH_BASED;
    }

    private static boolean isUnityCatalogTable(CatalogTable catalogTable) {
        Option<String> ucId = catalogTable.storage().properties().get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
        if(ucId.isEmpty()) {
            return false;
        }
        return TableFeatures.isPropertiesManuallySupportingTableFeature(JavaConverters.mapAsJavaMap(catalogTable.properties()), TableFeatures.CATALOG_MANAGED_R_W_FEATURE_PREVIEW);
    }
}
