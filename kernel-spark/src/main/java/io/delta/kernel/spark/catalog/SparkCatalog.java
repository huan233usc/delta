package io.delta.kernel.spark.catalog;

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.delta.DeltaTableUtils;
import scala.collection.JavaConverters;

public class SparkCatalog extends DelegatingCatalogExtension {

  @Override
  public Table loadTable(Identifier identifier) throws NoSuchTableException {
    Table delegated = super.loadTable(identifier);
    if (delegated instanceof V1Table
        && DeltaTableUtils.isDeltaTable(((V1Table) delegated).catalogTable())) {
      CatalogTable catalogTable = ((V1Table) delegated).catalogTable();
      return new SparkTable(
          identifier,
          catalogTable,
          JavaConverters.mapAsJavaMap(catalogTable.storage().properties()));
    }
    return delegated;
  }
}
