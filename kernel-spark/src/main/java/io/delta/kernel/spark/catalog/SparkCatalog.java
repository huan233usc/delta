package io.delta.kernel.spark.catalog;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.delta.DeltaTableIdentifier;
import org.apache.spark.sql.delta.DeltaTableUtils;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;

public class SparkCatalog extends DeltaCatalog {

  @Override
  public Table loadTable(Identifier identifier) {
    try {
      // Load table from delegate catalog directly
      Table delegateTable = ((TableCatalog) delegate).loadTable(identifier);

      // If delegate table is a V1Table and it's a Delta table, return SparkTable
      if (delegateTable instanceof V1Table) {
        V1Table v1Table = (V1Table) delegateTable;
        if (DeltaTableUtils.isDeltaTable(v1Table.catalogTable())) {
          return new SparkTable(identifier, v1Table.catalogTable());
        }
      }
      // Otherwise return the delegate table as-is
      return delegateTable;
    } catch (AnalysisException e) {
      // Handle NoSuchTableException and its related exceptions
      if (e instanceof NoSuchTableException
          || e instanceof NoSuchNamespaceException
          || e instanceof NoSuchDatabaseException) {
        if (isPathIdentifier(identifier)) {
          return newDeltaPathTable(identifier);
        } else if (isIcebergPathIdentifier(identifier)) {
          return newIcebergPathTable(identifier);
        }
      } else if (DeltaTableIdentifier.gluePermissionError(e) && isPathIdentifier(identifier)) {
        // Handle Glue permission errors for path identifiers
        return newDeltaPathTable(identifier);
      }
      // Rethrow as RuntimeException since AnalysisException is checked
      throw new RuntimeException(e);
    }
  }
}
