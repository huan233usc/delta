package io.delta.spark.dsv2;

import io.delta.spark.dsv2.utils.CCv2Utils;
import java.util.Set;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableWritePrivilege;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;

public class SparkCatalog extends DeltaCatalog {
  @Override
  public Table loadTable(Identifier identifier) {
    Table deltaTable = super.loadTable(identifier);
    if (deltaTable instanceof DeltaTableV2) {
      return CCv2Utils.convertToV2Connector((DeltaTableV2) deltaTable);
    }
    return deltaTable;
  }

  @Override
  public Table loadTable(Identifier ident, Set<TableWritePrivilege> writePrivileges)
      throws NoSuchTableException {
    return super.loadTable(ident, writePrivileges);
  }
}
