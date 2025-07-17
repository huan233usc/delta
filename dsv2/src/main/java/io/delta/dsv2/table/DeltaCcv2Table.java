package io.delta.dsv2.table;

import io.delta.dsv2.read.DeltaScanBuilder;
import io.delta.dsv2.utils.SchemaUtils;
import io.delta.kernel.ResolvedTable;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class DeltaCcv2Table implements Table, SupportsRead {

  private final ResolvedTable resolvedTable;
  private Engine kernelEngine;
  // Hack fields for credentials
  private final String accessKey;
  private final String secretKey;
  private final String sessionToken;
  private final String tableName;

  public DeltaCcv2Table(
      ResolvedTable resolvedTable,
      String tableName,
      Engine kernelEngine,
      String accessKey,
      String secretKey,
      String sessionToken) {
    this.resolvedTable = resolvedTable;
    this.kernelEngine = kernelEngine;
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.sessionToken = sessionToken;
    this.tableName = tableName;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new DeltaScanBuilder(resolvedTable, kernelEngine, accessKey, secretKey, sessionToken);
  }

  @Override
  public String name() {
    return tableName + "kernel";
  }

  // TODO: this is deprecated product.
  @Override
  public StructType schema() {
    return SchemaUtils.convertKernelSchemaToSparkSchema(resolvedTable.getSchema());
  }

  @Override
  public Set<TableCapability> capabilities() {
    Set<TableCapability> capabilities = new HashSet<>();
    capabilities.add(TableCapability.BATCH_READ);
    capabilities.add(TableCapability.MICRO_BATCH_READ);
    return capabilities;
  }

  @Override
  public Transform[] partitioning() {
    try {
      List<String> partColNames =
          resolvedTable.getPartitionColumns().stream()
              .map(column -> String.join(".", column.getNames()))
              .collect(Collectors.toList());

      Transform[] transforms = new Transform[partColNames.size()];
      for (int i = 0; i < partColNames.size(); i++) {
        transforms[i] = Expressions.identity(partColNames.get(i));
      }
      return transforms;
    } catch (TableNotFoundException e) {
      return new Transform[0];
    }
  }
}
