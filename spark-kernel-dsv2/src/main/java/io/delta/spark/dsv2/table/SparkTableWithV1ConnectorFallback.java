package io.delta.spark.dsv2.table;

import io.delta.kernel.internal.SnapshotImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;

public class SparkTableWithV1ConnectorFallback extends SparkTable {

  private final Table fallback;

  public SparkTableWithV1ConnectorFallback(
      Identifier identifier, SnapshotImpl snapshot, Configuration hadoopConf, Table v1Fallback) {
    super(identifier, snapshot, hadoopConf);
    this.fallback = v1Fallback;
  }

  public Table getFallback() {
    return fallback;
  }
}
