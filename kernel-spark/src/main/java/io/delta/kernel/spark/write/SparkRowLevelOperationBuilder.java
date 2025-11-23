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
package io.delta.kernel.spark.write;

import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.spark.table.SparkTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;

/**
 * Builder for creating {@link RowLevelOperation} for Delta tables. This supports UPDATE, DELETE,
 * and MERGE operations via DSv2.
 */
public class SparkRowLevelOperationBuilder implements RowLevelOperationBuilder {

  private final SparkTable table;
  private final Snapshot snapshot;
  private final Engine engine;
  private final Configuration hadoopConf;
  private final RowLevelOperationInfo info;

  public SparkRowLevelOperationBuilder(
      SparkTable table,
      Snapshot snapshot,
      Engine engine,
      Configuration hadoopConf,
      RowLevelOperationInfo info) {
    this.table = table;
    this.snapshot = snapshot;
    this.engine = engine;
    this.hadoopConf = hadoopConf;
    this.info = info;
  }

  @Override
  public RowLevelOperation build() {
    return new SparkRowLevelOperation(table, snapshot, engine, hadoopConf, info);
  }
}
