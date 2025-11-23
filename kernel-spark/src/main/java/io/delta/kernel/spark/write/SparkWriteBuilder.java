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

import io.delta.kernel.Transaction;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.spark.read.SparkScan;
import io.delta.kernel.transaction.UpdateTableTransactionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

/**
 * Implementation of Spark's {@link WriteBuilder} for Delta tables using Kernel.
 *
 * <p>This class wraps Kernel's UpdateTableTransactionBuilder and creates the DSv2 Write instance.
 */
public class SparkWriteBuilder implements WriteBuilder {
  private final UpdateTableTransactionBuilder txnBuilder;
  private final Configuration hadoopConf;
  private final StructType sparkSchema;
  private final String queryId;
  private final SparkScan scanForCOW;
  private final Engine engine;

  public SparkWriteBuilder(
      UpdateTableTransactionBuilder txnBuilder,
      Configuration hadoopConf,
      StructType sparkSchema,
      String queryId) {
    this(txnBuilder, hadoopConf, sparkSchema, queryId, null, null);
  }

  public SparkWriteBuilder(
      UpdateTableTransactionBuilder txnBuilder,
      Configuration hadoopConf,
      StructType sparkSchema,
      String queryId,
      SparkScan scanForCOW,
      Engine engine) {
    this.txnBuilder = txnBuilder;
    this.hadoopConf = hadoopConf;
    this.sparkSchema = sparkSchema;
    this.queryId = queryId;
    this.scanForCOW = scanForCOW;
    this.engine = engine;
  }

  @Override
  public Write build() {
    return new SparkWrite(txnBuilder, hadoopConf, sparkSchema, queryId, scanForCOW, engine);
  }

  /** Internal Write implementation that creates the actual BatchWrite. */
  private static class SparkWrite implements Write {
    private final UpdateTableTransactionBuilder txnBuilder;
    private final Configuration hadoopConf;
    private final StructType sparkSchema;
    private final String queryId;
    private final SparkScan scanForCOW;
    private final Engine engineForCOW;

    public SparkWrite(
        UpdateTableTransactionBuilder txnBuilder,
        Configuration hadoopConf,
        StructType sparkSchema,
        String queryId,
        SparkScan scanForCOW,
        Engine engineForCOW) {
      this.txnBuilder = txnBuilder;
      this.hadoopConf = hadoopConf;
      this.sparkSchema = sparkSchema;
      this.queryId = queryId;
      this.scanForCOW = scanForCOW;
      this.engineForCOW = engineForCOW;
    }

    @Override
    public BatchWrite toBatch() {
      // Build Engine on driver for transaction building (reuse existing engine if available)
      Engine engine = engineForCOW != null ? engineForCOW : DefaultEngine.create(hadoopConf);
      // Build the actual transaction from the builder
      Transaction transaction = txnBuilder.build(engine);
      // Wrap hadoopConf in SerializableConfiguration for serialization to executors
      SerializableConfiguration serializableConf = new SerializableConfiguration(hadoopConf);
      return new SparkBatchWrite(
          transaction, engine, serializableConf, sparkSchema, queryId, scanForCOW);
    }
  }
}
