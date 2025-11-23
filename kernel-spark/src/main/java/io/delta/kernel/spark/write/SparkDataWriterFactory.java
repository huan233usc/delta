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

import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.spark.utils.SerializableKernelRowWrapper;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

/**
 * Factory for creating {@link SparkDataWriter} instances on executors.
 *
 * <p>This factory is serialized and sent to executors, where it creates DataWriter instances for
 * each partition/task.
 *
 * <p>Note: We use SerializableKernelRowWrapper to wrap the TransactionState Row for serialization,
 * since Kernel Row implementations (like TransactionStateRow) are not directly serializable. The
 * OutputWriterFactory is prepared on the driver and serialized to executors.
 */
public class SparkDataWriterFactory implements DataWriterFactory {
  private final SerializableKernelRowWrapper transactionStateWrapper;
  private final SerializableConfiguration hadoopConf;
  private final OutputWriterFactory writerFactory;
  private final StructType sparkSchema;
  private final String queryId;

  public SparkDataWriterFactory(
      SerializableKernelRowWrapper transactionStateWrapper,
      SerializableConfiguration hadoopConf,
      OutputWriterFactory writerFactory,
      StructType sparkSchema,
      String queryId) {
    this.transactionStateWrapper = transactionStateWrapper;
    this.hadoopConf = hadoopConf;
    this.writerFactory = writerFactory;
    this.sparkSchema = sparkSchema;
    this.queryId = queryId;
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    // Reconstruct Engine and TransactionState on executor
    Engine engine = DefaultEngine.create(hadoopConf.value());
    Row transactionState = transactionStateWrapper.getRow();
    return new SparkDataWriter(
        transactionState,
        engine,
        hadoopConf.value(),
        writerFactory,
        sparkSchema,
        queryId,
        partitionId,
        taskId);
  }
}
