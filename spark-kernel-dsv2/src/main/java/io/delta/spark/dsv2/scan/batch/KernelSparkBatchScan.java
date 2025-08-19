package io.delta.spark.dsv2.scan.batch;

import static java.util.Objects.requireNonNull;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/**
 * Spark Batch implementation backed by Delta Kernel Scan. Creates InputPartitions from Kernel scan
 * files and provides a PartitionReaderFactory.
 */
public class KernelSparkBatchScan implements Batch {

  private final KernelSparkScanContext sharedContext;

  public KernelSparkBatchScan(KernelSparkScanContext sharedContext) {
    this.sharedContext = requireNonNull(sharedContext, "sharedContext is null");
  }

  @Override
  public InputPartition[] planInputPartitions() {
    return sharedContext.planPartitions();
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new KernelSparkPartitionReaderFactory();
  }
}
