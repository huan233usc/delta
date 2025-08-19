package io.delta.spark.dsv2.scan.batch;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/**
 * Factory for creating KernelSparkPartitionReader instances. This factory can be reused across
 * multiple partitions.
 */
public class KernelSparkPartitionReaderFactory implements PartitionReaderFactory {

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    if (!(partition instanceof KernelSparkInputPartition)) {
      throw new IllegalArgumentException(
          "Expected KernelSparkInputPartition, got: " + partition.getClass().getSimpleName());
    }

    KernelSparkInputPartition kernelPartition = (KernelSparkInputPartition) partition;
    return new KernelSparkPartitionReader(
        kernelPartition.getSerializedScanState(), kernelPartition.getSerializedScanFileRow());
  }
}
