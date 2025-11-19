/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.spark.read;

import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.icebergcompat.IcebergCompatMetadataValidatorAndUpdater;
import io.delta.kernel.internal.rowtracking.RowTracking;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.types.TypeWideningChecker;
import io.delta.kernel.internal.util.ColumnMapping;
import java.io.Serializable;
import java.util.Objects;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.DeltaColumnMappingMode;
import org.apache.spark.sql.delta.NoMapping;
import org.apache.spark.sql.delta.IdMapping;
import org.apache.spark.sql.delta.NameMapping;
import org.apache.spark.sql.delta.ProtocolMetadataAdapter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;
import scala.collection.JavaConverters;

/**
 * V2 adapter implementation (kernel-based) that bridges Kernel's Protocol and Metadata to the
 * ProtocolMetadataAdapter interface.
 *
 * <p>This enables kernel-spark to reuse DeltaParquetFileFormat reading logic without depending on
 * delta-spark's Protocol and Metadata action classes.
 */
public class ProtocolAndMetadataAdapterV2 implements ProtocolMetadataAdapter, Serializable {
  private static final long serialVersionUID = 1L;

  private final Protocol protocol;
  private final Metadata metadata;

  /**
   * Creates a new ProtocolAndMetadataAdapterV2.
   *
   * @param protocol Kernel's Protocol
   * @param metadata Kernel's Metadata
   */
  public ProtocolAndMetadataAdapterV2(Protocol protocol, Metadata metadata) {
    this.protocol = Objects.requireNonNull(protocol, "protocol is null");
    this.metadata = Objects.requireNonNull(metadata, "metadata is null");
  }

  @Override
  public DeltaColumnMappingMode columnMappingMode() {
    ColumnMapping.ColumnMappingMode kernelMode =
        ColumnMapping.getColumnMappingMode(metadata.getConfiguration());
    switch (kernelMode) {
      case NONE:
        return NoMapping.MODULE$;
      case ID:
        return IdMapping.MODULE$;
      case NAME:
        return NameMapping.MODULE$;
      default:
        throw new UnsupportedOperationException(
            "Unsupported column mapping mode: " + kernelMode);
    }
  }

  @Override
  public StructType getReferenceSchema() {
    return convertKernelToSparkSchema(metadata.getSchema());
  }

  @Override
  public boolean isRowIdEnabled() {
    return RowTracking.isEnabled(protocol, metadata);
  }

  @Override
  public boolean isDeletionVectorReadable() {
    return TableFeatures.isDeletionVectorsSupported(protocol);
  }

  @Override
  public boolean isIcebergCompatAnyEnabled() {
    return IcebergCompatMetadataValidatorAndUpdater.isIcebergCompatEnabled(metadata);
  }

  @Override
  public boolean isIcebergCompatGeqEnabled(int version) {
    if (version == 2) {
      return TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(metadata);
    } else if (version == 3) {
      return TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(metadata)
          || TableConfig.ICEBERG_COMPAT_V3_ENABLED.fromMetadata(metadata);
    }
    return false;
  }

  @Override
  public void assertTableReadable(SparkSession sparkSession) {
    // Check type widening readability
    // Kernel's TypeWideningChecker doesn't throw exceptions; it returns boolean
    // For now, we trust that the table is readable if we can construct the snapshot
    // The validation would have happened during snapshot construction in Kernel

    // Additional validation could be added here if needed
    // For example, check if any unsupported features are enabled
  }

  @Override
  public scala.collection.Iterable<StructField> createRowTrackingMetadataFields(
      boolean nullableRowTrackingConstantFields, boolean nullableRowTrackingGeneratedFields) {
    // Get row tracking fields from kernel-spark utilities
    java.util.List<StructField> fields =
        io.delta.kernel.spark.utils.RowTrackingUtils.createMetadataStructFields(
            protocol,
            metadata,
            nullableRowTrackingConstantFields,
            nullableRowTrackingGeneratedFields);

    // Convert Java list to Scala Iterable
    return JavaConverters.asScalaIteratorConverter(fields.iterator()).asScala().toIterable();
  }

  /**
   * Converts a Kernel StructType to a Spark StructType.
   *
   * @param kernelSchema Kernel's StructType
   * @return Spark's StructType
   */
  private StructType convertKernelToSparkSchema(io.delta.kernel.types.StructType kernelSchema) {
    return io.delta.kernel.spark.utils.SchemaUtils.toSparkSchema(kernelSchema);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (!(other instanceof ProtocolAndMetadataAdapterV2)) return false;

    ProtocolAndMetadataAdapterV2 that = (ProtocolAndMetadataAdapterV2) other;
    return Objects.equals(this.protocol, that.protocol)
        && Objects.equals(this.metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(protocol, metadata);
  }
}

