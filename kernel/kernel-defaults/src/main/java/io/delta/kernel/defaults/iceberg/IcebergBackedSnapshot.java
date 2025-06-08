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
package io.delta.kernel.defaults.iceberg;

import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionField;

public class IcebergBackedSnapshot implements Snapshot {

  private final org.apache.iceberg.Table icebergTable;

  public IcebergBackedSnapshot(org.apache.iceberg.Table icebergTable) {
    this.icebergTable = icebergTable;
  }

  @Override
  public long getVersion() {
    BaseTable baseTable = (BaseTable) icebergTable;
    return baseTable.operations().current().lastSequenceNumber() - 1;
  }

  @Override
  public List<String> getPartitionColumnNames() {
    return icebergTable.spec().fields().stream()
        .map(PartitionField::name)
        .collect(Collectors.toList());
  }

  @Override
  public long getTimestamp(Engine engine) {
    return icebergTable.currentSnapshot().timestampMillis();
  }

  @Override
  public StructType getSchema() {
    return IcebergToDeltaSchemaConverter.toKernelSchema(icebergTable.schema());
  }

  @Override
  public Optional<String> getDomainMetadata(String domain) {
    return Optional.empty();
  }

  @Override
  public ScanBuilder getScanBuilder() {
    return new IcebergScanBuilder(icebergTable, icebergTable.currentSnapshot());
  }

  /** ScanBuilder implementation for Iceberg tables */
  private static class IcebergScanBuilder implements ScanBuilder {

    private final org.apache.iceberg.Table icebergTable;
    private final org.apache.iceberg.Snapshot icebergSnapshot;
    private Optional<Predicate> filter = Optional.empty();
    private Optional<StructType> readSchema = Optional.empty();

    IcebergScanBuilder(
        org.apache.iceberg.Table icebergTable, org.apache.iceberg.Snapshot icebergSnapshot) {
      this.icebergTable = icebergTable;
      this.icebergSnapshot = icebergSnapshot;
    }

    @Override
    public ScanBuilder withFilter(Predicate predicate) {
      IcebergScanBuilder newBuilder = new IcebergScanBuilder(icebergTable, icebergSnapshot);
      newBuilder.filter = Optional.of(predicate);
      newBuilder.readSchema = this.readSchema; // Copy existing read schema
      return newBuilder;
    }

    @Override
    public ScanBuilder withReadSchema(StructType readSchema) {
      IcebergScanBuilder newBuilder = new IcebergScanBuilder(icebergTable, icebergSnapshot);
      newBuilder.readSchema = Optional.of(readSchema);
      newBuilder.filter = this.filter; // Copy existing filter
      return newBuilder;
    }

    @Override
    public Scan build() {
      // Use read schema if provided, otherwise use the full table schema
      StructType effectiveReadSchema =
          readSchema.orElse(IcebergToDeltaSchemaConverter.toKernelSchema(icebergTable.schema()));

      // For now, we'll create a simple engine placeholder - in real usage this would be passed in
      // or we'd modify the build method signature to accept an Engine parameter
      return new IcebergBackedScan(icebergTable, icebergSnapshot, effectiveReadSchema) {
        // Override to handle the filter if provided
        @Override
        public Optional<io.delta.kernel.expressions.Predicate> getRemainingFilter() {
          // For now, return the original filter as remaining filter
          // In a full implementation, you'd apply what you can at the Iceberg level
          // and return only what couldn't be pushed down
          return filter;
        }
      };
    }
  }
}
