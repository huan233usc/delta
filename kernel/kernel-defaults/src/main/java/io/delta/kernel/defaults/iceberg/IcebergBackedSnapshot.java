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
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;

public class IcebergBackedSnapshot implements Snapshot {

  private final TableMetadata tableMetadata;
  private final FileIO icebergIo;

  public IcebergBackedSnapshot(TableMetadata tableMetadata, FileIO icebergIo) {
    this.tableMetadata = tableMetadata;
    this.icebergIo = icebergIo;
  }

  @Override
  public long getVersion() {
    return tableMetadata.currentSnapshot().sequenceNumber() - 1;
  }

  @Override
  public List<String> getPartitionColumnNames() {
    return tableMetadata.spec().fields().stream()
        .map(PartitionField::name)
        .collect(Collectors.toList());
  }

  @Override
  public long getTimestamp(Engine engine) {
    return tableMetadata.currentSnapshot().timestampMillis();
  }

  @Override
  public StructType getSchema() {
    return IcebergToDeltaSchemaConverter.toKernelSchema(tableMetadata.schema());
  }

  @Override
  public Optional<String> getDomainMetadata(String domain) {
    return Optional.empty();
  }

  @Override
  public ScanBuilder getScanBuilder() {
    return new IcebergScanBuilder(tableMetadata, tableMetadata.currentSnapshot(), icebergIo);
  }

  /** ScanBuilder implementation for Iceberg tables */
  private static class IcebergScanBuilder implements ScanBuilder {

    private final org.apache.iceberg.TableMetadata tableMetadata;
    private final org.apache.iceberg.Snapshot icebergSnapshot;
    private final FileIO icebergIo;
    private Optional<Predicate> filter = Optional.empty();
    private Optional<StructType> readSchema = Optional.empty();

    IcebergScanBuilder(
        org.apache.iceberg.TableMetadata tableMetadata,
        org.apache.iceberg.Snapshot icebergSnapshot,
        FileIO icebergIo) {
      this.tableMetadata = tableMetadata;
      this.icebergSnapshot = icebergSnapshot;
      this.icebergIo = icebergIo;
    }

    @Override
    public ScanBuilder withFilter(Predicate predicate) {
      IcebergScanBuilder newBuilder =
          new IcebergScanBuilder(tableMetadata, icebergSnapshot, icebergIo);
      newBuilder.filter = Optional.of(predicate);
      newBuilder.readSchema = this.readSchema;
      return newBuilder;
    }

    @Override
    public ScanBuilder withReadSchema(StructType readSchema) {
      IcebergScanBuilder newBuilder =
          new IcebergScanBuilder(tableMetadata, icebergSnapshot, icebergIo);
      newBuilder.readSchema = Optional.of(readSchema);
      newBuilder.filter = this.filter;
      return newBuilder;
    }

    @Override
    public Scan build() {
      StructType effectiveReadSchema =
          readSchema.orElse(IcebergToDeltaSchemaConverter.toKernelSchema(tableMetadata.schema()));
      return new IcebergBackedScan(icebergIo, tableMetadata, icebergSnapshot, effectiveReadSchema) {
        // Override to handle the filter if provided
        @Override
        public Optional<io.delta.kernel.expressions.Predicate> getRemainingFilter() {
          // For now, return the original filter as remaining filter
          return filter;
        }
      };
    }
  }
}
