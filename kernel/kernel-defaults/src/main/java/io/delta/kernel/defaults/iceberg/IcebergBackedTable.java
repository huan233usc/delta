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

import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.CheckpointAlreadyExistsException;
import io.delta.kernel.exceptions.TableNotFoundException;
import java.io.IOException;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.util.SnapshotUtil;

public class IcebergBackedTable implements Table {

  private final org.apache.iceberg.Table icebergTable;

  public IcebergBackedTable(org.apache.iceberg.Table icebergTable) {
    this.icebergTable = icebergTable;
  }

  @Override
  public String getPath(Engine engine) {
    return icebergTable.location();
  }

  @Override
  public Snapshot getLatestSnapshot(Engine engine) throws TableNotFoundException {
    TableMetadata tableMetadata = ((BaseTable) icebergTable).operations().current();
    org.apache.iceberg.Snapshot currentSnapshot = tableMetadata.currentSnapshot();

    if (currentSnapshot == null) {
      throw new TableNotFoundException("No current snapshot found in table");
    }

    return new IcebergBackedSnapshot(tableMetadata, icebergTable.io());
  }

  @Override
  public Snapshot getSnapshotAsOfVersion(Engine engine, long versionId)
      throws TableNotFoundException {
    // Map Delta version to Iceberg sequence number
    long targetSequenceNumber = versionId + 1;

    // Find the snapshot with the target sequence number
    org.apache.iceberg.Snapshot targetSnapshot = null;
    for (org.apache.iceberg.Snapshot snapshot : icebergTable.snapshots()) {
      if (snapshot.sequenceNumber() == targetSequenceNumber) {
        targetSnapshot = snapshot;
        break;
      }
    }

    if (targetSnapshot == null) {
      throw new TableNotFoundException("Snapshot with version " + versionId + " not found");
    }

    // Build new TableMetadata with the target snapshot as current
    TableMetadata baseMetadata = ((BaseTable) icebergTable).operations().current();
    TableMetadata snapshotMetadata =
        TableMetadata.buildFrom(baseMetadata)
            .setBranchSnapshot(targetSnapshot.snapshotId(), SnapshotRef.MAIN_BRANCH)
            .build();

    return new IcebergBackedSnapshot(snapshotMetadata, icebergTable.io());
  }

  @Override
  public Snapshot getSnapshotAsOfTimestamp(Engine engine, long millisSinceEpochUTC)
      throws TableNotFoundException {
    try {
      // Use SnapshotUtil to find the snapshot ID as of the given timestamp
      Long snapshotId = SnapshotUtil.nullableSnapshotIdAsOfTime(icebergTable, millisSinceEpochUTC);

      if (snapshotId == null) {
        throw new TableNotFoundException(
            "No snapshot found at or before timestamp " + millisSinceEpochUTC);
      }

      // Verify the snapshot exists
      org.apache.iceberg.Snapshot targetSnapshot = icebergTable.snapshot(snapshotId);
      if (targetSnapshot == null) {
        throw new TableNotFoundException("Snapshot with ID " + snapshotId + " not found");
      }

      // Build new TableMetadata with the target snapshot as current
      TableMetadata baseMetadata = ((BaseTable) icebergTable).operations().current();
      TableMetadata snapshotMetadata =
          TableMetadata.buildFrom(baseMetadata)
              .setBranchSnapshot(snapshotId, SnapshotRef.MAIN_BRANCH)
              .build();

      return new IcebergBackedSnapshot(snapshotMetadata, icebergTable.io());

    } catch (IllegalArgumentException e) {
      throw new TableNotFoundException(
          "No snapshot found at or before timestamp " + millisSinceEpochUTC);
    }
  }

  @Override
  public TransactionBuilder createTransactionBuilder(
      Engine engine, String engineInfo, Operation operation) {
    // TODO:
    return new IcebergBackedTransactionBuilder(icebergTable, engineInfo, operation);
  }

  @Override
  public void checkpoint(Engine engine, long version)
      throws TableNotFoundException, CheckpointAlreadyExistsException, IOException {
    throw new UnsupportedOperationException(
        "Checkpoint operations are not supported for Iceberg-backed tables");
  }

  @Override
  public void checksum(Engine engine, long version) throws TableNotFoundException, IOException {
    throw new UnsupportedOperationException(
        "Checksum operations are not supported for Iceberg-backed tables");
  }
}
