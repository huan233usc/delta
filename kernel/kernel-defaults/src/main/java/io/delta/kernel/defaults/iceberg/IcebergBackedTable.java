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
import org.apache.iceberg.hadoop.HadoopTables;

public class IcebergBackedTable implements Table {

  private final String tablePath;
  private final org.apache.iceberg.Table icebergTable;

  public IcebergBackedTable(HadoopTables icebergCatalog, String tablePath) {
    this.tablePath = tablePath;
    this.icebergTable = icebergCatalog.load(tablePath);
  }

  @Override
  public String getPath(Engine engine) {
    return tablePath;
  }

  @Override
  public Snapshot getLatestSnapshot(Engine engine) throws TableNotFoundException {
    return new IcebergBackedSnapshot(icebergTable);
  }

  @Override
  public Snapshot getSnapshotAsOfVersion(Engine engine, long versionId)
      throws TableNotFoundException {
    return null;
  }

  @Override
  public Snapshot getSnapshotAsOfTimestamp(Engine engine, long millisSinceEpochUTC)
      throws TableNotFoundException {
    return null;
  }

  @Override
  public TransactionBuilder createTransactionBuilder(
      Engine engine, String engineInfo, Operation operation) {
    return null;
  }

  @Override
  public void checkpoint(Engine engine, long version)
      throws TableNotFoundException, CheckpointAlreadyExistsException, IOException {}

  @Override
  public void checksum(Engine engine, long version) throws TableNotFoundException, IOException {}
}
