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
package io.delta.kernel.internal.replay;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructType;

public class VersionStats {
  public static VersionStats fromColumnarBatch(long version, ColumnarBatch batch, int rowId) {
    // fromColumnVector already takes care of nulls
    Protocol protocol = Protocol.fromColumnVector(batch.getColumnVector(PROTOCOL_ORDINAL), rowId);
    Metadata metadata = Metadata.fromColumnVector(batch.getColumnVector(METADATA_ORDINAL), rowId);
    long tableSizeBytes = batch.getColumnVector(TABLE_SIZE_BYTES_ORDINAL).getLong(rowId);
    long numFiles = batch.getColumnVector(NUM_FILES_ORDINAL).getLong(rowId);
    return new VersionStats(version, metadata, protocol, tableSizeBytes, numFiles);
  }
  // We can add additional fields later
  public static final StructType FULL_SCHEMA =
      new StructType()
          .add("protocol", Protocol.FULL_SCHEMA)
          .add("metadata", Metadata.FULL_SCHEMA)
          .add("tableSizeBytes", LongType.LONG)
          .add("numFiles", LongType.LONG);
  private static final int PROTOCOL_ORDINAL = 0;
  private static final int METADATA_ORDINAL = 1;
  private static final int TABLE_SIZE_BYTES_ORDINAL = 2;
  private static final int NUM_FILES_ORDINAL = 3;
  private final long version;
  private final Metadata metadata;
  private final Protocol protocol;
  private final long tableSizeBytes;
  private final long numFiles;

  protected VersionStats(
      long version, Metadata metadata, Protocol protocol, long tableSizeBytes, long numFiles) {
    this.version = version;
    this.metadata = metadata;
    this.protocol = protocol;
    this.tableSizeBytes = tableSizeBytes;
    this.numFiles = numFiles;
  }
  /** The version of the Delta table that this VersionStats represents. */
  public long getVersion() {
    return version;
  }
  /** The {@link Metadata} stored in this VersionStats. May be null. */
  public Metadata getMetadata() {
    return metadata;
  }
  /** The {@link Protocol} stored in this VersionStats. May be null. */
  public Protocol getProtocol() {
    return protocol;
  }

  public long getTableSizeBytes() {
    return tableSizeBytes;
  }

  public long getNumFiles() {
    return numFiles;
  }
}
