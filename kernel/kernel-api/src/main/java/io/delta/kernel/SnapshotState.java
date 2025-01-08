/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SnapshotState {
  // TODO have a seperate class as some fields are not belonging to the snapshot
  public static StructType CRC_FILE_SCHEMA =
      new StructType()
          .add("tableSizeBytes", LongType.LONG)
          .add("numFiles", LongType.LONG)
          .add("numMetadata", LongType.LONG)
          .add("numProtocol", LongType.LONG)
          .add("metadata", Metadata.FULL_SCHEMA)
          .add("protocol", Protocol.FULL_SCHEMA)
          .add("txnId", StringType.STRING);

  private final Metadata metadata;
  private final Protocol protocol;
  private final long tableSizeBytes;
  private final long numFiles;

  // TODO: consolidate the data structure in the private branch
  public SnapshotState(Metadata metadata, Protocol protocol, long tableSizeBytes, long numFiles) {
    this.metadata = metadata;
    this.protocol = protocol;
    this.tableSizeBytes = tableSizeBytes;
    this.numFiles = numFiles;
  }

  public long getNumFiles() {
    return numFiles;
  }

  public long getTableSizeBytes() {
    return tableSizeBytes;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public Protocol getProtocol() {
    return protocol;
  }

  public Row toCrcRow(Optional<String> tnxId) {
    Map<Integer, Object> value = new HashMap<>();
    value.put(CRC_FILE_SCHEMA.indexOf("tableSizeBytes"), tableSizeBytes);
    value.put(CRC_FILE_SCHEMA.indexOf("numFiles"), numFiles);
    value.put(CRC_FILE_SCHEMA.indexOf("numMetadata"), 1L);
    value.put(CRC_FILE_SCHEMA.indexOf("numProtocol"), 1L);
    value.put(CRC_FILE_SCHEMA.indexOf("metadata"), metadata.toRow());
    value.put(CRC_FILE_SCHEMA.indexOf("protocol"), protocol.toRow());
    tnxId.ifPresent(id -> value.put(CRC_FILE_SCHEMA.indexOf("txnId"), id));
    return new GenericRow(CRC_FILE_SCHEMA, value);
  }
}
