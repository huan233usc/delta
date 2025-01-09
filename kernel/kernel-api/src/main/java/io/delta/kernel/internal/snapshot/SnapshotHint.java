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

package io.delta.kernel.internal.snapshot;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.*;
import java.util.*;
import java.util.stream.Collectors;

/** Contains summary information of a {@link io.delta.kernel.Snapshot}. */
public class SnapshotHint {
  private final long version;
  private final Protocol protocol;
  private final Metadata metadata;
  private final OptionalLong tableSizeBytes;
  private final OptionalLong numFiles;
  private final Optional<List<AddFile>> allFiles;

  public SnapshotHint(
      long version,
      Protocol protocol,
      Metadata metadata,
      OptionalLong tableSizeBytes,
      OptionalLong numFiles,
      Optional<List<AddFile>> allFiles) {
    this.version = version;
    this.protocol = protocol;
    this.metadata = metadata;
    this.tableSizeBytes = tableSizeBytes;
    this.numFiles = numFiles;
    this.allFiles = allFiles;
  }

  public long getVersion() {
    return version;
  }

  public Protocol getProtocol() {
    return protocol;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public OptionalLong getTableSizeBytes() {
    return tableSizeBytes;
  }

  public OptionalLong getNumFiles() {
    return numFiles;
  }

  public Optional<List<AddFile>> getAllFiles() {
    return allFiles;
  }

  // HACK
  public static StructType CRC_FILE_SCHEMA =
      new StructType()
          .add("tableSizeBytes", LongType.LONG)
          .add("numFiles", LongType.LONG)
          .add("numMetadata", LongType.LONG)
          .add("numProtocol", LongType.LONG)
          .add("metadata", Metadata.FULL_SCHEMA)
          .add("protocol", Protocol.FULL_SCHEMA)
          .add("txnId", StringType.STRING)
          .add("allFiles", new ArrayType(AddFile.FULL_SCHEMA, true));

  // Hack, to be moved to utils
  public Row toCrcRow(String tnxId) {
    Map<Integer, Object> value = new HashMap<>();
    value.put(CRC_FILE_SCHEMA.indexOf("tableSizeBytes"), tableSizeBytes.getAsLong());
    value.put(CRC_FILE_SCHEMA.indexOf("numFiles"), numFiles.getAsLong());
    value.put(CRC_FILE_SCHEMA.indexOf("numMetadata"), 1L);
    value.put(CRC_FILE_SCHEMA.indexOf("numProtocol"), 1L);
    value.put(CRC_FILE_SCHEMA.indexOf("metadata"), metadata.toRow());
    value.put(CRC_FILE_SCHEMA.indexOf("protocol"), protocol.toRow());
    value.put(CRC_FILE_SCHEMA.indexOf("txnId"), tnxId);
    System.out.println(allFiles);
    allFiles.ifPresent(
        af ->
            value.put(
                CRC_FILE_SCHEMA.indexOf("allFiles"),
                new ArrayValue() {
                  @Override
                  public int getSize() {
                    return af.size();
                  }

                  @Override
                  public ColumnVector getElements() {
                    return new ColumnVector() {
                      @Override
                      public DataType getDataType() {
                        return AddFile.FULL_SCHEMA;
                      }

                      @Override
                      public int getSize() {
                        return af.size();
                      }

                      @Override
                      public void close() {
                        // no-op
                      }

                      @Override
                      public boolean isNullAt(int rowId) {
                        return af.get(rowId) == null;
                      }

                      @Override
                      public ColumnVector getChild(int colId) {
                        if (colId == 0) {
                          return VectorUtils.stringVector(
                              af.stream().map(a -> a.getPath()).collect(Collectors.toList()));
                        }
                        return null;
                      }
                    };
                  }
                }));
    return new GenericRow(CRC_FILE_SCHEMA, value);
  }
}
