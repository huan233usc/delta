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

import static io.delta.kernel.internal.DeltaErrors.wrapEngineExceptionThrowsIO;
import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructType;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CrcFileTransactionStatCollector implements TransactionStatCollector {

  private long tableSizeBytes;
  private long numFiles;
  private Metadata metadata;
  private Protocol protocol;
  private Path crcFilePath;

  public CrcFileTransactionStatCollector(Path crcFilePath) {
    this.tableSizeBytes = 0;
    this.numFiles = 0;
    this.crcFilePath = crcFilePath;
  }

  public static StructType CRC_FILE_SCHEMA =
      new StructType()
          .add("tableSizeBytes", LongType.LONG)
          .add("numFiles", LongType.LONG)
          .add("numMetadata", LongType.LONG)
          .add("numProtocol", LongType.LONG)
          .add("metadata", Metadata.FULL_SCHEMA)
          .add("protocol", Protocol.FULL_SCHEMA);

  @Override
  public void recordAddedFile(AddFile addFile) {
    this.numFiles = this.numFiles + 1;
    this.tableSizeBytes = this.tableSizeBytes + addFile.getSize();
  }

  @Override
  public void recordMetadata(Metadata metadata) {
    this.metadata = metadata;
  }

  @Override
  public void recordProtocol(Protocol protocol) {
    this.protocol = protocol;
  }

  public Row getCrcRow() {
    Map<Integer, Object> value = new HashMap<>();
    value.put(CRC_FILE_SCHEMA.indexOf("tableSizeBytes"), tableSizeBytes);
    value.put(CRC_FILE_SCHEMA.indexOf("numFiles"), numFiles);
    value.put(CRC_FILE_SCHEMA.indexOf("numMetadata"), 1L);
    value.put(CRC_FILE_SCHEMA.indexOf("numProtocol"), 1L);
    value.put(CRC_FILE_SCHEMA.indexOf("metadata"), metadata.toRow());
    value.put(CRC_FILE_SCHEMA.indexOf("protocal"), protocol.toRow());
    return new GenericRow(CRC_FILE_SCHEMA, value);
  }

  @Override
  public void onCommitSucceeds(Engine engine, long commitAsVersion) throws IOException {
    // Write the staged data to a delta file
    wrapEngineExceptionThrowsIO(
        () -> {
          engine
              .getJsonHandler()
              .writeJsonFileAtomically(
                  FileNames.crcFile(crcFilePath, commitAsVersion),
                  toCloseableIterator(Arrays.asList(getCrcRow()).iterator()),
                  false /* overwrite */);
          return null;
        },
        "Write file actions to JSON log file `%s`",
        FileNames.crcFile(crcFilePath, commitAsVersion));
  }
}
