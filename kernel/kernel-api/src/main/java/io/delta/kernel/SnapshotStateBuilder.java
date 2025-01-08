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

import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;

public class SnapshotStateBuilder {

  private Metadata metadata;
  private Protocol protocol;
  private long tableSizeBytes;
  private long numFiles;

  public SnapshotStateBuilder(Metadata metadata, Protocol protocol) {
    this.metadata = metadata;
    this.protocol = protocol;
  }

  public SnapshotStateBuilder(SnapshotState snapshotState) {
    this.metadata = snapshotState.getMetadata();
    this.protocol = snapshotState.getProtocol();
    this.numFiles = snapshotState.getNumFiles();
    this.tableSizeBytes = snapshotState.getTableSizeBytes();
  }

  public void addFile(AddFile addFile) {
    this.numFiles = this.numFiles + 1;
    this.tableSizeBytes = this.tableSizeBytes + addFile.getSize();
  }

  public void setMetadata(Metadata metadata) {
    this.metadata = metadata;
  }

  public void setProtocol(Protocol protocol) {
    this.protocol = protocol;
  }

  public SnapshotState build() {
    return new SnapshotState(metadata, protocol, numFiles, tableSizeBytes);
  }
}
