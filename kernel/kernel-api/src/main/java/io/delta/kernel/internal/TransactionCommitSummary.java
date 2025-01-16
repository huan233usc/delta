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
package io.delta.kernel.internal;

import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TransactionCommitSummary {
  private Optional<Metadata> updatedMetadata;
  private Optional<Protocol> updatedProtocol;
  private long addedTableSize;
  private long addedFileCounts;
  private List<AddFile> addFiles;

  public TransactionCommitSummary() {
    updatedMetadata = Optional.empty();
    updatedProtocol = Optional.empty();
    addFiles = new ArrayList<>();
  }

  public void setUpdatedMetadata(Metadata metadata) {
    updatedMetadata = Optional.of(metadata);
  }

  public void setUpdatedProtocol(Protocol protocol) {
    updatedProtocol = Optional.of(protocol);
  }

  public void onAddFile(AddFile file) {
    addedFileCounts++;
    addedTableSize += file.getSize();
    addFiles.add(file);
  }

  public Optional<Metadata> getUpdatedMetadata() {
    return updatedMetadata;
  }

  public Optional<Protocol> getUpdatedProtocol() {
    return updatedProtocol;
  }

  public long getAddedTableSize() {
    return addedTableSize;
  }

  public long getAddedFileCounts() {
    return addedFileCounts;
  }

  public List<AddFile> getAddFiles() {
    return addFiles;
  }
}
