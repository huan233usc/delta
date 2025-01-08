package io.delta.kernel.internal;

import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import java.util.Optional;

public class TransactionCommitSummary {
  private Optional<Metadata> updatedMetadata;
  private Optional<Protocol> updatedProtocol;
  private long addedTableSize;
  private long addedFileCounts;

  public TransactionCommitSummary() {
    updatedMetadata = Optional.empty();
    updatedProtocol = Optional.empty();
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
}
