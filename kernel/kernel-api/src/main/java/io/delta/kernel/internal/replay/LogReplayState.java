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

package io.delta.kernel.internal.replay;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.checksum.CRCInfo;
import io.delta.kernel.internal.checksum.ChecksumReader;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.snapshot.SnapshotHint;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.DomainMetadataUtils;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages state during log replay, allowing progressive loading of different action types. This
 * class provides efficient access to table metadata by scanning the log only once.
 */
public class LogReplayState implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(LogReplayState.class);

  // State machine for log scanning
  private enum ScanState {
    START, // Initial state
    SCAN_CURRENT_CRC, // Try to use current CRC
    SCAN_LOG, // Scan log files
    COMPLETED // Scan complete
  }

  private ScanState scanState = ScanState.START;

  private final LogSegment logSegment;
  private final CrcInfoContext crcInfoContext;
  private final Path dataPath;

  // Current position in the log
  private long currentVersion = -1;

  // Iterator for scanning the log
  private final CloseableIterator<ActionWrapper> actionIterator;

  // State components
  private final PandMState coreComponent;
  private final DomainMetadataState domainMetadataComponent;
  private final List<ComponentState> allComponents = new ArrayList<>();

  /** Constructs a new LogReplayState and initializes the action iterator. */
  public LogReplayState(
      Engine engine, LogSegment logSegment, Path dataPath, Optional<SnapshotHint> snapshotHint) {
    this.logSegment = logSegment;
    this.dataPath = dataPath;
    this.crcInfoContext = new CrcInfoContext(engine, logSegment);

    // Initialize components with snapshot hint
    this.coreComponent = new PandMState(snapshotHint);
    this.domainMetadataComponent = new DomainMetadataState();

    // Add components to the list for batch processing
    allComponents.add(coreComponent);
    allComponents.add(domainMetadataComponent);

    // Initialize iterator with schema for all possible components we might need
    StructType schema = buildSchema();
    this.actionIterator =
        new ActionsIterator(engine, getLogReplayFiles(logSegment), schema, Optional.empty());
  }

  /** Get Protocol and Metadata. */
  public Tuple2<Protocol, Metadata> getProtocolAndMetadata() {
    // Make sure core state is loaded
    loadComponent(coreComponent);

    // Get Protocol and Metadata
    Protocol protocol = coreComponent.getProtocol().orElse(null);
    Metadata metadata = coreComponent.getMetadata().orElse(null);

    // Validate
    if (protocol == null) {
      throw new IllegalStateException(
          String.format("No protocol found at version %s", logSegment.getVersion()));
    }

    if (metadata == null) {
      throw new IllegalStateException(
          String.format("No metadata found at version %s", logSegment.getVersion()));
    }

    TableFeatures.validateKernelCanReadTheTable(protocol, dataPath.toString());
    return new Tuple2<>(protocol, metadata);
  }

  /** Get domain metadata. */
  public Map<String, DomainMetadata> getActiveDomainMetadataMap() {
    // Load domain metadata component
    loadComponent(domainMetadataComponent);
    return domainMetadataComponent.getActiveDomainMetadataMap();
  }

  /** Returns the CRC info for the current snapshot if it is cached. */
  public Optional<CRCInfo> getCurrentCrcInfo() {
    return crcInfoContext
        .getLastSeenCrcInfo()
        .filter(crcInfo -> crcInfo.getVersion() == logSegment.getVersion());
  }

  /** Loads a component according to the state machine flow. */
  private void loadComponent(ComponentState targetComponent) {
    // If component is already loaded, return immediately
    if (targetComponent.isLoaded()) {
      return;
    }

    // State machine processing
    while (!targetComponent.isLoaded()) {
      switch (scanState) {
        case START:
          // Move to next state - we've already processed snapshot hint in constructor
          scanState = ScanState.SCAN_CURRENT_CRC;
          break;
        case SCAN_CURRENT_CRC:
          // Try to load from current CRC
          tryLoadFromCurrentCrc();
          // Proceed to log scanning if components still need loading
          scanState = ScanState.SCAN_LOG;
          break;

        case SCAN_LOG:
          // Scan the log until the component is loaded or we reach the end
          scanLog(targetComponent);
          break;

        default:
          // Should not reach here since we check in the loop condition
          break;
      }
    }
  }

  /** Try to load data from current CRC file if available and applicable. */
  private void tryLoadFromCurrentCrc() {
    if (!crcInfoContext.getLastSeenCrcVersion().isPresent()) {
      return;
    }
    if (crcInfoContext.getLastSeenCrcVersion().get() != logSegment.getVersion()) {
      return;
    }
    Optional<CRCInfo> crcInfo = crcInfoContext.getLastSeenCrcInfo();
    checkState(
        crcInfo.isPresent() && crcInfo.get().getVersion() == logSegment.getVersion(), "TODO");
    // Load all unloaded components from CRC
    for (ComponentState component : allComponents) {
      if (!component.isLoaded()) {
        component.loadFromCurrentCrc(crcInfo.get());
      }
    }
  }

  /** Scans the log until the specified component is loaded or we reach the end. */
  private void scanLog(ComponentState targetComponent) {
    try {
      // Continue scanning until target component is loaded or we reach the end
      while (!targetComponent.isLoaded()) {
        if (!actionIterator.hasNext()) {
          scanState = ScanState.COMPLETED;
          break;
        }
        ActionWrapper nextAction = actionIterator.next();
        currentVersion = nextAction.getVersion();
        ColumnarBatch batch = nextAction.getColumnarBatch();

        // Process this batch for ALL components, not just the target one
        for (ComponentState component : allComponents) {
          if (!component.isLoaded()) {
            component.processColumnarBatch(batch, currentVersion);
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Error scanning log" + e, e);
    }
  }

  /** Builds schema for all components that might need loading. */
  private StructType buildSchema() {
    return new StructType()
        .add("protocol", Protocol.FULL_SCHEMA)
        .add("metaData", Metadata.FULL_SCHEMA)
        .add("domainMetadata", DomainMetadata.FULL_SCHEMA);
  }

  /** Get the files to use for log replay. */
  private List<FileStatus> getLogReplayFiles(LogSegment logSegment) {
    return logSegment.allFilesWithCompactionsReversed();
  }

  @Override
  public void close() throws IOException {
    actionIterator.close();
  }

  /** Base interface for components that store state from the log. */
  private interface ComponentState {
    /** Determines if this component is fully loaded. */
    boolean isLoaded();

    /** Try to load component data from CRC info. */
    void loadFromCurrentCrc(CRCInfo crcInfo);

    /** Process a columnar batch to extract relevant data. */
    void processColumnarBatch(ColumnarBatch batch, long version);
  }

  /** Tracks Protocol and Metadata state. */
  private class PandMState implements ComponentState {
    private Optional<Protocol> protocol;
    private Optional<Metadata> metadata;
    private final Optional<SnapshotHint> snapshotHint;

    PandMState(Optional<SnapshotHint> snapshotHint) {
      if (snapshotHint.isPresent() && snapshotHint.get().getVersion() == logSegment.getVersion()) {
        this.protocol = Optional.of(snapshotHint.get().getProtocol());
        this.metadata = Optional.of(snapshotHint.get().getMetadata());
      } else {
        this.protocol = Optional.empty();
        this.metadata = Optional.empty();
      }
      this.snapshotHint = snapshotHint;
    }

    @Override
    public void processColumnarBatch(ColumnarBatch batch, long version) {
      StructType schema = batch.getSchema();
      List<String> fieldNames = schema.fieldNames();

      if (fieldNames.contains("protocol") && fieldNames.contains("metaData")) {
        ColumnVector protocolVector = batch.getColumnVector(schema.indexOf("protocol"));
        ColumnVector metadataVector = batch.getColumnVector(schema.indexOf("metaData"));

        // Process Protocol if not found yet
        if (!protocol.isPresent()) {
          for (int i = 0; i < protocolVector.getSize(); i++) {
            if (!protocolVector.isNullAt(i)) {
              protocol = Optional.of(Protocol.fromColumnVector(protocolVector, i));
              break;
            }
          }
        }

        // Process Metadata if not found yet
        if (!metadata.isPresent()) {
          for (int i = 0; i < metadataVector.getSize(); i++) {
            if (!metadataVector.isNullAt(i)) {
              metadata = Optional.of(Metadata.fromColumnVector(metadataVector, i));
              break;
            }
          }
        }
      }
      if (!isLoaded()) {
        if (snapshotHint.isPresent() && snapshotHint.get().getVersion() == version - 1) {
          this.protocol = Optional.of(snapshotHint.get().getProtocol());
          this.metadata = Optional.of(snapshotHint.get().getMetadata());
        } else if (crcInfoContext.getLastSeenCrcVersion().isPresent()
            && crcInfoContext.getLastSeenCrcVersion().get() == version - 1) {
          checkState(
              crcInfoContext.getLastSeenCrcInfo().isPresent()
                  && crcInfoContext.getLastSeenCrcInfo().get().getVersion() == version - 1,
              "TODO");
          this.protocol = Optional.of(crcInfoContext.getLastSeenCrcInfo().get().getProtocol());
          this.metadata = Optional.of(crcInfoContext.getLastSeenCrcInfo().get().getMetadata());
        }
      }
    }

    @Override
    public boolean isLoaded() {
      return protocol.isPresent() && metadata.isPresent();
    }

    @Override
    public void loadFromCurrentCrc(CRCInfo crcInfo) {
      checkArgument(!isLoaded());
      checkArgument(crcInfo.getVersion() == logSegment.getVersion());
      protocol = Optional.of(crcInfo.getProtocol());
      metadata = Optional.of(crcInfo.getMetadata());
    }

    public Optional<Protocol> getProtocol() {
      return protocol;
    }

    public Optional<Metadata> getMetadata() {
      return metadata;
    }
  }

  /** Tracks Domain Metadata state. */
  private class DomainMetadataState implements ComponentState {
    private final Map<String, DomainMetadata> domainMetadataMap = new HashMap<>();
    private boolean fullyLoaded = false;

    @Override
    public void processColumnarBatch(ColumnarBatch batch, long version) {
      StructType schema = batch.getSchema();
      List<String> fieldNames = schema.fieldNames();

      if (fieldNames.contains("domainMetadata")) {
        ColumnVector dmVector = batch.getColumnVector(schema.indexOf("domainMetadata"));
        DomainMetadataUtils.populateDomainMetadataMap(dmVector, domainMetadataMap);
      }

      // Try to supplement from CRC info if appropriate
      trySupplementFromCrc(version);
    }

    /** Try to supplement domain metadata from CRC info if we've reached an appropriate version. */
    private void trySupplementFromCrc(long currentVersion) {
      if (!crcInfoContext.getLastSeenCrcVersion().isPresent()) {
        return;
      }
      if (crcInfoContext.getLastSeenCrcVersion().get() != currentVersion - 1) {
        return;
      }
      Optional<CRCInfo> crcInfo = crcInfoContext.getLastSeenCrcInfo();
      checkState(crcInfo.isPresent() && crcInfo.get().getVersion() == currentVersion - 1, "TODO");
      if (crcInfo.get().getDomainMetadata().isPresent()) {
        // Use domain metadata from CRC to supplement what we've collected
        crcInfo
            .get()
            .getDomainMetadata()
            .get()
            .forEach(
                dm -> {
                  if (!domainMetadataMap.containsKey(dm.getDomain())) {
                    domainMetadataMap.put(dm.getDomain(), dm);
                  }
                });

        // Mark as fully loaded since we've supplemented from CRC
        fullyLoaded = true;
      }
    }

    @Override
    public void loadFromCurrentCrc(CRCInfo crcInfo) {
      checkArgument(crcInfo.getVersion() == logSegment.getVersion());
      checkArgument(!isLoaded());
      checkArgument(domainMetadataMap.isEmpty());
      if (crcInfo.getDomainMetadata().isPresent()) {
        crcInfo.getDomainMetadata().get().forEach(dm -> domainMetadataMap.put(dm.getDomain(), dm));
        fullyLoaded = true;
      }
    }

    /** Returns only active (not removed) domain metadata. */
    public Map<String, DomainMetadata> getActiveDomainMetadataMap() {
      return domainMetadataMap.entrySet().stream()
          .filter(entry -> !entry.getValue().isRemoved())
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public boolean isLoaded() {
      return fullyLoaded || scanState == ScanState.COMPLETED;
    }
  }

  /**
   * Encapsulates CRC-related functionality and state for the LogReplayState. This includes caching
   * CRC info and extracting snapshot hints from CRC files.
   */
  public static class CrcInfoContext {
    private final Engine engine;
    private final LogSegment logSegment;
    private Optional<CRCInfo> cachedLastSeenCrcInfo;

    public CrcInfoContext(Engine engine, LogSegment logSegment) {
      this.engine = requireNonNull(engine);
      this.logSegment = requireNonNull(logSegment);
      this.cachedLastSeenCrcInfo = Optional.empty();
    }

    public Optional<Long> getLastSeenCrcVersion() {
      return logSegment
          .getLastSeenChecksum()
          .map(fileStatus -> FileNames.getFileVersion(new Path(fileStatus.getPath())));
    }

    /** Returns the CRC info persisted in the logSegment's lastSeenChecksum File */
    public Optional<CRCInfo> getLastSeenCrcInfo() {
      if (!cachedLastSeenCrcInfo.isPresent()) {
        cachedLastSeenCrcInfo =
            logSegment
                .getLastSeenChecksum()
                .flatMap(crcFile -> ChecksumReader.getCRCInfo(engine, crcFile));
      }
      return cachedLastSeenCrcInfo;
    }
  }
}
