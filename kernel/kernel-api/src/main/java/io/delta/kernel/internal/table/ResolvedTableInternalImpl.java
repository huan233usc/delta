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

package io.delta.kernel.internal.table;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.ScanBuilder;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.ScanBuilderImpl;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.metrics.SnapshotQueryContext;
import io.delta.kernel.internal.metrics.SnapshotReportImpl;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.Clock;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.metrics.SnapshotReport;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.util.*;
import java.util.stream.Collectors;

/** An implementation of {@link ResolvedTableInternal}. */
public class ResolvedTableInternalImpl implements ResolvedTableInternal {
  private final String path;
  private final String logPath;
  private final long version;
  private final Protocol protocol;
  private final Metadata metadata;
  private final Lazy<LogSegment> lazyLogSegment;
  private final LogReplay logReplay;
  private final Clock clock;
  private final SnapshotReport snapshotReport;

  public ResolvedTableInternalImpl(
      String path,
      long version,
      Protocol protocol,
      Metadata metadata,
      Lazy<LogSegment> lazyLogSegment,
      LogReplay logReplay,
      Clock clock,
      SnapshotQueryContext snapshotCtx) {
    this.path = requireNonNull(path, "path is null");
    this.logPath = new Path(path, "_delta_log").toString();
    this.version = version;
    this.protocol = requireNonNull(protocol, "protocol is null");
    this.metadata = requireNonNull(metadata, "metadata is null");
    this.lazyLogSegment = requireNonNull(lazyLogSegment, "lazyLogSegment is null");
    this.logReplay = requireNonNull(logReplay, "logReplay is null");
    this.clock = requireNonNull(clock, "clock is null");
    this.snapshotReport = SnapshotReportImpl.forSuccess(snapshotCtx);
  }

  //////////////////////////////////
  // Public ResolvedTable Methods //
  //////////////////////////////////

  @Override
  public String getPath() {
    return path;
  }

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public long getTimestamp() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public List<Column> getPartitionColumns() {
    return VectorUtils.<String>toJavaList(getMetadata().getPartitionColumns()).stream()
        .map(Column::new)
        .collect(Collectors.toList());
  }

  @Override
  public Optional<String> getDomainMetadata(String domain) {
    return Optional.ofNullable(getActiveDomainMetadataMap().get(domain))
        .map(DomainMetadata::getConfiguration);
  }

  @Override
  public StructType getSchema() {
    return getMetadata().getSchema();
  }

  @Override
  public ScanBuilder getScanBuilder() {
    return new ScanBuilderImpl(
        new Path(getPath()), getProtocol(), getMetadata(), getSchema(), logReplay, snapshotReport);
  }

  ///////////////////////////////////////
  // ResolvedTableInternalImpl Methods //
  ///////////////////////////////////////

  @Override
  public String getLogPath() {
    return logPath;
  }

  @Override
  public Protocol getProtocol() {
    return protocol;
  }

  @Override
  public Metadata getMetadata() {
    return metadata;
  }

  @Override
  public Clock getClock() {
    return clock;
  }

  @Override
  public Map<String, DomainMetadata> getActiveDomainMetadataMap() {
    return logReplay.getActiveDomainMetadataMap();
  }

  @Override
  @VisibleForTesting
  public LogSegment getLogSegment() {
    return lazyLogSegment.get();
  }

  @Override
  @VisibleForTesting
  public Lazy<LogSegment> getLazyLogSegment() {
    return lazyLogSegment;
  }

  /**
   * Returns the raw delta actions for each version between startVersion and endVersion. Only reads
   * the actions requested in actionSet from the JSON log files.
   *
   * <p>For the returned columnar batches:
   *
   * <ul>
   *   <li>Each row within the same batch is guaranteed to have the same commit version
   *   <li>The batch commit versions are monotonically increasing
   *   <li>The top-level columns include "version", "timestamp", and the actions requested in
   *       actionSet. "version" and "timestamp" are the first and second columns in the schema,
   *       respectively. The remaining columns are based on the actions requested and each have the
   *       schema found in {@code DeltaAction.schema}.
   * </ul>
   *
   * @param engine {@link Engine} instance to use in Delta Kernel.
   * @param startVersion start version (inclusive)
   * @param endVersion end version (inclusive)
   * @param actionSet the actions to read and return from the JSON log files
   * @return an iterator of batches where each row in the batch has exactly one non-null action and
   *     its commit version and timestamp
   * @throws TableNotFoundException if the table does not exist or if it is not a delta table
   * @throws KernelException if a commit file does not exist for any of the versions in the provided
   *     range
   * @throws KernelException if provided an invalid version range
   */
  private CloseableIterator<ColumnarBatch> getRawChanges(
      Engine engine,
      long startVersion,
      long endVersion,
      Set<DeltaLogActionUtils.DeltaAction> actionSet) {
    ;
    List<FileStatus> commitFiles =
        DeltaLogActionUtils.getCommitFilesForVersionRange(
            engine, new Path(path), startVersion, endVersion);

    StructType readSchema =
        new StructType(
            actionSet.stream()
                .map(action -> new StructField(action.colName, action.schema, true))
                .collect(Collectors.toList()));

    return DeltaLogActionUtils.readCommitFiles(engine, commitFiles, readSchema);
  }

  public CloseableIterator<ColumnarBatch> getChanges(
      Engine engine,
      long startVersion,
      long endVersion,
      Set<DeltaLogActionUtils.DeltaAction> actionSet) {
    // Create a new action set which is a super set of the requested actions.
    // The extra actions are needed either for checks or to extract
    // extra information. We will strip out the extra actions before
    // returning the result.
    Set<DeltaLogActionUtils.DeltaAction> copySet = new HashSet<>(actionSet);
    copySet.add(DeltaLogActionUtils.DeltaAction.PROTOCOL);
    // commitInfo is needed to extract the inCommitTimestamp of delta files
    copySet.add(DeltaLogActionUtils.DeltaAction.COMMITINFO);
    // Determine whether the additional actions were in the original set.
    boolean shouldDropProtocolColumn =
        !actionSet.contains(DeltaLogActionUtils.DeltaAction.PROTOCOL);
    boolean shouldDropCommitInfoColumn =
        !actionSet.contains(DeltaLogActionUtils.DeltaAction.COMMITINFO);

    return getRawChanges(engine, startVersion, endVersion, copySet)
        .map(
            batch -> {
              int protocolIdx = batch.getSchema().indexOf("protocol"); // must exist
              ColumnVector protocolVector = batch.getColumnVector(protocolIdx);
              for (int rowId = 0; rowId < protocolVector.getSize(); rowId++) {
                if (!protocolVector.isNullAt(rowId)) {
                  Protocol protocol = Protocol.fromColumnVector(protocolVector, rowId);
                  TableFeatures.validateKernelCanReadTheTable(protocol, path.toString());
                }
              }
              ColumnarBatch batchToReturn = batch;
              if (shouldDropProtocolColumn) {
                batchToReturn = batchToReturn.withDeletedColumnAt(protocolIdx);
              }
              int commitInfoIdx = batchToReturn.getSchema().indexOf("commitInfo");
              if (shouldDropCommitInfoColumn) {
                batchToReturn = batchToReturn.withDeletedColumnAt(commitInfoIdx);
              }
              return batchToReturn;
            });
  }
}
