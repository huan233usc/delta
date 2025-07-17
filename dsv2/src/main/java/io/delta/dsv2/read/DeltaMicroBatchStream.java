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
package io.delta.dsv2.read;

import io.delta.kernel.ResolvedTable;
import io.delta.kernel.Table;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.ScanImpl;
import io.delta.kernel.internal.TableImpl;
import io.delta.kernel.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.streaming.LongOffset;

/** Implements the logic for reading a Delta table as a stream of micro-batches. */
public class DeltaMicroBatchStream implements MicroBatchStream {

  private final Engine engine;
  private final ResolvedTable resolvedTable;
  private final String checkpointLocation;
  private final String accessKey;
  private final String secretKey;
  private final String sessionToken;

  //    private final InitialOffsetStore initialOffsetStore;

  public DeltaMicroBatchStream(
      Engine engine,
      ResolvedTable table,
      String checkpointLocation,
      String accessKey,
      String secretKey,
      String sessionToken) {
    this.engine = engine;
    this.resolvedTable = table;
    this.checkpointLocation = checkpointLocation;
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.sessionToken = sessionToken;
    //        this.initialOffsetStore =
    //            new InitialOffsetStore(engine, resolvedTable, checkpointLocation,
    // Optional.empty());
  }

  @Override
  public Offset latestOffset() {
    long latestVersion = TableManager.loadTable(resolvedTable.getPath()).build(engine).getVersion();
    return new LongOffset(latestVersion);
  }

  @Override
  public InputPartition[] planInputPartitions(Offset start, Offset end) {
    System.out.println("start" + start);
    System.out.println("end" + end);
    long startVersion = ((LongOffset) start).offset();
    long endVersion = ((LongOffset) end).offset();
    Set<DeltaLogActionUtils.DeltaAction> set = new HashSet<>();
    set.add(DeltaLogActionUtils.DeltaAction.ADD);
    Table tableForChanges = TableImpl.forPath(engine, resolvedTable.getPath());
    ScanImpl scan = (ScanImpl) tableForChanges.getLatestSnapshot(engine).getScanBuilder().build();
    CloseableIterator<FilteredColumnarBatch> files =
        scan.getScanFiles(engine, true, startVersion, endVersion);

    String scanStateJson =
        JsonUtils.rowToJson(
            tableForChanges
                .getLatestSnapshot(engine)
                .getScanBuilder()
                .build()
                .getScanState(engine));

    List<DeltaInputPartition> partitions = new ArrayList<>();

    while (files.hasNext()) {
      FilteredColumnarBatch columnarBatch = files.next();
      Iterator<Row> rows = columnarBatch.getRows();
      while (rows.hasNext()) {
        Row row = rows.next();
        String fileRowJson = JsonUtils.rowToJson(row);
        System.out.println(fileRowJson);
        partitions.add(
            new DeltaInputPartition(
                fileRowJson, scanStateJson, accessKey, secretKey, sessionToken));
      }
    }
    return partitions.toArray(new InputPartition[0]);
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new DeltaReaderFactory();
  }

  @Override
  public Offset initialOffset() {
    return LongOffset.apply(0);
  }

  @Override
  public Offset deserializeOffset(String json) {
    return LongOffset.apply(Long.valueOf(json));
  }

  @Override
  public void commit(Offset end) {
    // No-op for Delta's transactional model. Offsets are stored by Spark.
  }

  @Override
  public void stop() {
    // No-op.
  }

  //    /**
  //     * Manages the logic for determining and persisting the initial offset for the stream.
  //     * This ensures that on restart, the stream continues from a consistent starting point.
  //     * This implementation is inspired by Apache Iceberg's SparkMicroBatchStream.
  //     */
  //    private static class InitialOffsetStore {
  //        private final Engine engine;
  //        private final ResolvedTable resolvedTable;
  //        private final String offsetLocation;
  //        private final Optional<Long> fromTimestamp;
  //        private final transient FileIO fileIO;
  //
  //        InitialOffsetStore(
  //                Engine engine,
  //                ResolvedTable resolvedTable,
  //                String checkpointLocation,
  //                Optional<Long> fromTimestamp) {
  //            this.engine = engine;
  //            this.resolvedTable = resolvedTable;
  //            this.offsetLocation =
  //                new Path(new Path(checkpointLocation, "offsets"), "0").toString();
  //            this.fromTimestamp = fromTimestamp;
  //
  //            // For accessing checkpoint files, which are not part of the table data,
  //            // we create our own FileIO instance. In a real Spark environment, this
  //            // should get the configuration from the Spark session.
  //            this.fileIO = HadoopFileIO.builder()
  //                .withConf(new Configuration())
  //                .build();
  //        }
  //
  //        public LongOffset getInitialOffset() {
  //            try {
  //                if (fileIO.fileExists(offsetLocation)) {
  //                    InputFile inputFile = fileIO.newInputFile(offsetLocation);
  //                    return readOffset(inputFile);
  //                } else {
  //                    LongOffset offset = determineStartingOffset();
  //                    OutputFile outputFile = fileIO.newOutputFile(offsetLocation);
  //                    writeOffset(offset, outputFile);
  //                    return offset;
  //                }
  //            } catch (IOException e) {
  //                throw new UncheckedIOException(
  //                    "Failed to read/write initial offset from: " + offsetLocation, e);
  //            }
  //        }
  //
  //        private LongOffset determineStartingOffset() {
  //            if (fromTimestamp.isPresent()) {
  //                try {
  //                    Table table = Table.forPath(engine, resolvedTable.getPath());
  //                    long version = table.getSnapshotAsOfTimestamp(engine, fromTimestamp.get())
  //                            .getVersion(engine);
  //                    return new LongOffset(version);
  //                } catch (TableNotFoundException e) {
  //                    throw new RuntimeException(
  //                        "Failed to determine starting version from timestamp", e);
  //                }
  //            } else {
  //                return new LongOffset(0L);
  //            }
  //        }
  //
  //        private void writeOffset(LongOffset offset, OutputFile outputFile) throws IOException {
  //             try (OutputStream outputStream = outputFile.createOrOverwrite()) {
  //                try (BufferedWriter writer =
  //                    new BufferedWriter(new OutputStreamWriter(outputStream,
  // StandardCharsets.UTF_8))) {
  //                    writer.write(offset.json());
  //                    writer.flush();
  //                }
  //            }
  //        }
  //
  //        private LongOffset readOffset(InputFile inputFile) throws IOException {
  //            try (InputStream in = inputFile.open()) {
  //                try (BufferedReader reader =
  //                    new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
  //                    String json = reader.readLine();
  //                    if (json == null) {
  //                        throw new IOException("Offset file is empty: " + inputFile.getPath());
  //                    }
  //                    return LongOffset.fromJson(json);
  //                }
  //            }
  //        }
  //    }
}
