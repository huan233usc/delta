package io.delta.kernel.defaults.engine;


import io.delta.kernel.data.Row;
import io.delta.kernel.engine.*;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.HadoopTables;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class IcebergEngine implements Engine {

    private final Engine delegate;

    private final FileSystemClient deltaLogSimulatingFS;

    private final Configuration hadoopConf;


    public IcebergEngine(Engine delegateEngine, Configuration hadoopConf) {
        this.delegate = delegateEngine;
        this.deltaLogSimulatingFS = delegateEngine.getFileSystemClient();
        this.hadoopConf =hadoopConf;
    }


    @Override
    public ExpressionHandler getExpressionHandler() {
        return delegate.getExpressionHandler();
    }

    @Override
    public JsonHandler getJsonHandler() {
        return new IcebergJsonHandler(delegate.getJsonHandler(), hadoopConf);
    }

    @Override
    public FileSystemClient getFileSystemClient() {
        return deltaLogSimulatingFS;
    }

    @Override
    public ParquetHandler getParquetHandler() {
        return delegate.getParquetHandler();
    }

    @Override
    public List<MetricsReporter> getMetricsReporters() {
        return delegate.getMetricsReporters();
    }

    /**
     * Custom JsonHandler that intercepts Delta JSON writes to create Iceberg commits.
     */
    private static class IcebergJsonHandler implements JsonHandler {
        private final JsonHandler base;
        private final Configuration hadoopConf;
        private final HadoopTables tables;

        public IcebergJsonHandler(JsonHandler base, Configuration conf) {
            this.base = base;
            this.hadoopConf = conf;
            this.tables = new HadoopTables(conf);
        }

        @Override
        public void writeJsonFileAtomically(String filePath, CloseableIterator<Row> data, boolean overwrite)
                throws IOException {
            if (filePath.contains("/_delta_log/")) {
                try {
                    // Collect all rows representing actions
                    List<Row> actions = new ArrayList<>();
                    while (data.hasNext()) {
                        actions.add(data.next());
                    }
                    data.close();
                    String tablePath = filePath.split("/_delta_log/")[0];
                    // Load or create the Iceberg table

                    boolean tableExists = tables.exists(tablePath);
                    Transaction icebergTransaction;

                    if(tableExists) {
                        icebergTransaction = tables.load(tablePath).newTransaction();
                    } else {
                       Optional<Row> rowWithMetadata = actions.stream().filter(
                                row -> !row.isNullAt(SingleAction.METADATA_ORDINAL)
                        ).findFirst();
                       Metadata metadata = Metadata.fromRow(rowWithMetadata.get().getStruct(SingleAction.METADATA_ORDINAL));
                       metadata.getSchema()

                        Catalog.TableBuilder builder = tables.buildTable(
                                tablePath,
                        )

                    }

                } catch (Exception e) {
                    throw new IOException("Failed to apply Delta commit to Iceberg table: " + e.getMessage(), e);
                }
            } else {
                // Delegate other JSON writes (if any)
                base.writeJsonFileAtomically(filePath, data, overwrite);
            }
        }

        @Override
        public io.delta.kernel.engine.ColumnarBatch parseJson(io.delta.kernel.engine.ColumnVector jsonStringVector,
                                                              io.delta.kernel.engine.StructType outputSchema,
                                                              Optional<io.delta.kernel.engine.ColumnVector> selectionVector) {
            // Delegate JSON parsing to base
            return base.parseJson(jsonStringVector, outputSchema, selectionVector);
        }

        @Override
        public io.delta.kernel.engine.StructType deserializeStructType(String structTypeJson) {
            return base.deserializeStructType(structTypeJson);
        }

        @Override
        public CloseableIterator<io.delta.kernel.engine.ColumnarBatch> readJsonFiles(
                CloseableIterator<FileStatus> fileIter,
                io.delta.kernel.engine.StructType physicalSchema,
                Optional<io.delta.kernel.engine.Predicate> predicate) throws IOException {
            return base.readJsonFiles(fileIter, physicalSchema, predicate);
        }

        // Fallback parseJson without selectionVector
        public io.delta.kernel.engine.ColumnarBatch parseJson(io.delta.kernel.engine.ColumnVector jsonStringVector,
                                                              io.delta.kernel.engine.StructType outputSchema) {
            return base.parseJson(jsonStringVector, outputSchema, Optional.empty());
        }
    }
}
