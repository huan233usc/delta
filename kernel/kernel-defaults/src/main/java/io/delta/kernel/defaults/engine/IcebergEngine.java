package io.delta.kernel.defaults.engine;


import io.delta.kernel.engine.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

public class IcebergEngine implements Engine {

    private final Engine delegate;

    private final FileSystemClient deltaLogSimulatingFS;


    public IcebergEngine(Engine delegateEngine) {
        this.delegate = delegateEngine;
        this.deltaLogSimulatingFS = new IcebergFileSystemClient(delegateEngine.getFileSystemClient());
    }


    @Override
    public ExpressionHandler getExpressionHandler() {
        return delegate.getExpressionHandler();
    }

    @Override
    public JsonHandler getJsonHandler() {
        return delegate.getJsonHandler();
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

    private class IcebergFileSystemClient implements FileSystemClient {

        private final FileSystemClient baseFs;

        IcebergFileSystemClient(FileSystemClient baseFs) {
            this.baseFs = baseFs;
        }

        @Override
        public CloseableIterator<FileStatus> listFrom(String filePath) throws IOException {
            return baseFs.listFrom(filePath);
        }

        @Override
        public String resolvePath(String path) throws IOException {
            return baseFs.resolvePath(path);
        }

        @Override
        public CloseableIterator<ByteArrayInputStream> readFiles(CloseableIterator<FileReadRequest> readRequests) throws IOException {
            return baseFs.readFiles(readRequests);
        }

        @Override
        public boolean mkdirs(String path) throws IOException {
            return baseFs.mkdirs(path);
        }

        @Override
        public boolean delete(String path) throws IOException {
            return baseFs.delete(path);
        }

        private void generateDeltaLogFromIceberg(String tablePath) {
            Table table = new HadoopTables().load(tablePath);

        }
    }
}
