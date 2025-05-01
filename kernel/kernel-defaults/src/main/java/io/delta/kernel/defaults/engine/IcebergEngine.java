package io.delta.kernel.defaults.engine;


import io.delta.kernel.engine.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.hadoop.HadoopTables;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

public class IcebergEngine implements Engine {

    private final Engine delegate;

    private final FileSystemClient deltaLogSimulatingFS;


    public IcebergEngine(Engine delegateEngine, Configuration hadoopConf) {
        this.delegate = delegateEngine;
        this.deltaLogSimulatingFS = new IcebergFileSystemClient(delegateEngine.getFileSystemClient(), hadoopConf);
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
        private final HadoopTables hadoopCatalog;

        IcebergFileSystemClient(FileSystemClient baseFs, Configuration hadoopConf) {
            this.baseFs = baseFs;
            this.hadoopCatalog = new HadoopTables(hadoopConf);
        }

        @Override
        public CloseableIterator<FileStatus> listFrom(String filePath) throws IOException {
            if(!filePath.contains("/_delta_log")){
                return baseFs.listFrom(filePath);
            }
            BaseTable icebergTable = (BaseTable) hadoopCatalog.load(filePath.substring(0, filePath.length() -"/_delta_log".length()));
            long version = getVersionFromIcebergMetadataFile(icebergTable.operations().current().metadataFileLocation());
            
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

    }

    public static long getVersionFromIcebergMetadataFile(String icebergMetadataFileName) {
        // Validate and extract version from Iceberg metadata file name
        if (!icebergMetadataFileName.matches("v\\d+\\.metadata\\.json")) {
            throw new IllegalArgumentException(
                    "Invalid Iceberg metadata file name: " + icebergMetadataFileName);
        }

        // Extract version number (remove 'v' prefix and '.metadata.json' suffix)
        String versionStr = icebergMetadataFileName
                .substring(1, icebergMetadataFileName.indexOf(".metadata.json"));

        try {
            return Long.parseLong(versionStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Could not parse version number from " + icebergMetadataFileName, e);
        }
    }
}
