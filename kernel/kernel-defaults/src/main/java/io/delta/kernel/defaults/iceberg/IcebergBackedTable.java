package io.delta.kernel.defaults.iceberg;

import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.CheckpointAlreadyExistsException;
import io.delta.kernel.exceptions.TableNotFoundException;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopTables;

import java.io.IOException;

public class IcebergBackedTable implements Table {

    private final HadoopTables icebergCatalog;
    private final String tablePath;
    private final org.apache.iceberg.Table icebergTable;

    public IcebergBackedTable(HadoopTables icebergCatalog, String tablePath) {
        this.icebergCatalog = icebergCatalog;
        this.tablePath = tablePath;
        this.icebergTable = icebergCatalog.load(tablePath);
    }


    @Override
    public String getPath(Engine engine) {
        return tablePath;
    }

    @Override
    public Snapshot getLatestSnapshot(Engine engine) throws TableNotFoundException {
        return new IcebergBackedSnapshot(icebergTable);
    }

    @Override
    public Snapshot getSnapshotAsOfVersion(Engine engine, long versionId) throws TableNotFoundException {
        return null;
    }

    @Override
    public Snapshot getSnapshotAsOfTimestamp(Engine engine, long millisSinceEpochUTC) throws TableNotFoundException {
        return null;
    }

    @Override
    public TransactionBuilder createTransactionBuilder(Engine engine, String engineInfo, Operation operation) {
        return null;
    }

    @Override
    public void checkpoint(Engine engine, long version) throws TableNotFoundException, CheckpointAlreadyExistsException, IOException {

    }

    @Override
    public void checksum(Engine engine, long version) throws TableNotFoundException, IOException {

    }
}
