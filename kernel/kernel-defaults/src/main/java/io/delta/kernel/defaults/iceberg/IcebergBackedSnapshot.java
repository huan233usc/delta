package io.delta.kernel.defaults.iceberg;

import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.TableMetadata;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class IcebergBackedSnapshot implements Snapshot {

    private final org.apache.iceberg.Table icebergTable;

    public IcebergBackedSnapshot(org.apache.iceberg.Table icebergTable) {
        this.icebergTable = icebergTable;
    }


    @Override
    public long getVersion() {
        BaseTable baseTable = (BaseTable) icebergTable;
        return baseTable.operations().current().lastSequenceNumber() - 1;
    }

    @Override
    public List<String> getPartitionColumnNames() {
        return icebergTable.spec().fields().stream().map(PartitionField::name).collect(Collectors.toList());
    }

    @Override
    public long getTimestamp(Engine engine) {
        return icebergTable.currentSnapshot().timestampMillis();
    }

    @Override
    public StructType getSchema() {
        return IcebergToDeltaSchemaConverter.toKernelSchema(icebergTable.schema());
    }

    @Override
    public Optional<String> getDomainMetadata(String domain) {
        return Optional.empty();
    }

    @Override
    public ScanBuilder getScanBuilder() {
        return null;
    }
}
