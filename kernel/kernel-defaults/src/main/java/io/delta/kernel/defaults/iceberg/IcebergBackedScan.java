package io.delta.kernel.defaults.iceberg;

import io.delta.kernel.Scan;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.PartitionSpec;

import java.util.*;

public class IcebergBackedScan implements Scan {

    private final Table icebergTable;
    private final Snapshot snapshot;
    private final Engine engine;
    private final String tableRootPath;

    public IcebergBackedScan(Table icebergTable, Snapshot snapshot, Engine engine) {
        this.icebergTable = icebergTable;
        this.snapshot = snapshot;
        this.engine = engine;
        this.tableRootPath = icebergTable.location();
    }

    @Override
    public CloseableIterator<FilteredColumnarBatch> getScanFiles(Engine engine) {
        System.out.println("IcebergBackedScan.getScanFiles called");
        System.out.println("Snapshot version: " + snapshot.snapshotId());
        System.out.println("Table location: " + icebergTable.location());

        List<DataFile> dataFiles = new ArrayList<>();
        List<ManifestFile> manifests = snapshot.dataManifests(icebergTable.io());
        System.out.println("Manifest count: " + manifests.size());

        for (ManifestFile mf : manifests) {
            System.out.println("Reading manifest file: " + mf.path());
            try (ManifestReader<DataFile> reader = ManifestReader.read(mf, icebergTable.io())) {
                for (DataFile df : reader) {
                    System.out.println("  → DataFile: " + df.path());
                    dataFiles.add(df);
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to read manifest: " + mf.path(), e);
            }
        }

        FilteredColumnarBatch result = buildScanFilesBatch(engine, dataFiles, icebergTable.spec(), tableRootPath);
        System.out.println("Returning batch with rows: " + result.getData().getSize());
        return CloseableIterator.fromList(List.of(result));
    }

    private FilteredColumnarBatch buildScanFilesBatch(
            Engine engine,
            List<DataFile> dataFiles,
            PartitionSpec spec,
            String tableRoot
    ) {
        int numRows = dataFiles.size();

        List<String> pathList = new ArrayList<>(numRows);
        List<Map<String, String>> partitionMapList = new ArrayList<>(numRows);
        List<Long> sizeList = new ArrayList<>(numRows);
        List<Long> modTimeList = new ArrayList<>(numRows);
        List<Boolean> dataChangeList = new ArrayList<>(numRows);
        List<Map<String, String>> tagsList = new ArrayList<>(numRows);
        List<String> tableRootList = new ArrayList<>(numRows);

        for (DataFile file : dataFiles) {
            pathList.add(file.path().toString());

            Map<String, String> partMap = new HashMap<>();
            StructLike partition = file.partition();
            for (int i = 0; i < spec.fields().size(); i++) {
                String name = spec.fields().get(i).name();
                Object value = partition.get(i, Object.class);
                partMap.put(name, value == null ? null : value.toString());
            }
            partitionMapList.add(partMap);

            sizeList.add(file.fileSizeInBytes());
            modTimeList.add(System.currentTimeMillis());
            dataChangeList.add(true);
            tagsList.add(Collections.emptyMap());
            tableRootList.add(tableRoot);
        }

        StructType addFileSchema = new StructType(new StructField[]{
                new StructField("path", new StringType(), false),
                new StructField("partitionValues", new MapType(new StringType(), new StringType(), true), false),
                new StructField("size", new LongType(), false),
                new StructField("modificationTime", new LongType(), false),
                new StructField("dataChange", new BooleanType(), false),
                new StructField("deletionVector", new StringType(), true),
                new StructField("tags", new MapType(new StringType(), new StringType(), true), true)
        });

        ColumnVector[] addCols = new ColumnVector[]{
                engine.getVectorUtils().stringVectorFrom(pathList),
                engine.getVectorUtils().mapVectorFrom(partitionMapList, engine.getVectorUtils()::stringVectorFrom, engine.getVectorUtils()::stringVectorFrom),
                engine.getVectorUtils().longVectorFrom(sizeList),
                engine.getVectorUtils().longVectorFrom(modTimeList),
                engine.getVectorUtils().booleanVectorFrom(dataChangeList),
                engine.getVectorUtils().nullableVectorFrom(Collections.nCopies(numRows, null), new StringType()),
                engine.getVectorUtils().mapVectorFrom(tagsList, engine.getVectorUtils()::stringVectorFrom, engine.getVectorUtils()::stringVectorFrom)
        };

        ColumnVector addStructVector = engine.getVectorUtils().structVector(addFileSchema, addCols);
        ColumnVector tableRootVector = engine.getVectorUtils().stringVectorFrom(tableRootList);

        ColumnarBatch batch = engine.getVectorUtils().columnarBatch(new ColumnVector[]{
                addStructVector,
                tableRootVector
        });

        return new FilteredColumnarBatch(batch);
    }

    @Override
    public Optional<io.delta.kernel.expressions.Predicate> getRemainingFilter() {
        return Optional.empty();
    }

    @Override
    public io.delta.kernel.data.Row getScanState(Engine engine) {
        return VectorUtils.emptyRow();
    }
}
