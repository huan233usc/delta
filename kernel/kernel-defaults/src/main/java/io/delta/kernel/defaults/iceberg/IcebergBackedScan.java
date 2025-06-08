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
package io.delta.kernel.defaults.iceberg;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

import io.delta.kernel.Scan;
import io.delta.kernel.data.*;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Format;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.iceberg.*;

public class IcebergBackedScan implements Scan {

  private final Table icebergTable;
  private final Snapshot snapshot;
  private final String tableRootPath;
  private final StructType readSchema;

  public IcebergBackedScan(Table icebergTable, Snapshot snapshot, StructType readSchema) {
    this.icebergTable = icebergTable;
    this.snapshot = snapshot;
    this.tableRootPath = icebergTable.location();
    this.readSchema = readSchema;
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
      try (ManifestReader<DataFile> reader = ManifestFiles.read(mf, icebergTable.io())) {
        for (DataFile df : reader) {
          System.out.println("  → DataFile: " + df.path());
          dataFiles.add(df);
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to read manifest: " + mf.path(), e);
      }
    }

    FilteredColumnarBatch result = buildScanFilesBatch(engine, dataFiles, icebergTable.spec());
    System.out.println("Returning batch with rows: " + result.getData().getSize());
    return singletonCloseableIterator(result);
  }

  private FilteredColumnarBatch buildScanFilesBatch(
      Engine engine, List<DataFile> dataFiles, PartitionSpec spec) {
    int numRows = dataFiles.size();

    // Build the data for AddFile structure - these match AddFile.FULL_SCHEMA field order
    List<String> pathList = new ArrayList<>(numRows);
    List<MapValue> partitionValuesList = new ArrayList<>(numRows); // Changed to MapValue
    List<Long> sizeList = new ArrayList<>(numRows);
    List<Long> modificationTimeList = new ArrayList<>(numRows);
    List<Boolean> dataChangeList = new ArrayList<>(numRows);
    List<String> statsList = new ArrayList<>(numRows);
    List<MapValue> tagsList = new ArrayList<>(numRows); // Changed to MapValue

    for (DataFile file : dataFiles) {
      pathList.add(file.path().toString());

      // Build partition values map
      Map<String, String> partitionMap = new HashMap<>();
      StructLike partition = file.partition();
      for (int i = 0; i < spec.fields().size(); i++) {
        String name = spec.fields().get(i).name();
        Object value = partition.get(i, Object.class);
        partitionMap.put(name, value == null ? null : value.toString());
      }
      // Convert Map to MapValue
      partitionValuesList.add(VectorUtils.stringStringMapValue(partitionMap));

      sizeList.add(file.fileSizeInBytes());
      modificationTimeList.add(System.currentTimeMillis());
      dataChangeList.add(true);
      statsList.add(null); // No stats for now
      // Convert Map to MapValue
      tagsList.add(VectorUtils.stringStringMapValue(Collections.emptyMap()));
    }

    // Create column vectors using VectorUtils - following AddFile.FULL_SCHEMA field order
    ColumnVector pathVector = VectorUtils.buildColumnVector(pathList, StringType.STRING);
    ColumnVector partitionValuesVector =
        VectorUtils.buildColumnVector(
            partitionValuesList, new MapType(StringType.STRING, StringType.STRING, true));
    ColumnVector sizeVector = VectorUtils.buildColumnVector(sizeList, LongType.LONG);
    ColumnVector modificationTimeVector =
        VectorUtils.buildColumnVector(modificationTimeList, LongType.LONG);
    ColumnVector dataChangeVector =
        VectorUtils.buildColumnVector(dataChangeList, BooleanType.BOOLEAN);
    ColumnVector statsVector = VectorUtils.buildColumnVector(statsList, StringType.STRING);
    ColumnVector tagsVector =
        VectorUtils.buildColumnVector(
            tagsList, new MapType(StringType.STRING, StringType.STRING, true));

    // Rest of the method remains the same...
    StructType addFileSchema = AddFile.FULL_SCHEMA;
    ColumnVector[] addFileColumns =
        new ColumnVector[] {
          pathVector,
          partitionValuesVector,
          sizeVector,
          modificationTimeVector,
          dataChangeVector,
          statsVector,
          tagsVector
        };

    ColumnVector addFileStructVector =
        VectorUtils.buildColumnVector(
            createStructRowsList(addFileColumns, addFileSchema, numRows), addFileSchema);

    // Create table root path vector
    List<String> tableRootList = Collections.nCopies(numRows, tableRootPath);
    ColumnVector tableRootVector = VectorUtils.buildColumnVector(tableRootList, StringType.STRING);

    // Create the final columnar batch with the expected scan file schema
    ColumnVector[] finalColumns = new ColumnVector[] {addFileStructVector, tableRootVector};

    // Create inline ColumnarBatch implementation
    ColumnarBatch batch =
        new ColumnarBatch() {
          private final ColumnVector[] columns = finalColumns;
          private final StructType schema =
              new StructType().add("add", addFileSchema).add("tableRoot", StringType.STRING);

          @Override
          public StructType getSchema() {
            return schema;
          }

          @Override
          public ColumnVector getColumnVector(int ordinal) {
            if (ordinal < 0 || ordinal >= columns.length) {
              throw new IndexOutOfBoundsException("Invalid ordinal: " + ordinal);
            }
            return columns[ordinal];
          }

          @Override
          public int getSize() {
            return numRows;
          }
        };

    // Create FilteredColumnarBatch with no selection vector (all rows are valid)
    return new FilteredColumnarBatch(batch, Optional.empty());
  }

  private List<Row> createStructRowsList(ColumnVector[] columns, StructType schema, int numRows) {
    List<Row> rows = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      Map<Integer, Object> ordinalToValue = new HashMap<>();
      for (int j = 0; j < columns.length; j++) {
        ColumnVector column = columns[j];
        if (column.isNullAt(i)) {
          ordinalToValue.put(j, null);
        } else {
          ordinalToValue.put(j, getValueFromColumn(column, i));
        }
      }
      rows.add(new io.delta.kernel.internal.data.GenericRow(schema, ordinalToValue));
    }
    return rows;
  }

  private Object getValueFromColumn(ColumnVector column, int rowId) {
    DataType dataType = column.getDataType();
    if (column.isNullAt(rowId)) {
      return null;
    } else if (dataType instanceof StringType) {
      return column.getString(rowId);
    } else if (dataType instanceof LongType) {
      return column.getLong(rowId);
    } else if (dataType instanceof BooleanType) {
      return column.getBoolean(rowId);
    } else if (dataType instanceof MapType) {
      return column.getMap(rowId);
    } else {
      throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  @Override
  public Optional<io.delta.kernel.expressions.Predicate> getRemainingFilter() {
    return Optional.empty();
  }

  @Override
  public Row getScanState(Engine engine) {
    // Create Metadata from Iceberg table
    Metadata metadata = createMetadataFromIcebergTable();

    // Create Protocol - use basic Delta protocol versions
    Protocol protocol = new Protocol(1, 2); // Basic Delta protocol without advanced features

    // Physical equivalent of the logical read schema
    StructType physicalReadSchema =
        readSchema; // For Iceberg, logical = physical (no column mapping)

    // For Iceberg, partition columns are in the files, so physical data read schema = physical read
    // schema
    StructType physicalDataReadSchema = physicalReadSchema;

    return ScanStateRow.of(
        metadata,
        protocol,
        readSchema.toJson(),
        physicalReadSchema.toJson(),
        physicalDataReadSchema.toJson(),
        tableRootPath);
  }

  private Metadata createMetadataFromIcebergTable() {
    // Generate a stable ID based on table location
    String tableId = UUID.nameUUIDFromBytes(tableRootPath.getBytes()).toString();

    // Create partition columns array
    List<String> partitionColumnNames =
        icebergTable.spec().fields().stream()
            .map(PartitionField::name)
            .collect(Collectors.toList());
    ArrayValue partitionColumns =
        VectorUtils.buildArrayValue(partitionColumnNames, StringType.STRING);

    // Use the existing Format class - defaults to "parquet" provider with empty options
    Format format = new Format();

    // Create empty configuration
    MapValue configurationMapValue = VectorUtils.stringStringMapValue(Collections.emptyMap());

    // Get current timestamp for creation time
    long currentTime = System.currentTimeMillis();

    return new Metadata(
        tableId,
        Optional.empty(), // name
        Optional.empty(), // description
        format,
        readSchema.toJson(), // schema string
        readSchema, // parsed schema
        partitionColumns,
        Optional.of(currentTime), // creation time
        configurationMapValue);
  }
}
