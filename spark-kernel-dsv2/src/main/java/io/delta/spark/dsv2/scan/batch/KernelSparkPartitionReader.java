package io.delta.spark.dsv2.scan.batch;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Scan;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Partition reader that reads data from a single Delta file using the Kernel API. Deserializes scan
 * state and file information, then reads the actual data.
 */
public class KernelSparkPartitionReader implements PartitionReader<InternalRow> {

  private static final Logger LOG = LoggerFactory.getLogger(KernelSparkPartitionReader.class);

  private final String serializedScanState;
  private final String serializedScanFileRow;
  private CloseableIterator<InternalRow> dataIterator;
  private boolean initialized = false;
  private final Engine engine;

  public KernelSparkPartitionReader(String serializedScanState, String serializedScanFileRow) {
    this.serializedScanState = requireNonNull(serializedScanState, "serializedScanState is null");
    this.serializedScanFileRow =
        requireNonNull(serializedScanFileRow, "serializedScanFileRow is null");
    this.engine = DefaultEngine.create(new Configuration());
  }

  private void initialize() {
    if (initialized) {
      return;
    }

    try {
      // 1. Deserialize scan state using proper schema
      // Use a simplified scan state schema - in practice, this should come from the actual scan
      // state
      StructType scanStateSchema =
          new StructType()
              .add(
                  "configuration",
                  new io.delta.kernel.types.MapType(
                      io.delta.kernel.types.StringType.STRING,
                      io.delta.kernel.types.StringType.STRING,
                      false))
              .add("logicalSchemaJson", io.delta.kernel.types.StringType.STRING)
              .add("physicalSchemaJson", io.delta.kernel.types.StringType.STRING)
              .add(
                  "partitionColumns",
                  new io.delta.kernel.types.ArrayType(
                      io.delta.kernel.types.StringType.STRING, false))
              .add("minReaderVersion", io.delta.kernel.types.IntegerType.INTEGER)
              .add("minWriterVersion", io.delta.kernel.types.IntegerType.INTEGER)
              .add("tablePath", io.delta.kernel.types.StringType.STRING);
      Row scanStateRow = JsonUtils.rowFromJson(serializedScanState, scanStateSchema);

      // 2. Deserialize file row using SCAN_FILE_SCHEMA
      StructType scanFileSchema = InternalScanFileUtils.SCAN_FILE_SCHEMA;
      Row scanFileRow = JsonUtils.rowFromJson(serializedScanFileRow, scanFileSchema);

      // 3. Get physical data read schema and file status
      StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(scanStateRow);
      FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);

      // 4. Read physical data from Parquet file
      CloseableIterator<FileReadResult> physicalDataIter =
          engine
              .getParquetHandler()
              .readParquetFiles(
                  Utils.singletonCloseableIterator(fileStatus),
                  physicalReadSchema,
                  Optional.empty());

      // 5. Transform physical data to logical data using Kernel API
      CloseableIterator<FilteredColumnarBatch> transformedDataIter =
          Scan.transformPhysicalData(
              engine,
              scanStateRow,
              scanFileRow,
              new CloseableIterator<ColumnarBatch>() {
                @Override
                public boolean hasNext() {
                  return physicalDataIter.hasNext();
                }

                @Override
                public ColumnarBatch next() {
                  return physicalDataIter.next().getData();
                }

                @Override
                public void close() throws IOException {
                  physicalDataIter.close();
                }
              });

      // 6. Convert Kernel FilteredColumnarBatch to Spark InternalRows
      dataIterator = convertToInternalRows(transformedDataIter);

      initialized = true;
    } catch (Exception e) {
      LOG.error("Failed to initialize partition reader", e);
      throw new UncheckedIOException("Failed to initialize partition reader", new IOException(e));
    }
  }

  @Override
  public boolean next() throws IOException {
    initialize();
    return dataIterator.hasNext();
  }

  @Override
  public InternalRow get() {
    if (!initialized) {
      throw new IllegalStateException("Reader not initialized. Call next() first.");
    }
    return dataIterator.next();
  }

  @Override
  public void close() throws IOException {
    if (dataIterator != null) {
      dataIterator.close();
    }
  }

  /** Convert Kernel FilteredColumnarBatch iterator to Spark InternalRow iterator. */
  private CloseableIterator<InternalRow> convertToInternalRows(
      CloseableIterator<FilteredColumnarBatch> batchIterator) {
    return new CloseableIterator<InternalRow>() {
      private CloseableIterator<InternalRow> currentBatchRows;

      @Override
      public boolean hasNext() {
        // Check if current batch has more rows
        if (currentBatchRows != null && currentBatchRows.hasNext()) {
          return true;
        }

        // Try to get next batch
        while (batchIterator.hasNext()) {
          FilteredColumnarBatch batch = batchIterator.next();
          currentBatchRows = convertBatchToRows(batch);
          if (currentBatchRows.hasNext()) {
            return true;
          }
        }

        return false;
      }

      @Override
      public InternalRow next() {
        if (!hasNext()) {
          throw new RuntimeException("No more rows available");
        }
        return currentBatchRows.next();
      }

      @Override
      public void close() throws IOException {
        if (currentBatchRows != null) {
          currentBatchRows.close();
        }
        batchIterator.close();
      }
    };
  }

  /** Convert a single FilteredColumnarBatch to an iterator of InternalRows. */
  private CloseableIterator<InternalRow> convertBatchToRows(FilteredColumnarBatch batch) {
    ColumnarBatch data = batch.getData();
    Optional<ColumnVector> selectionVector = batch.getSelectionVector();
    StructType schema = data.getSchema();
    int numRows = data.getSize();
    int numColumns = schema.length();

    return new CloseableIterator<InternalRow>() {
      private int currentRow = 0;

      @Override
      public boolean hasNext() {
        // Find next valid row based on selection vector
        while (currentRow < numRows) {
          if (!selectionVector.isPresent()
              || (!selectionVector.get().isNullAt(currentRow)
                  && selectionVector.get().getBoolean(currentRow))) {
            return true;
          }
          currentRow++;
        }
        return false;
      }

      @Override
      public InternalRow next() {
        if (!hasNext()) {
          throw new RuntimeException("No more rows available");
        }

        // Convert current row to InternalRow
        Object[] values = new Object[numColumns];
        for (int col = 0; col < numColumns; col++) {
          ColumnVector columnVector = data.getColumnVector(col);
          values[col] = extractValue(columnVector, currentRow, schema.at(col).getDataType());
        }

        currentRow++;
        return new GenericInternalRow(values);
      }

      @Override
      public void close() throws IOException {
        // No resources to close for row iterator
      }
    };
  }

  /** Extract value from Kernel ColumnVector and convert to Spark-compatible type. */
  private Object extractValue(
      ColumnVector columnVector, int rowIndex, io.delta.kernel.types.DataType dataType) {
    if (columnVector.isNullAt(rowIndex)) {
      return null;
    }

    if (dataType instanceof io.delta.kernel.types.BooleanType) {
      return columnVector.getBoolean(rowIndex);
    } else if (dataType instanceof io.delta.kernel.types.ByteType) {
      return columnVector.getByte(rowIndex);
    } else if (dataType instanceof io.delta.kernel.types.ShortType) {
      return columnVector.getShort(rowIndex);
    } else if (dataType instanceof io.delta.kernel.types.IntegerType) {
      return columnVector.getInt(rowIndex);
    } else if (dataType instanceof io.delta.kernel.types.LongType) {
      return columnVector.getLong(rowIndex);
    } else if (dataType instanceof io.delta.kernel.types.FloatType) {
      return columnVector.getFloat(rowIndex);
    } else if (dataType instanceof io.delta.kernel.types.DoubleType) {
      return columnVector.getDouble(rowIndex);
    } else if (dataType instanceof io.delta.kernel.types.StringType) {
      return UTF8String.fromString(columnVector.getString(rowIndex));
    } else if (dataType instanceof io.delta.kernel.types.DateType) {
      return columnVector.getInt(rowIndex); // Date as days since epoch
    } else if (dataType instanceof io.delta.kernel.types.TimestampType) {
      return columnVector.getLong(rowIndex); // Timestamp as microseconds since epoch
    } else if (dataType instanceof io.delta.kernel.types.DecimalType) {
      return columnVector.getDecimal(rowIndex);
    } else if (dataType instanceof io.delta.kernel.types.BinaryType) {
      return columnVector.getBinary(rowIndex);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported data type for conversion: " + dataType.getClass().getSimpleName());
    }
  }
}
