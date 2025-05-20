package io.delta.kernel.defaults.iceberg;

import io.delta.kernel.types.*;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to convert Iceberg schemas to Delta Kernel schemas.
 */
public class IcebergToDeltaSchemaConverter {

    /**
     * Converts an Iceberg schema to a Delta Kernel schema.
     *
     * @param icebergSchema The Iceberg schema to convert
     * @return An equivalent Delta Kernel schema
     */
    public static io.delta.kernel.types.StructType toKernelSchema(Schema icebergSchema) {
        return (io.delta.kernel.types.StructType) convertType(icebergSchema.asStruct());
    }

    /**
     * Recursively converts Iceberg types to Delta Kernel types.
     *
     * @param icebergType The Iceberg type to convert
     * @return The equivalent Delta Kernel type
     */
    private static DataType convertType(Type icebergType) {
        if (icebergType == null) {
            return null;
        }

        if (icebergType instanceof Types.BooleanType) {
            return BooleanType.BOOLEAN;
        } else if (icebergType instanceof Types.IntegerType) {
            return IntegerType.INTEGER;
        } else if (icebergType instanceof Types.LongType) {
            return LongType.LONG;
        } else if (icebergType instanceof Types.FloatType) {
            return FloatType.FLOAT;
        } else if (icebergType instanceof Types.DoubleType) {
            return DoubleType.DOUBLE;
        } else if (icebergType instanceof Types.DateType) {
            return DateType.DATE;
        } else if (icebergType instanceof Types.TimeType) {
            throw new UnsupportedOperationException("Time type is not supported");
        } else if (icebergType instanceof Types.TimestampType) {
            Types.TimestampType tsType = (Types.TimestampType) icebergType;
            if (tsType.shouldAdjustToUTC()) {
                return TimestampType.TIMESTAMP;
            } else {
                return TimestampNTZType.TIMESTAMP_NTZ;
            }
        } else if (icebergType instanceof Types.StringType) {
            return StringType.STRING;
        } else if (icebergType instanceof Types.BinaryType) {
            return BinaryType.BINARY;
        } else if (icebergType instanceof Types.DecimalType) {
            Types.DecimalType decimalType = (Types.DecimalType) icebergType;
            return new DecimalType(decimalType.precision(), decimalType.scale());
        } else if (icebergType instanceof Types.ListType) {
            Types.ListType listType = (Types.ListType) icebergType;
            return new ArrayType(
                    convertType(listType.elementType()),
                    listType.isElementOptional()
            );
        } else if (icebergType instanceof Types.MapType) {
            Types.MapType mapType = (Types.MapType) icebergType;
            return new MapType(
                    convertType(mapType.keyType()),
                    convertType(mapType.valueType()),
                    mapType.isValueOptional()
            );
        } else if (icebergType instanceof Types.StructType) {
            Types.StructType structType = (Types.StructType) icebergType;
            io.delta.kernel.types.StructType deltaStructType = new io.delta.kernel.types.StructType();

            for (Types.NestedField field : structType.fields()) {
                // Convert field metadata if necessary
                FieldMetadata.Builder fieldMetadata = new FieldMetadata.Builder();

                // Add field ID to metadata
                fieldMetadata.putLong("delta.columnMapping.id", field.fieldId());

                deltaStructType = deltaStructType.add(
                        field.name(),
                        convertType(field.type()),
                        !field.isRequired(),  // In Iceberg, isRequired() means non-nullable
                        fieldMetadata.build()
                );
            }

            return deltaStructType;
        }

        // For any unhandled types, fall back to string
        return StringType.STRING;
    }
}