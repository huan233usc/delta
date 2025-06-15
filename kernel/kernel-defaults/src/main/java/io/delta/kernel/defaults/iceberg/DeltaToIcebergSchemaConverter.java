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

import io.delta.kernel.types.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** Utility class to convert Delta Kernel schemas to Iceberg schemas. */
public class DeltaToIcebergSchemaConverter {

  /**
   * Converts a Delta Kernel schema to an Iceberg schema.
   *
   * @param deltaSchema The Delta schema to convert
   * @return An equivalent Iceberg schema
   */
  public static Schema toIcebergSchema(StructType deltaSchema) {
    AtomicInteger fieldIdCounter = new AtomicInteger(1);
    List<Types.NestedField> fields =
        deltaSchema.fields().stream()
            .map(field -> convertField(field, fieldIdCounter))
            .collect(Collectors.toList());

    return new Schema(fields);
  }

  /**
   * Converts a Delta StructField to an Iceberg NestedField.
   *
   * @param deltaField The Delta field to convert
   * @param fieldIdCounter Counter for assigning field IDs
   * @return The equivalent Iceberg NestedField
   */
  private static Types.NestedField convertField(
      StructField deltaField, AtomicInteger fieldIdCounter) {
    // Try to extract existing field ID from Delta metadata if available
    int fieldId = extractFieldId(deltaField, fieldIdCounter);

    Type icebergType = convertType(deltaField.getDataType(), fieldIdCounter);

    if (deltaField.isNullable()) {
      return Types.NestedField.optional(fieldId, deltaField.getName(), icebergType);
    } else {
      return Types.NestedField.required(fieldId, deltaField.getName(), icebergType);
    }
  }

  /** Extracts field ID from Delta field metadata, or assigns a new one. */
  private static int extractFieldId(StructField deltaField, AtomicInteger fieldIdCounter) {
    // Check if Delta field metadata contains column mapping ID
    if (deltaField.getMetadata() != null) {
      try {
        // Look for Delta column mapping ID in metadata
        Object fieldIdObj = deltaField.getMetadata().get("delta.columnMapping.id");
        if (fieldIdObj instanceof Number) {
          return ((Number) fieldIdObj).intValue();
        }
      } catch (Exception e) {
        // If extraction fails, fall back to auto-generated ID
      }
    }

    // Auto-assign field ID if not found in metadata
    return fieldIdCounter.getAndIncrement();
  }

  /**
   * Recursively converts Delta DataType to Iceberg Type.
   *
   * @param deltaType The Delta type to convert
   * @param fieldIdCounter Counter for assigning field IDs to nested fields
   * @return The equivalent Iceberg type
   */
  private static Type convertType(DataType deltaType, AtomicInteger fieldIdCounter) {
    if (deltaType == null) {
      throw new IllegalArgumentException("Delta type cannot be null");
    }

    if (deltaType instanceof BooleanType) {
      return Types.BooleanType.get();
    } else if (deltaType instanceof IntegerType) {
      return Types.IntegerType.get();
    } else if (deltaType instanceof LongType) {
      return Types.LongType.get();
    } else if (deltaType instanceof FloatType) {
      return Types.FloatType.get();
    } else if (deltaType instanceof DoubleType) {
      return Types.DoubleType.get();
    } else if (deltaType instanceof StringType) {
      return Types.StringType.get();
    } else if (deltaType instanceof BinaryType) {
      return Types.BinaryType.get();
    } else if (deltaType instanceof DateType) {
      return Types.DateType.get();
    } else if (deltaType instanceof TimestampType) {
      return Types.TimestampType.withZone();
    } else if (deltaType instanceof TimestampNTZType) {
      return Types.TimestampType.withoutZone();
    } else if (deltaType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) deltaType;
      return Types.DecimalType.of(decimalType.getPrecision(), decimalType.getScale());
    } else if (deltaType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) deltaType;
      Type elementType = convertType(arrayType.getElementType(), fieldIdCounter);
      int elementId = fieldIdCounter.getAndIncrement();

      if (arrayType.containsNull()) {
        return Types.ListType.ofOptional(elementId, elementType);
      } else {
        return Types.ListType.ofRequired(elementId, elementType);
      }
    } else if (deltaType instanceof MapType) {
      MapType mapType = (MapType) deltaType;
      Type keyType = convertType(mapType.getKeyType(), fieldIdCounter);
      Type valueType = convertType(mapType.getValueType(), fieldIdCounter);
      int keyId = fieldIdCounter.getAndIncrement();
      int valueId = fieldIdCounter.getAndIncrement();

      if (mapType.isValueContainsNull()) {
        return Types.MapType.ofOptional(keyId, valueId, keyType, valueType);
      } else {
        return Types.MapType.ofRequired(keyId, valueId, keyType, valueType);
      }
    } else if (deltaType instanceof StructType) {
      StructType structType = (StructType) deltaType;
      List<Types.NestedField> fields =
          structType.fields().stream()
              .map(field -> convertField(field, fieldIdCounter))
              .collect(Collectors.toList());
      return Types.StructType.of(fields);
    }

    throw new UnsupportedOperationException(
        "Unsupported Delta type: " + deltaType.getClass().getSimpleName());
  }

  /**
   * Converts a Delta Kernel schema to an Iceberg schema with explicit field ID management. This
   * version allows for custom field ID assignment strategy.
   *
   * @param deltaSchema The Delta schema to convert
   * @param startingFieldId The starting field ID to use
   * @return An equivalent Iceberg schema
   */
  public static Schema toIcebergSchema(StructType deltaSchema, int startingFieldId) {
    AtomicInteger fieldIdCounter = new AtomicInteger(startingFieldId);
    List<Types.NestedField> fields =
        deltaSchema.fields().stream()
            .map(field -> convertField(field, fieldIdCounter))
            .collect(Collectors.toList());

    return new Schema(fields);
  }

  /**
   * Converts a Delta Kernel schema to an Iceberg schema while preserving existing field IDs and
   * only assigning new IDs to fields that don't have them.
   *
   * @param deltaSchema The Delta schema to convert
   * @param existingSchema Optional existing Iceberg schema to preserve field IDs from
   * @return An equivalent Iceberg schema with preserved field IDs where possible
   */
  public static Schema toIcebergSchemaWithPreservedIds(
      StructType deltaSchema, Schema existingSchema) {
    // Create a field ID mapping from existing schema
    int maxExistingId = existingSchema != null ? findMaxFieldId(existingSchema) : 0;
    AtomicInteger fieldIdCounter = new AtomicInteger(maxExistingId + 1);

    List<Types.NestedField> fields =
        deltaSchema.fields().stream()
            .map(field -> convertFieldWithPreservedId(field, existingSchema, fieldIdCounter))
            .collect(Collectors.toList());

    return new Schema(fields);
  }

  private static Types.NestedField convertFieldWithPreservedId(
      StructField deltaField, Schema existingSchema, AtomicInteger fieldIdCounter) {

    // Try to find field ID from existing schema
    int fieldId =
        findExistingFieldId(deltaField.getName(), existingSchema)
            .orElse(extractFieldId(deltaField, fieldIdCounter));

    Type icebergType = convertType(deltaField.getDataType(), fieldIdCounter);

    if (deltaField.isNullable()) {
      return Types.NestedField.optional(fieldId, deltaField.getName(), icebergType);
    } else {
      return Types.NestedField.required(fieldId, deltaField.getName(), icebergType);
    }
  }

  private static java.util.Optional<Integer> findExistingFieldId(
      String fieldName, Schema existingSchema) {
    if (existingSchema == null) {
      return java.util.Optional.empty();
    }

    try {
      Types.NestedField field = existingSchema.findField(fieldName);
      return field != null ? java.util.Optional.of(field.fieldId()) : java.util.Optional.empty();
    } catch (IllegalArgumentException e) {
      return java.util.Optional.empty();
    }
  }

  private static int findMaxFieldId(Schema schema) {
    return schema.columns().stream().mapToInt(Types.NestedField::fieldId).max().orElse(0);
  }
}
