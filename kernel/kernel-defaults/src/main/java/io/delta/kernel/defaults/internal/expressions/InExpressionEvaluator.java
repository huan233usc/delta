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
package io.delta.kernel.defaults.internal.expressions;

import static io.delta.kernel.defaults.internal.DefaultEngineErrors.unsupportedExpressionException;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.*;
import java.util.List;
import java.util.Optional;

/** Utility methods to evaluate {@code IN} expression. */
public class InExpressionEvaluator {
  private InExpressionEvaluator() {}

  /**
   * Validates and transforms the {@code IN} expression.
   *
   * @param in The IN predicate expression
   * @param childrenExpressions List of child expressions (first is the value, rest are IN list)
   * @param childrenOutputTypes List of output types for each child expression
   * @return Transformed IN predicate
   */
  static Predicate validateAndTransform(
      Predicate in, List<Expression> childrenExpressions, List<DataType> childrenOutputTypes) {
    if (childrenExpressions.size() < 2) {
      throw unsupportedExpressionException(
          in,
          "IN expression requires at least 2 arguments: value and "
              + "at least one element in the list. "
              + "Example usage: column IN (value1, value2, ...)");
    }

    DataType valueType = childrenOutputTypes.get(0);

    // Validate all IN list elements have compatible types with the value expression
    for (int i = 1; i < childrenOutputTypes.size(); i++) {
      DataType listElementType = childrenOutputTypes.get(i);
      if (!areTypesCompatible(valueType, listElementType)) {
        throw unsupportedExpressionException(
            in,
            String.format(
                "IN expression requires all elements to have compatible types. "
                    + "Value type: %s, incompatible element type at position %d: %s",
                valueType, i, listElementType));
      }
    }

    return new Predicate(in.getName(), childrenExpressions);
  }

  /**
   * Evaluates the IN expression on the given column vectors.
   *
   * @param childrenVectors List of column vectors (first is value, rest are IN list elements)
   * @return ColumnVector containing boolean results of IN evaluation
   */
  static ColumnVector eval(List<ColumnVector> childrenVectors) {
    return new ColumnVector() {
      final ColumnVector valueVector = childrenVectors.get(0);
      final List<ColumnVector> inListVectors = childrenVectors.subList(1, childrenVectors.size());

      @Override
      public DataType getDataType() {
        return BooleanType.BOOLEAN;
      }

      @Override
      public int getSize() {
        return valueVector.getSize();
      }

      @Override
      public void close() {
        Utils.closeCloseables(valueVector);
        inListVectors.forEach(Utils::closeCloseables);
      }

      @Override
      public boolean getBoolean(int rowId) {
        if (isNullAt(rowId)) {
          // Return value is undefined when null, but we return false for consistency
          return false;
        }
        return evaluateInLogic(rowId).orElse(false);
      }

      @Override
      public boolean isNullAt(int rowId) {
        Optional<Boolean> result = evaluateInLogic(rowId);
        return !result.isPresent();
      }

      /**
       * Implements the IN expression logic following SQL NULL semantics: - TRUE if the non-NULL
       * value is found in the list - FALSE if the non-NULL value is not found in the list and the
       * list does not contain NULL values - NULL if the value is NULL, or the non-NULL value is not
       * found in the list and the list contains at least one NULL value
       *
       * @param rowId Row index to evaluate
       * @return Optional<Boolean> where empty means NULL, true means TRUE, false means FALSE
       */
      private Optional<Boolean> evaluateInLogic(int rowId) {
        // If the value is NULL, return NULL
        if (valueVector.isNullAt(rowId)) {
          return Optional.empty();
        }

        boolean foundNull = false;
        Object valueToFind = getValue(valueVector, rowId);

        // Check each element in the IN list
        for (ColumnVector inListVector : inListVectors) {
          if (inListVector.isNullAt(rowId)) {
            foundNull = true;
          } else {
            Object inListValue = getValue(inListVector, rowId);
            if (compareValues(valueToFind, inListValue, valueVector.getDataType())) {
              return Optional.of(true); // Found match, return TRUE
            }
          }
        }

        // Value not found in list
        if (foundNull) {
          return Optional.empty(); // Return NULL if list contained NULL values
        } else {
          return Optional.of(false); // Return FALSE if no NULL values in list
        }
      }
    };
  }

  /** Gets value from column vector based on data type. */
  private static Object getValue(ColumnVector vector, int rowId) {
    DataType dataType = vector.getDataType();
    if (dataType instanceof BooleanType) {
      return vector.getBoolean(rowId);
    } else if (dataType instanceof ByteType) {
      return vector.getByte(rowId);
    } else if (dataType instanceof ShortType) {
      return vector.getShort(rowId);
    } else if (dataType instanceof IntegerType) {
      return vector.getInt(rowId);
    } else if (dataType instanceof LongType) {
      return vector.getLong(rowId);
    } else if (dataType instanceof FloatType) {
      return vector.getFloat(rowId);
    } else if (dataType instanceof DoubleType) {
      return vector.getDouble(rowId);
    } else if (dataType instanceof StringType) {
      return vector.getString(rowId);
    } else if (dataType instanceof BinaryType) {
      return vector.getBinary(rowId);
    } else if (dataType instanceof DecimalType) {
      return vector.getDecimal(rowId);
    } else if (dataType instanceof DateType) {
      return vector.getInt(rowId); // Date is stored as int (days since epoch)
    } else if (dataType instanceof TimestampType || dataType instanceof TimestampNTZType) {
      return vector.getLong(rowId); // Timestamp is stored as long (microseconds since epoch)
    } else {
      throw new UnsupportedOperationException(
          "IN expression does not support data type: " + dataType);
    }
  }

  /** Compares two values for equality considering their data type. */
  private static boolean compareValues(Object value1, Object value2, DataType dataType) {
    if (value1 == null || value2 == null) {
      return false; // NULL values should be handled before calling this method
    }

    // Handle numeric type comparisons - convert to common type for comparison
    if (value1 instanceof Number && value2 instanceof Number) {
      Number num1 = (Number) value1;
      Number num2 = (Number) value2;

      // For integer types, compare as long
      if (isIntegerType(value1) && isIntegerType(value2)) {
        return num1.longValue() == num2.longValue();
      }

      // For floating point types or mixed types, compare as double
      if (isFloatingPointType(value1) || isFloatingPointType(value2)) {
        return Double.compare(num1.doubleValue(), num2.doubleValue()) == 0;
      }

      // Fallback to long comparison for other numeric types
      return num1.longValue() == num2.longValue();
    }

    if (dataType instanceof FloatType || dataType instanceof DoubleType) {
      // Handle floating point comparison with tolerance for NaN
      if (value1 instanceof Float && value2 instanceof Float) {
        Float f1 = (Float) value1;
        Float f2 = (Float) value2;
        return Float.compare(f1, f2) == 0;
      } else if (value1 instanceof Double && value2 instanceof Double) {
        Double d1 = (Double) value1;
        Double d2 = (Double) value2;
        return Double.compare(d1, d2) == 0;
      }
    }

    return value1.equals(value2);
  }

  private static boolean isIntegerType(Object value) {
    return value instanceof Byte
        || value instanceof Short
        || value instanceof Integer
        || value instanceof Long;
  }

  private static boolean isFloatingPointType(Object value) {
    return value instanceof Float || value instanceof Double;
  }

  /** Checks if two data types are compatible for IN expression comparison. */
  private static boolean areTypesCompatible(DataType type1, DataType type2) {
    // Same types are always compatible
    if (type1.equivalent(type2)) {
      return true;
    }

    // Numeric types are compatible with each other
    if (isNumericType(type1) && isNumericType(type2)) {
      return true;
    }

    // Timestamp types are compatible with each other
    if (isTimestampType(type1) && isTimestampType(type2)) {
      return true;
    }

    return false;
  }

  private static boolean isNumericType(DataType dataType) {
    return dataType instanceof ByteType
        || dataType instanceof ShortType
        || dataType instanceof IntegerType
        || dataType instanceof LongType
        || dataType instanceof FloatType
        || dataType instanceof DoubleType
        || dataType instanceof DecimalType;
  }

  private static boolean isTimestampType(DataType dataType) {
    return dataType instanceof TimestampType || dataType instanceof TimestampNTZType;
  }
}
