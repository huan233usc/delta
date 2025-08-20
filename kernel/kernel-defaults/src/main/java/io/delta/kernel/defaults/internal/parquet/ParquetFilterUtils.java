/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.internal.parquet;

import static io.delta.kernel.internal.util.ExpressionUtils.*;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;

import io.delta.kernel.expressions.*;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.util.*;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.filter2.predicate.*;
import org.apache.parquet.filter2.predicate.Operators.*;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.LogicalTypeAnnotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities to convert the Kernel {@link Predicate} into `parquet-mr` {@link FilterPredicate}. */
public class ParquetFilterUtils {
  private static final Logger logger = LoggerFactory.getLogger(ParquetFilterUtils.class);

  private ParquetFilterUtils() {}

  /**
   * Convert the given Kernel predicate {@code kernelPredicate} into `parquet-mr` predicate.
   *
   * @param parquetFileSchema Schema of the Parquet file. We need it to find what columns exists in
   *     the Parquet file in order to remove predicates on columns that do not exist in the file.
   *     There is no clear way to handle the predicate on columns that don't exist in the Parquet
   *     file.
   * @param kernelPredicate Kernel predicate to convert.
   * @return instance of {@link Filter} (`parquet-mr` filter)
   */
  public static Optional<FilterPredicate> toParquetFilter(
      MessageType parquetFileSchema, Predicate kernelPredicate) {
    // Construct a map of field names to field metadata objects
    Map<Column, ParquetField> parquetFieldMap = extractParquetFields(parquetFileSchema);
    return convertToParquetFilter(parquetFieldMap, kernelPredicate);
  }

  private static class ParquetField {
    final LogicalTypeAnnotation logicalType;
    final PrimitiveType primitiveType;

    private ParquetField(LogicalTypeAnnotation logicalType, PrimitiveType primitiveType) {
      this.logicalType = logicalType;
      this.primitiveType = primitiveType;
    }

    static ParquetField of(LogicalTypeAnnotation logicalType, PrimitiveType primitiveType) {
      return new ParquetField(logicalType, primitiveType);
    }
  }

  /**
   * Create a mapping of column to ParquetField for each non-repeated leaf-level column in the given
   * parquet schema.
   *
   * @param parquetSchema Schema of the Parquet file
   * @return Mapping of column to ParquetField
   */
  private static Map<Column, ParquetField> extractParquetFields(MessageType parquetSchema) {
    Map<Column, ParquetField> parquetFields = new HashMap<>();
    for (ColumnDescriptor columnDescriptor : parquetSchema.getColumns()) {
      String[] columnPath = columnDescriptor.getPath();
      Type type = parquetSchema.getType(columnPath);
      if (type.getRepetition() == Type.Repetition.REPEATED) {
        // `parquet-mr` doesn't support applying filter on a repeated column
        continue;
      }
      assert type.isPrimitive() : "Only primitive types are expected from .getColumns()";
      PrimitiveType primitiveType = type.asPrimitiveType();
      parquetFields.put(
          new Column(columnPath), ParquetField.of(type.getLogicalTypeAnnotation(), primitiveType));
    }
    return parquetFields;
  }

  private static boolean canUseLiteral(Literal literal, PrimitiveType parquetType) {
    DataType litType = literal.getDataType();
    LogicalTypeAnnotation logicalType = parquetType.getLogicalTypeAnnotation();
    // Debug output
    System.out.println(
        "DEBUG: canUseLiteral called - litType="
            + litType.getClass().getSimpleName()
            + ", parquetType="
            + parquetType.getPrimitiveTypeName()
            + ", logicalType="
            + (logicalType != null ? logicalType.getClass().getSimpleName() : "null")
            + ", literal value="
            + literal.getValue());
    switch (parquetType.getPrimitiveTypeName()) {
      case BOOLEAN:
        return litType instanceof BooleanType;
      case INT32:
        // Handle decimal columns stored as INT32
        if (logicalType instanceof DecimalLogicalTypeAnnotation && isDecimal(literal)) {
          return true;
        }
        // Handle regular integer columns
        if (!isInteger(literal)) {
          return false;
        }
        return logicalType == null
            || // no logical type when the type is int32 or int64
            (logicalType instanceof IntLogicalTypeAnnotation
                && ((IntLogicalTypeAnnotation) logicalType).getBitWidth() <= 32)
            || logicalType instanceof DateLogicalTypeAnnotation;
      case INT64:
        // Handle decimal columns stored as INT64
        if (logicalType instanceof DecimalLogicalTypeAnnotation && isDecimal(literal)) {
          return true;
        }
        // Handle regular long columns
        if (!isLong(literal)) {
          return false;
        }
        return logicalType == null
            || // no logical type when the type is int32 or int64
            (logicalType instanceof IntLogicalTypeAnnotation
                && ((IntLogicalTypeAnnotation) logicalType).getBitWidth() <= 64);
      case FLOAT:
        return isFloat(literal);
      case DOUBLE:
        return isDouble(literal);
      case BINARY:
        {
          return isBinary(literal)
                  &&
                  // logical type should be binary (null) or string
                  (logicalType == null || logicalType instanceof StringLogicalTypeAnnotation)
              || (logicalType instanceof DecimalLogicalTypeAnnotation && isDecimal(literal));
        }
      case FIXED_LEN_BYTE_ARRAY:
        return logicalType instanceof DecimalLogicalTypeAnnotation && isDecimal(literal);
      default:
        return false;
    }
  }

  private static Optional<FilterPredicate> convertToParquetFilter(
      Map<Column, ParquetField> parquetFieldMap, Predicate deltaPredicate) {
    String name = deltaPredicate.getName().toLowerCase(Locale.ROOT);
    switch (name) {
      case "=":
      case "<":
      case "<=":
      case ">":
      case ">=":
        return convertComparatorToParquetFilter(parquetFieldMap, deltaPredicate);
      case "not":
        return convertNotToParquetFilter(parquetFieldMap, deltaPredicate);
      case "and":
        return convertAndToParquetFilter(parquetFieldMap, deltaPredicate);
      case "or":
        return convertOrToParquetFilter(parquetFieldMap, deltaPredicate);
      case "is_null":
        return convertIsNullIsNotNull(parquetFieldMap, deltaPredicate, false /* isNotNull */);
      case "is_not_null":
        return convertIsNullIsNotNull(parquetFieldMap, deltaPredicate, true /* isNotNull */);
      case "in":
        return convertInToParquetFilter(parquetFieldMap, deltaPredicate);
      default:
        return visitUnsupported(deltaPredicate, name + " is not a supported predicate.");
    }
  }

  private static Optional<FilterPredicate> convertComparatorToParquetFilter(
      Map<Column, ParquetField> parquetFieldMap, Predicate deltaPredicate) {
    Expression child0 = getLeft(deltaPredicate);
    Expression child1 = getRight(deltaPredicate);

    if (child0 instanceof Literal && child1 instanceof Column) {
      Expression temp = child0;
      child0 = child1;
      child1 = temp;
    }

    if (!(child0 instanceof Column) || !(child1 instanceof Literal)) {
      return visitUnsupported(
          deltaPredicate, "Comparison predicate must have a column and a literal.");
    }

    Column column = (Column) child0;
    Literal literal = (Literal) child1;

    ParquetField parquetField = parquetFieldMap.get(column);
    if (parquetField == null) {
      return visitUnsupported(
          deltaPredicate, "Column used in predicate does not exist in the parquet file.");
    }

    if (literal.getValue() == null) {
      return visitUnsupported(
          deltaPredicate,
          "Literal value is null for a comparator operator. Comparator is not "
              + "supported for null values as the Parquet comparator is not null safe");
    }

    if (!canUseLiteral(literal, parquetField.primitiveType)) {
      return visitUnsupported(
          deltaPredicate,
          "Literal type is not compatible with the column type: " + literal.getDataType());
    }

    PrimitiveType parquetType = parquetField.primitiveType;
    String columnPath = ColumnPath.get(column.getNames()).toDotString();
    String comparator = deltaPredicate.getName();

    switch (parquetType.getPrimitiveTypeName()) {
      case BOOLEAN:
        BooleanColumn booleanColumn = booleanColumn(columnPath);
        if ("=".equals(comparator)) { // Only = is supported for boolean
          return Optional.of(FilterApi.eq(booleanColumn, getBoolean(literal)));
        }
        break;
      case INT32:
        // Handle decimal columns stored as INT32
        if (parquetField.logicalType instanceof DecimalLogicalTypeAnnotation
            && isDecimal(literal)) {
          return convertDecimalComparator(
              columnPath,
              literal,
              comparator,
              parquetType,
              (DecimalLogicalTypeAnnotation) parquetField.logicalType);
        } else {
          // Handle regular integer columns
          IntColumn intColumn = intColumn(columnPath);
          switch (comparator) {
            case "=":
              return Optional.of(FilterApi.eq(intColumn, getInt(literal)));
            case "<":
              return Optional.of(FilterApi.lt(intColumn, getInt(literal)));
            case "<=":
              return Optional.of(FilterApi.ltEq(intColumn, getInt(literal)));
            case ">":
              return Optional.of(FilterApi.gt(intColumn, getInt(literal)));
            case ">=":
              return Optional.of(FilterApi.gtEq(intColumn, getInt(literal)));
          }
        }
        break;
      case INT64:
        // Handle decimal columns stored as INT64
        if (parquetField.logicalType instanceof DecimalLogicalTypeAnnotation
            && isDecimal(literal)) {
          return convertDecimalComparator(
              columnPath,
              literal,
              comparator,
              parquetType,
              (DecimalLogicalTypeAnnotation) parquetField.logicalType);
        } else {
          // Handle regular long columns
          LongColumn longColumn = longColumn(columnPath);
          switch (comparator) {
            case "=":
              return Optional.of(FilterApi.eq(longColumn, getLong(literal)));
            case "<":
              return Optional.of(FilterApi.lt(longColumn, getLong(literal)));
            case "<=":
              return Optional.of(FilterApi.ltEq(longColumn, getLong(literal)));
            case ">":
              return Optional.of(FilterApi.gt(longColumn, getLong(literal)));
            case ">=":
              return Optional.of(FilterApi.gtEq(longColumn, getLong(literal)));
          }
        }
        break;
      case FLOAT:
        FloatColumn floatColumn = floatColumn(columnPath);
        switch (comparator) {
          case "=":
            return Optional.of(FilterApi.eq(floatColumn, getFloat(literal)));
          case "<":
            return Optional.of(FilterApi.lt(floatColumn, getFloat(literal)));
          case "<=":
            return Optional.of(FilterApi.ltEq(floatColumn, getFloat(literal)));
          case ">":
            return Optional.of(FilterApi.gt(floatColumn, getFloat(literal)));
          case ">=":
            return Optional.of(FilterApi.gtEq(floatColumn, getFloat(literal)));
        }
        break;
      case DOUBLE:
        DoubleColumn doubleColumn = doubleColumn(columnPath);
        switch (comparator) {
          case "=":
            return Optional.of(FilterApi.eq(doubleColumn, getDouble(literal)));
          case "<":
            return Optional.of(FilterApi.lt(doubleColumn, getDouble(literal)));
          case "<=":
            return Optional.of(FilterApi.ltEq(doubleColumn, getDouble(literal)));
          case ">":
            return Optional.of(FilterApi.gt(doubleColumn, getDouble(literal)));
          case ">=":
            return Optional.of(FilterApi.gtEq(doubleColumn, getDouble(literal)));
        }
        break;
      case BINARY:
        // Handle decimal columns stored as BINARY
        if (parquetField.logicalType instanceof DecimalLogicalTypeAnnotation
            && isDecimal(literal)) {
          return convertDecimalComparator(
              columnPath,
              literal,
              comparator,
              parquetType,
              (DecimalLogicalTypeAnnotation) parquetField.logicalType);
        } else {
          // Handle regular binary/string columns
          BinaryColumn binaryColumn = binaryColumn(columnPath);
          Binary binary = getBinary(literal);
          switch (comparator) {
            case "=":
              return Optional.of(FilterApi.eq(binaryColumn, binary));
            case "<":
              return Optional.of(FilterApi.lt(binaryColumn, binary));
            case "<=":
              return Optional.of(FilterApi.ltEq(binaryColumn, binary));
            case ">":
              return Optional.of(FilterApi.gt(binaryColumn, binary));
            case ">=":
              return Optional.of(FilterApi.gtEq(binaryColumn, binary));
          }
        }
        break;
      case FIXED_LEN_BYTE_ARRAY:
        // Decimal columns stored as FIXED_LEN_BYTE_ARRAY
        if (parquetField.logicalType instanceof DecimalLogicalTypeAnnotation
            && isDecimal(literal)) {
          return convertDecimalComparator(
              columnPath,
              literal,
              comparator,
              parquetType,
              (DecimalLogicalTypeAnnotation) parquetField.logicalType);
        }
        break;
    }
    return visitUnsupported(
        deltaPredicate,
        String.format(
            "Unsupported column type (%s) with comparator (%s): ", parquetType, comparator));
  }

  private static Optional<FilterPredicate> convertNotToParquetFilter(
      Map<Column, ParquetField> parquetFieldMap, Predicate deltaPredicate) {
    Optional<FilterPredicate> childFilter =
        convertToParquetFilter(parquetFieldMap, (Predicate) getUnaryChild(deltaPredicate));

    return childFilter.map(FilterApi::not);
  }

  private static Optional<FilterPredicate> convertOrToParquetFilter(
      Map<Column, ParquetField> parquetFieldMap, Predicate deltaPredicate) {
    Optional<FilterPredicate> leftFilter =
        convertToParquetFilter(parquetFieldMap, asPredicate(getLeft(deltaPredicate)));
    Optional<FilterPredicate> rightFilter =
        convertToParquetFilter(parquetFieldMap, asPredicate(getRight(deltaPredicate)));

    if (leftFilter.isPresent() && rightFilter.isPresent()) {
      return Optional.of(FilterApi.or(leftFilter.get(), rightFilter.get()));
    }
    return Optional.empty();
  }

  private static Optional<FilterPredicate> convertAndToParquetFilter(
      Map<Column, ParquetField> parquetFieldMap, Predicate deltaPredicate) {
    Optional<FilterPredicate> leftFilter =
        convertToParquetFilter(parquetFieldMap, asPredicate(getLeft(deltaPredicate)));
    Optional<FilterPredicate> rightFilter =
        convertToParquetFilter(parquetFieldMap, asPredicate(getRight(deltaPredicate)));

    if (leftFilter.isPresent() && rightFilter.isPresent()) {
      return Optional.of(FilterApi.and(leftFilter.get(), rightFilter.get()));
    }
    if (leftFilter.isPresent()) {
      return leftFilter;
    }
    return rightFilter;
  }

  private static Optional<FilterPredicate> convertIsNullIsNotNull(
      Map<Column, ParquetField> parquetFieldMap, Predicate deltaPredicate, boolean isNotNull) {
    Expression child = getUnaryChild(deltaPredicate);
    if (!(child instanceof Column)) {
      return visitUnsupported(deltaPredicate, "IS NULL predicate must have a column input.");
    }

    Column column = (Column) child;
    ParquetField parquetField = parquetFieldMap.get(column);
    if (parquetField == null) {
      return visitUnsupported(
          deltaPredicate, "Column used in predicate does not exist in the parquet file.");
    }

    String columnPath = ColumnPath.get(column.getNames()).toDotString();
    // Parquet filter keeps records if their value is equal to the provided value.
    // Nulls are treated the same way the java programming language does.
    // For example: eq(column, null) will keep all records whose value is null. eq(column, 7)
    // will keep all records whose value is 7, and will drop records whose value is null
    // NOTE: this is different from how some query languages handle null.
    switch (parquetField.primitiveType.getPrimitiveTypeName()) {
      case BOOLEAN:
        return createIsNullOrIsNotNullPredicate(booleanColumn(columnPath), isNotNull);
      case INT32:
        return createIsNullOrIsNotNullPredicate(intColumn(columnPath), isNotNull);
      case INT64:
        return createIsNullOrIsNotNullPredicate(longColumn(columnPath), isNotNull);
      case FLOAT:
        return createIsNullOrIsNotNullPredicate(floatColumn(columnPath), isNotNull);
      case DOUBLE:
        return createIsNullOrIsNotNullPredicate(doubleColumn(columnPath), isNotNull);
      case BINARY:
        return createIsNullOrIsNotNullPredicate(binaryColumn(columnPath), isNotNull);
      case FIXED_LEN_BYTE_ARRAY:
        return createIsNullOrIsNotNullPredicate(binaryColumn(columnPath), isNotNull);
      default:
        return visitUnsupported(
            deltaPredicate, "Unsupported column type: " + parquetField.primitiveType);
    }
  }

  private static <T extends Comparable<T>, C extends Operators.Column<T> & SupportsEqNotEq>
      Optional<FilterPredicate> createIsNullOrIsNotNullPredicate(C column, boolean isNotNull) {
    return Optional.of(isNotNull ? FilterApi.notEq(column, null) : FilterApi.eq(column, null));
  }

  private static Optional<FilterPredicate> visitUnsupported(Predicate predicate, String message) {
    logger.info("Unsupported predicate: {}. Reason: {}", predicate, message);
    // Filtering is a best effort. If an unsupported predicate expression is received,
    // do not consider it for filtering.
    return Optional.empty();
  }

  private static boolean isBoolean(Literal literal) {
    return literal.getDataType() instanceof BooleanType;
  }

  private static boolean getBoolean(Literal literal) {
    checkArgument(isBoolean(literal), "Literal is not a boolean: %s", literal);
    return (boolean) literal.getValue();
  }

  private static boolean isInteger(Literal literal) {
    DataType dataType = literal.getDataType();
    if (dataType instanceof LongType) {
      // Check if the long value can be represented as an integer
      return ((Long) literal.getValue()).intValue() == (Long) literal.getValue();
    }

    return dataType instanceof ByteType
        || dataType instanceof ShortType
        || dataType instanceof IntegerType
        || dataType instanceof DateType;
  }

  private static int getInt(Literal literal) {
    checkArgument(isInteger(literal), "Literal is not an integer: %s", literal);
    DataType dataType = literal.getDataType();
    if (dataType instanceof LongType) {
      return ((Long) literal.getValue()).intValue();
    }

    return ((Number) literal.getValue()).intValue();
  }

  private static boolean isLong(Literal literal) {
    DataType dataType = literal.getDataType();
    return dataType instanceof LongType
        || dataType instanceof ByteType
        || dataType instanceof ShortType
        || dataType instanceof IntegerType
        || dataType instanceof DateType;
  }

  private static long getLong(Literal literal) {
    checkArgument(isLong(literal), "Literal is not a long: %s", literal);
    DataType dataType = literal.getDataType();
    if (dataType instanceof LongType) {
      return (long) literal.getValue();
    }

    return ((Number) literal.getValue()).longValue();
  }

  private static boolean isFloat(Literal literal) {
    return literal.getDataType() instanceof FloatType;
  }

  private static float getFloat(Literal literal) {
    checkArgument(isFloat(literal), "Literal is not a float: %s", literal);
    return ((Number) literal.getValue()).floatValue();
  }

  private static boolean isDouble(Literal literal) {
    return literal.getDataType() instanceof DoubleType;
  }

  private static double getDouble(Literal literal) {
    checkArgument(isDouble(literal), "Literal is not a double: %s", literal);
    return ((Number) literal.getValue()).doubleValue();
  }

  private static boolean isBinary(Literal literal) {
    DataType type = literal.getDataType();
    return type instanceof BinaryType || type instanceof StringType;
  }

  private static Binary getBinary(Literal literal) {
    checkArgument(isBinary(literal), "Literal is not a binary: %s", literal);
    DataType type = literal.getDataType();
    if (type instanceof BinaryType) {
      return Binary.fromConstantByteArray((byte[]) literal.getValue());
    }
    return Binary.fromString((String) literal.getValue());
  }

  private static boolean isDecimal(Literal literal) {
    return literal.getDataType() instanceof DecimalType;
  }

  private static BigDecimal getDecimal(Literal literal) {
    checkArgument(isDecimal(literal), "Literal is not a decimal: %s", literal);
    return (BigDecimal) literal.getValue();
  }

  /**
   * Converts a BigDecimal to the appropriate format for Parquet filtering based on the primitive
   * type.
   */
  private static Object convertDecimalForParquet(
      BigDecimal decimal, PrimitiveType primitiveType, DecimalLogicalTypeAnnotation decimalType) {
    switch (primitiveType.getPrimitiveTypeName()) {
      case INT32:
        // For INT32-backed decimals, convert to int after scaling
        return decimal.unscaledValue().intValue();
      case INT64:
        // For INT64-backed decimals, convert to long after scaling
        return decimal.unscaledValue().longValue();
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        // For binary-backed decimals, convert to binary representation
        byte[] unscaledBytes = decimal.unscaledValue().toByteArray();
        return Binary.fromConstantByteArray(unscaledBytes);
      default:
        throw new IllegalArgumentException(
            "Unsupported primitive type for decimal: " + primitiveType.getPrimitiveTypeName());
    }
  }

  /** Creates the appropriate filter predicate for decimal comparison operations. */
  private static Optional<FilterPredicate> convertDecimalComparator(
      String columnPath,
      Literal literal,
      String comparator,
      PrimitiveType parquetType,
      DecimalLogicalTypeAnnotation decimalLogicalType) {

    BigDecimal decimalValue = getDecimal(literal);
    // Debug output
    System.out.println(
        "DEBUG: convertDecimalComparator called - column="
            + columnPath
            + ", decimal="
            + decimalValue
            + ", comparator="
            + comparator
            + ", parquetType="
            + parquetType.getPrimitiveTypeName());

    switch (parquetType.getPrimitiveTypeName()) {
      case INT32:
        {
          IntColumn intColumn = intColumn(columnPath);
          int intValue =
              (Integer) convertDecimalForParquet(decimalValue, parquetType, decimalLogicalType);
          System.out.println(
              "DEBUG: INT32 decimal conversion - input="
                  + decimalValue
                  + ", scale="
                  + decimalLogicalType.getScale()
                  + ", precision="
                  + decimalLogicalType.getPrecision()
                  + ", converted intValue="
                  + intValue);
          switch (comparator) {
            case "=":
              System.out.println(FilterApi.eq(intColumn, intValue));
              return Optional.of(FilterApi.eq(intColumn, intValue));
            case "<":
              return Optional.of(FilterApi.lt(intColumn, intValue));
            case "<=":
              return Optional.of(FilterApi.ltEq(intColumn, intValue));
            case ">":
              return Optional.of(FilterApi.gt(intColumn, intValue));
            case ">=":
              return Optional.of(FilterApi.gtEq(intColumn, intValue));
          }
        }
        break;
      case INT64:
        {
          LongColumn longColumn = longColumn(columnPath);
          long longValue =
              (Long) convertDecimalForParquet(decimalValue, parquetType, decimalLogicalType);
          switch (comparator) {
            case "=":
              return Optional.of(FilterApi.eq(longColumn, longValue));
            case "<":
              return Optional.of(FilterApi.lt(longColumn, longValue));
            case "<=":
              return Optional.of(FilterApi.ltEq(longColumn, longValue));
            case ">":
              return Optional.of(FilterApi.gt(longColumn, longValue));
            case ">=":
              return Optional.of(FilterApi.gtEq(longColumn, longValue));
          }
        }
        break;
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        {
          BinaryColumn binaryColumn = binaryColumn(columnPath);
          Binary binaryValue =
              (Binary) convertDecimalForParquet(decimalValue, parquetType, decimalLogicalType);
          switch (comparator) {
            case "=":
              return Optional.of(FilterApi.eq(binaryColumn, binaryValue));
            case "<":
              return Optional.of(FilterApi.lt(binaryColumn, binaryValue));
            case "<=":
              return Optional.of(FilterApi.ltEq(binaryColumn, binaryValue));
            case ">":
              return Optional.of(FilterApi.gt(binaryColumn, binaryValue));
            case ">=":
              return Optional.of(FilterApi.gtEq(binaryColumn, binaryValue));
          }
        }
        break;
    }
    return Optional.empty();
  }

  /**
   * Converts an IN expression to a Parquet filter by expanding it to a series of OR'ed equality
   * predicates. For example: "column IN (value1, value2, value3)" becomes "(column = value1) OR
   * (column = value2) OR (column = value3)".
   */
  private static Optional<FilterPredicate> convertInToParquetFilter(
      Map<Column, ParquetField> parquetFieldMap, Predicate deltaPredicate) {

    List<Expression> children = deltaPredicate.getChildren();
    if (children.size() < 2) {
      return visitUnsupported(
          deltaPredicate, "IN expression requires at least a column and one value.");
    }

    Expression columnExpr = children.get(0);
    if (!(columnExpr instanceof Column)) {
      return visitUnsupported(
          deltaPredicate, "IN expression must have a column as the first argument.");
    }

    Column column = (Column) columnExpr;
    ParquetField parquetField = parquetFieldMap.get(column);
    if (parquetField == null) {
      return visitUnsupported(
          deltaPredicate, "Column used in IN expression does not exist in the parquet file.");
    }

    // Convert each IN value to an equality predicate
    List<FilterPredicate> equalityPredicates = new ArrayList<>();

    for (int i = 1; i < children.size(); i++) {
      Expression valueExpr = children.get(i);
      if (!(valueExpr instanceof Literal)) {
        return visitUnsupported(deltaPredicate, "IN expression values must be literals.");
      }

      Literal literal = (Literal) valueExpr;

      // Skip null values - they don't participate in IN comparisons
      if (literal.getValue() == null) {
        continue;
      }

      // Create an equality predicate for this value
      Optional<FilterPredicate> equalityPredicate =
          convertSingleEqualityToParquetFilter(parquetField, column, literal);

      if (equalityPredicate.isPresent()) {
        equalityPredicates.add(equalityPredicate.get());
      } else {
        // If any individual equality predicate can't be converted,
        // we can't convert the entire IN expression
        return Optional.empty();
      }
    }

    // If no valid predicates were created (e.g., all values were null or unsupported),
    // return empty
    if (equalityPredicates.isEmpty()) {
      return Optional.empty();
    }

    // Debug: log number of equality predicates and the combined OR structure
    try {
      System.out.println(
          "DEBUG: IN conversion - column="
              + ColumnPath.get(column.getNames()).toDotString()
              + ", numValues="
              + equalityPredicates.size());
      for (FilterPredicate p : equalityPredicates) {
        System.out.println("DEBUG: IN equality predicate: " + p);
      }
    } catch (Throwable t) {
      // ignore debug failures
    }

    // Combine all equality predicates with OR
    FilterPredicate result = equalityPredicates.get(0);
    for (int i = 1; i < equalityPredicates.size(); i++) {
      result = FilterApi.or(result, equalityPredicates.get(i));
    }

    try {
      System.out.println("DEBUG: IN combined predicate: " + result);
    } catch (Throwable t) {
      // ignore debug failures
    }

    return Optional.of(result);
  }

  /**
   * Creates a single equality predicate for the given column and literal value. This is a helper
   * method for IN expression conversion.
   */
  private static Optional<FilterPredicate> convertSingleEqualityToParquetFilter(
      ParquetField parquetField, Column column, Literal literal) {

    if (!canUseLiteral(literal, parquetField.primitiveType)) {
      return Optional.empty();
    }

    PrimitiveType parquetType = parquetField.primitiveType;
    String columnPath = ColumnPath.get(column.getNames()).toDotString();

    switch (parquetType.getPrimitiveTypeName()) {
      case BOOLEAN:
        BooleanColumn booleanColumn = booleanColumn(columnPath);
        return Optional.of(FilterApi.eq(booleanColumn, getBoolean(literal)));

      case INT32:
        // Handle decimal columns stored as INT32
        if (parquetField.logicalType instanceof DecimalLogicalTypeAnnotation
            && isDecimal(literal)) {
          IntColumn intColumn = intColumn(columnPath);
          int intValue =
              (Integer)
                  convertDecimalForParquet(
                      getDecimal(literal),
                      parquetType,
                      (DecimalLogicalTypeAnnotation) parquetField.logicalType);
          return Optional.of(FilterApi.eq(intColumn, intValue));
        } else {
          // Handle regular integer columns
          IntColumn intColumn = intColumn(columnPath);
          return Optional.of(FilterApi.eq(intColumn, getInt(literal)));
        }

      case INT64:
        // Handle decimal columns stored as INT64
        if (parquetField.logicalType instanceof DecimalLogicalTypeAnnotation
            && isDecimal(literal)) {
          LongColumn longColumn = longColumn(columnPath);
          long longValue =
              (Long)
                  convertDecimalForParquet(
                      getDecimal(literal),
                      parquetType,
                      (DecimalLogicalTypeAnnotation) parquetField.logicalType);
          return Optional.of(FilterApi.eq(longColumn, longValue));
        } else {
          // Handle regular long columns
          LongColumn longColumn = longColumn(columnPath);
          return Optional.of(FilterApi.eq(longColumn, getLong(literal)));
        }

      case FLOAT:
        FloatColumn floatColumn = floatColumn(columnPath);
        return Optional.of(FilterApi.eq(floatColumn, getFloat(literal)));

      case DOUBLE:
        DoubleColumn doubleColumn = doubleColumn(columnPath);
        return Optional.of(FilterApi.eq(doubleColumn, getDouble(literal)));

      case BINARY:
        // Handle decimal columns stored as BINARY
        if (parquetField.logicalType instanceof DecimalLogicalTypeAnnotation
            && isDecimal(literal)) {
          BinaryColumn binaryColumn = binaryColumn(columnPath);
          Binary binaryValue =
              (Binary)
                  convertDecimalForParquet(
                      getDecimal(literal),
                      parquetType,
                      (DecimalLogicalTypeAnnotation) parquetField.logicalType);
          return Optional.of(FilterApi.eq(binaryColumn, binaryValue));
        } else {
          // Handle regular binary/string columns
          BinaryColumn binaryColumn = binaryColumn(columnPath);
          Binary binary = getBinary(literal);
          return Optional.of(FilterApi.eq(binaryColumn, binary));
        }

      case FIXED_LEN_BYTE_ARRAY:
        // Decimal columns stored as FIXED_LEN_BYTE_ARRAY
        if (parquetField.logicalType instanceof DecimalLogicalTypeAnnotation
            && isDecimal(literal)) {
          BinaryColumn binaryColumn = binaryColumn(columnPath);
          Binary binaryValue =
              (Binary)
                  convertDecimalForParquet(
                      getDecimal(literal),
                      parquetType,
                      (DecimalLogicalTypeAnnotation) parquetField.logicalType);
          return Optional.of(FilterApi.eq(binaryColumn, binaryValue));
        }
        break;
    }

    return Optional.empty();
  }
}
