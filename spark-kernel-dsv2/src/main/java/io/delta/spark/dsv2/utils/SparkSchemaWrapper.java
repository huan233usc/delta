package io.delta.spark.dsv2.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkSchemaWrapper {
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final StructType readSchema;

  public static SparkSchemaWrapper build(StructType baseSchema, Set<String> partitionColumns) {
    List<StructField> partitionFields = new ArrayList<>();
    List<StructField> dataFields = new ArrayList<>();

    Arrays.stream(baseSchema.fields())
        .forEach(
            field -> {
              if (partitionColumns.contains(field.name())) {
                partitionFields.add(field);
              } else {
                dataFields.add(field);
              }
            });

    return new SparkSchemaWrapper(
        new StructType(dataFields.toArray(new StructField[0])),
        new StructType(partitionFields.toArray(new StructField[0])));
  }

  private SparkSchemaWrapper(StructType dataSchema, StructType partitionSchema) {
    this.dataSchema = dataSchema;
    this.partitionSchema = partitionSchema;
    // Read schema = dataSchema.fields ++ partitionSchema.fields
    List<StructField> readFields = new ArrayList<>();
    readFields.addAll(Arrays.asList(dataSchema.fields()));
    readFields.addAll(Arrays.asList(partitionSchema.fields()));
    this.readSchema = new StructType(readFields.toArray(new StructField[0]));
  }

  public StructType getDataSchema() {
    return dataSchema;
  }

  public StructType getPartitionSchema() {
    return partitionSchema;
  }

  public StructType getReadSchema() {
    return readSchema;
  }
}
