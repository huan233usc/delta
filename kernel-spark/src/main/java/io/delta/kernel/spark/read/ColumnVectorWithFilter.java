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
package io.delta.kernel.spark.read;

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A zero-copy filtered column vector for deletion vector support. This wraps an existing column
 * vector and uses a row ID mapping array to remap row accesses, similar to Iceberg's approach.
 */
public class ColumnVectorWithFilter extends ColumnVector {
  private final ColumnVector baseVector;
  private final int[] rowIdMapping;

  public ColumnVectorWithFilter(ColumnVector baseVector, int[] rowIdMapping) {
    super(baseVector.dataType());
    this.baseVector = baseVector;
    this.rowIdMapping = rowIdMapping;
  }

  @Override
  public void close() {
    // Don't close base vector - it's managed externally
  }

  @Override
  public boolean hasNull() {
    return baseVector.hasNull();
  }

  @Override
  public int numNulls() {
    // Could cache this, but for simplicity just delegate
    int count = 0;
    for (int i = 0; i < rowIdMapping.length; i++) {
      if (isNullAt(i)) {
        count++;
      }
    }
    return count;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return baseVector.isNullAt(rowIdMapping[rowId]);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return baseVector.getBoolean(rowIdMapping[rowId]);
  }

  @Override
  public byte getByte(int rowId) {
    return baseVector.getByte(rowIdMapping[rowId]);
  }

  @Override
  public short getShort(int rowId) {
    return baseVector.getShort(rowIdMapping[rowId]);
  }

  @Override
  public int getInt(int rowId) {
    return baseVector.getInt(rowIdMapping[rowId]);
  }

  @Override
  public long getLong(int rowId) {
    return baseVector.getLong(rowIdMapping[rowId]);
  }

  @Override
  public float getFloat(int rowId) {
    return baseVector.getFloat(rowIdMapping[rowId]);
  }

  @Override
  public double getDouble(int rowId) {
    return baseVector.getDouble(rowIdMapping[rowId]);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    return baseVector.getArray(rowIdMapping[rowId]);
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    return baseVector.getMap(rowIdMapping[rowId]);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    return baseVector.getDecimal(rowIdMapping[rowId], precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return baseVector.getUTF8String(rowIdMapping[rowId]);
  }

  @Override
  public byte[] getBinary(int rowId) {
    return baseVector.getBinary(rowIdMapping[rowId]);
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    return new ColumnVectorWithFilter(baseVector.getChild(ordinal), rowIdMapping);
  }
}
