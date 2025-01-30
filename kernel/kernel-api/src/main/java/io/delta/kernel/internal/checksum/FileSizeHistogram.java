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
package io.delta.kernel.internal.checksum;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructType;
import java.util.*;
import java.util.stream.Collectors;

public class FileSizeHistogram {

  private static long KB = 1024;
  private static long MB = 1024 * 1024;
  private static long GB = 1024 * 1024 * 1024;

  public static final StructType FULL_SCHEMA =
      new StructType()
          .add("sortedBinBoundaries", new ArrayType(LongType.LONG, false))
          .add("fileCounts", new ArrayType(LongType.LONG, false))
          .add("totalbytes", new ArrayType(LongType.LONG, false));

  private static final List<Long> SORTED_BIN_BOUNDRY =
      Arrays.asList(
          0L,
          // Power of 2 till 4 MB
          8 * KB,
          16 * KB,
          32 * KB,
          64 * KB,
          128 * KB,
          256 * KB,
          512 * KB,
          1 * MB,
          2 * MB,
          4 * MB,
          // 4 MB jumps till 40 MB
          8 * MB,
          12 * MB,
          16 * MB,
          20 * MB,
          24 * MB,
          28 * MB,
          32 * MB,
          36 * MB,
          40 * MB,
          // 8 MB jumps till 120 MB
          48 * MB,
          56 * MB,
          64 * MB,
          72 * MB,
          80 * MB,
          88 * MB,
          96 * MB,
          104 * MB,
          112 * MB,
          120 * MB,
          // 4 MB jumps till 144 MB (since we want more detail around the 128 MB mark)
          124 * MB,
          128 * MB,
          132 * MB,
          136 * MB,
          140 * MB,
          144 * MB,
          // 16 MB jumps till 576 MB (reasonable detail until past the 512 MB mark)
          160 * MB,
          176 * MB,
          192 * MB,
          208 * MB,
          224 * MB,
          240 * MB,
          256 * MB,
          272 * MB,
          288 * MB,
          304 * MB,
          320 * MB,
          336 * MB,
          352 * MB,
          368 * MB,
          384 * MB,
          400 * MB,
          416 * MB,
          432 * MB,
          448 * MB,
          464 * MB,
          480 * MB,
          496 * MB,
          512 * MB,
          528 * MB,
          544 * MB,
          560 * MB,
          576 * MB,
          // 64 MB jumps till 1408 MB (detail around the 1024MB mark, allowing for overshoot)
          640 * MB,
          704 * MB,
          768 * MB,
          832 * MB,
          896 * MB,
          960 * MB,
          1024 * MB,
          1088 * MB,
          1152 * MB,
          1216 * MB,
          1280 * MB,
          1344 * MB,
          1408 * MB,
          // 128 MB jumps till 2 GB
          1536 * MB,
          1664 * MB,
          1792 * MB,
          1920 * MB,
          2048 * MB,
          // 256 MB jumps till 4 GB
          2304 * MB,
          2560 * MB,
          2816 * MB,
          3072 * MB,
          3328 * MB,
          3584 * MB,
          3840 * MB,
          4 * GB,
          // power of 2 till 256 GB
          8 * GB,
          16 * GB,
          32 * GB,
          64 * GB,
          128 * GB,
          256 * GB);
  private final Long[] fileCounts;
  private final Long[] totalBytes;
  private final List<Long> sortedBinBoundry;

  public static FileSizeHistogram init() {
    return new FileSizeHistogram(
        SORTED_BIN_BOUNDRY,
        buildZeroArray(SORTED_BIN_BOUNDRY.size()),
        buildZeroArray(SORTED_BIN_BOUNDRY.size()));
  }

  public static Optional<FileSizeHistogram> fromColumnVector(ColumnVector vector, int rowId) {
    if (vector.isNullAt(rowId)) {
      return Optional.empty();
    }
    ;
    List<Long> sortedBinBoundaries =
        VectorUtils.toJavaList(
            vector.getChild(FULL_SCHEMA.indexOf("sortedBinBoundaries")).getArray(rowId));
    Long[] totalBytes =
        VectorUtils.toJavaList(vector.getChild(FULL_SCHEMA.indexOf("totalbytes")).getArray(rowId))
            .toArray(new Long[sortedBinBoundaries.size()]);
    Long[] fileCounts =
        VectorUtils.toJavaList(vector.getChild(FULL_SCHEMA.indexOf("fileCounts")).getArray(rowId))
            .toArray(new Long[sortedBinBoundaries.size()]);
    return Optional.of(new FileSizeHistogram(sortedBinBoundaries, fileCounts, totalBytes));
  }

  public FileSizeHistogram(List<Long> sortedBinBoundry, Long[] fileCounts, Long[] totalBytes) {
    this.sortedBinBoundry = sortedBinBoundry;
    this.fileCounts = fileCounts;
    this.totalBytes = totalBytes;
  }

  /** Insert a given value into the appropriate histogram bin */
  public void insert(Long fileSize) {
    int index = getBinIndex(fileSize, sortedBinBoundry);
    if (index >= 0) {
      fileCounts[index] += 1L;
      totalBytes[index] += fileSize;
    }
  }

  /** Remove a given value into the appropriate histogram bin */
  public void remove(Long fileSize) {
    int index = getBinIndex(fileSize, sortedBinBoundry);
    if (index >= 0) {
      fileCounts[index] -= 1L;
      totalBytes[index] -= fileSize;
    }
  }

  private static int getBinIndex(long fileSize, List<Long> sortedBinBoundry) {
    // The search function on IndexedSeq uses binary search.
    int searchResult = Collections.binarySearch(sortedBinBoundry, fileSize);
    if (searchResult >= 0) {
      return searchResult;
    }
    return -(searchResult + 1);
  }

  private static Long[] buildZeroArray(int size) {
    Long[] list = new Long[size];
    Arrays.fill(list, 0L);
    return list;
  }

  public Row toRow() {
    Map<Integer, Object> value = new HashMap<>();
    value.put(
        FULL_SCHEMA.indexOf("sortedBinBoundaries"), VectorUtils.longArrayValue(sortedBinBoundry));
    value.put(
        FULL_SCHEMA.indexOf("fileCounts"),
        VectorUtils.longArrayValue(Arrays.stream(fileCounts).collect(Collectors.toList())));
    value.put(
        FULL_SCHEMA.indexOf("totalbytes"),
        VectorUtils.longArrayValue(Arrays.stream(totalBytes).collect(Collectors.toList())));
    return new GenericRow(FULL_SCHEMA, value);
  }
}
