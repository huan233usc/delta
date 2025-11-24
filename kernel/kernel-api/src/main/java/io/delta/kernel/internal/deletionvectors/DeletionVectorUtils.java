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

package io.delta.kernel.internal.deletionvectors;

import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import java.io.IOException;

/**
 * Utility methods for working with Deletion Vectors.
 *
 * <p>This class provides helper methods for creating inline deletion vectors from
 * RoaringBitmapArray instances.
 */
public class DeletionVectorUtils {

  /**
   * Create an inline deletion vector descriptor from a RoaringBitmapArray.
   *
   * <p>This method serializes the bitmap and stores it inline in the log. This is suitable for
   * small deletion vectors (typically < 10KB).
   *
   * @param bitmap The RoaringBitmapArray containing the deleted row indices
   * @return A DeletionVectorDescriptor with the serialized bitmap stored inline
   * @throws IOException If serialization fails
   */
  public static DeletionVectorDescriptor createInlineDV(RoaringBitmapArray bitmap)
      throws IOException {
    if (bitmap.cardinality() == 0) {
      return DeletionVectorDescriptor.EMPTY;
    }

    byte[] serializedBitmap = bitmap.serializeToBytes();
    return DeletionVectorDescriptor.inlineInLog(serializedBitmap, (int) bitmap.cardinality());
  }

  /**
   * Merge an existing deletion vector with new deletions and create a new inline DV.
   *
   * @param existingDv The existing deletion vector descriptor (may be null or empty)
   * @param newDeletions The new deletions to add
   * @param tableDataPath The table data path (required if existingDv is on-disk)
   * @param fileSystemClient The file system client (required if existingDv is on-disk)
   * @return A new inline DeletionVectorDescriptor with the merged result
   * @throws IOException If reading or serialization fails
   */
  public static DeletionVectorDescriptor mergeAndCreateInlineDV(
      DeletionVectorDescriptor existingDv,
      RoaringBitmapArray newDeletions,
      String tableDataPath,
      io.delta.kernel.engine.FileSystemClient fileSystemClient)
      throws IOException {

    if (existingDv == null || existingDv.getCardinality() == 0) {
      // No existing DV, just create from new deletions
      return createInlineDV(newDeletions);
    }

    if (newDeletions.cardinality() == 0) {
      // No new deletions, return existing (but convert to inline if it's on-disk)
      if (existingDv.isInline()) {
        return existingDv;
      } else {
        // Load on-disk DV and convert to inline
        DeletionVectorStoredBitmap storedBitmap =
            new DeletionVectorStoredBitmap(
                existingDv, java.util.Optional.ofNullable(tableDataPath));
        RoaringBitmapArray existingBitmap = storedBitmap.load(fileSystemClient);
        return createInlineDV(existingBitmap);
      }
    }

    // Load existing DV
    DeletionVectorStoredBitmap storedBitmap =
        new DeletionVectorStoredBitmap(existingDv, java.util.Optional.ofNullable(tableDataPath));
    RoaringBitmapArray existingBitmap = storedBitmap.load(fileSystemClient);

    // Merge with new deletions
    for (long value : newDeletions.toArray()) {
      existingBitmap.add(value);
    }

    // Create inline DV from merged bitmap
    return createInlineDV(existingBitmap);
  }
}
