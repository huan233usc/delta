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
package io.delta.kernel.utils;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.function.Function;

public class CloseableIteratorUtils {

  /** A helper method to handle both filtering and takeWhile behavior. */
  public static <T> CloseableIterator<T> conditionalFilter(
      CloseableIterator<T> source, Function<T, Boolean> mapper, boolean stopOnFalse) {
    return new CloseableIterator<T>() {
      T next;
      boolean hasLoadedNext;
      boolean falseFound;

      @Override
      public boolean hasNext() {
        if (stopOnFalse && falseFound) {
          return false;
        }
        if (hasLoadedNext) {
          return true;
        }

        while (source.hasNext()) {
          T potentialNext = source.next();
          if (mapper.apply(potentialNext)) {
            next = potentialNext;
            hasLoadedNext = true;
            return true;
          }
          falseFound = true;
          if (stopOnFalse) {
            break;
          }
        }
        return false;
      }

      @Override
      public T next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        hasLoadedNext = false;
        return next;
      }

      @Override
      public void close() throws IOException {
        this.close();
      }
    };
  }
}
