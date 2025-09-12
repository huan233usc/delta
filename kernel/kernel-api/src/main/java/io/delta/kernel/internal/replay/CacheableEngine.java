/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.replay;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.*;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CacheableEngine implements Engine {
  private static final Map<JsonFileKey, CloseableIterator<ColumnarBatch>> JSON_CACHE =
      new ConcurrentHashMap<>();
  private final Engine engine;

  public CacheableEngine(Engine engine) {
    this.engine = engine;
  }

  @Override
  public ExpressionHandler getExpressionHandler() {
    return engine.getExpressionHandler();
  }

  @Override
  public JsonHandler getJsonHandler() {
    return new CacheableJsonHandler();
  }

  @Override
  public FileSystemClient getFileSystemClient() {
    return engine.getFileSystemClient();
  }

  @Override
  public ParquetHandler getParquetHandler() {
    return engine.getParquetHandler();
  }

  private class CacheableJsonHandler implements JsonHandler {

    @Override
    public ColumnarBatch parseJson(
        ColumnVector jsonStringVector,
        StructType outputSchema,
        Optional<ColumnVector> selectionVector) {
      return engine.getJsonHandler().parseJson(jsonStringVector, outputSchema, selectionVector);
    }

    @Override
    public CloseableIterator<ColumnarBatch> readJsonFiles(
        CloseableIterator<FileStatus> fileIter,
        StructType physicalSchema,
        Optional<Predicate> predicate)
        throws IOException {
      if (predicate.isPresent()) {
        // No cache if predicate is present, since hard to hit the predicate cache.
        return engine.getJsonHandler().readJsonFiles(fileIter, physicalSchema, predicate);
      } else {
        // Fetch the target iterator.
        List<CloseableIterator<ColumnarBatch>> results =
            fileIter.map(fileStatus -> getCacheOrLoad(fileStatus, physicalSchema)).toInMemoryList();

        // Combine all of them into a combined one.
        CloseableIterator<ColumnarBatch> result = emptyCloseableIterator();
        for (CloseableIterator<ColumnarBatch> it : results) {
          result = result.combine(it);
        }
        return result;
      }
    }

    private CloseableIterator<ColumnarBatch> getCacheOrLoad(
        FileStatus fileStatus, StructType schema) {
      return JSON_CACHE.computeIfAbsent(
          JsonFileKey.of(fileStatus, schema),
          key -> directReadAsMemory(key.fileStatus, key.schema));
    }

    private CloseableIterator<ColumnarBatch> directReadAsMemory(
        FileStatus fileStatus, StructType schema) {
      try {
        CloseableIterator<ColumnarBatch> closeableIterator =
            engine
                .getJsonHandler()
                .readJsonFiles(singletonCloseableIterator(fileStatus), schema, Optional.empty());

        return asInMemoryCloseableIterator(closeableIterator);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void writeJsonFileAtomically(
        String filePath, CloseableIterator<Row> data, boolean overwrite) throws IOException {
      engine.getJsonHandler().writeJsonFileAtomically(filePath, data, overwrite);
    }
  }

  private static class JsonFileKey {
    private final FileStatus fileStatus;
    private final StructType schema;

    JsonFileKey(FileStatus fileStatus, StructType schema) {
      this.fileStatus = fileStatus;
      this.schema = schema;
    }

    static JsonFileKey of(FileStatus fileStatus, StructType schema) {
      return new JsonFileKey(fileStatus, schema);
    }
  }

  private CloseableIterator<ColumnarBatch> asInMemoryCloseableIterator(
      CloseableIterator<ColumnarBatch> iterator) {

    // Make it to be an in-memory array list.
    List<ColumnarBatch> list = new ArrayList<>();
    try (CloseableIterator<ColumnarBatch> ignored = iterator) {
      while (iterator.hasNext()) {
        list.add(iterator.next());
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    // Wrap it as a CloseableIterator.
    return new CloseableIterator<ColumnarBatch>() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        return index < list.size();
      }

      @Override
      public ColumnarBatch next() {
        return list.get(index++);
      }

      @Override
      public void close() {}
    };
  }

  private CloseableIterator<ColumnarBatch> emptyCloseableIterator() {
    return new CloseableIterator<ColumnarBatch>() {
      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public ColumnarBatch next() {
        throw new NoSuchElementException();
      }

      @Override
      public void close() {}
    };
  }
}
