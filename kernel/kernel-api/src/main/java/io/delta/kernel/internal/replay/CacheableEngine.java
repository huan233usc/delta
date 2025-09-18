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
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.*;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheableEngine implements Engine {
  private static final Logger LOG = LoggerFactory.getLogger(CacheableEngine.class);

  // NOTICE: THIS IS EXACTLY THE JSON META CACHE.
  private static final Map<JsonFileKey, List<ColumnarBatch>> JSON_CACHE = new HashMap<>();

  // NOTICE: THIS IS EXACTLY THE PARQUET META CACHE.
  private static final Map<ParquetKey, List<FileReadResult>> PARQUET_CACHE = new HashMap<>();

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
    return new CacheableJsonHandler(engine.getJsonHandler());
  }

  @Override
  public FileSystemClient getFileSystemClient() {
    return engine.getFileSystemClient();
  }

  @Override
  public ParquetHandler getParquetHandler() {
    return new CacheableParquetHandler(engine.getParquetHandler());
  }

  private static class CacheableParquetHandler implements ParquetHandler {
    private final ParquetHandler parquetHandler;

    CacheableParquetHandler(ParquetHandler parquetHandler) {
      this.parquetHandler = parquetHandler;
    }

    @Override
    public CloseableIterator<FileReadResult> readParquetFiles(
        CloseableIterator<FileStatus> fileIter,
        StructType physicalSchema,
        Optional<Predicate> predicate) {
      // Fetch the target iterator.
      List<CloseableIterator<FileReadResult>> results =
          fileIter
              .map(fileStatus -> getCacheOrLoad(fileStatus, physicalSchema, predicate))
              .toInMemoryList();

      // Combine all of them into a combined one.
      CloseableIterator<FileReadResult> result = emptyCloseableIterator();
      for (CloseableIterator<FileReadResult> it : results) {
        result = result.combine(it);
      }
      return result;
    }

    @Override
    public CloseableIterator<DataFileStatus> writeParquetFiles(
        String directoryPath,
        CloseableIterator<FilteredColumnarBatch> dataIter,
        List<Column> statsColumns)
        throws IOException {
      return parquetHandler.writeParquetFiles(directoryPath, dataIter, statsColumns);
    }

    @Override
    public void writeParquetFileAtomically(
        String filePath, CloseableIterator<FilteredColumnarBatch> data) throws IOException {
      parquetHandler.writeParquetFileAtomically(filePath, data);
    }

    private CloseableIterator<FileReadResult> getCacheOrLoad(
        FileStatus fileStatus, StructType schema, Optional<Predicate> predicate) {

      AtomicBoolean cacheHit = new AtomicBoolean(true);
      List<FileReadResult> res =
          PARQUET_CACHE.computeIfAbsent(
              new ParquetKey(fileStatus, schema, predicate),
              key -> {
                cacheHit.set(false);
                return directReadAsInMemory(key.fileStatus, key.schema, key.predicate);
              });
      System.out.println("---> Parquet READ --> Cache Key missed or hit ?" + cacheHit.get());

      return asCloseableIterator(res);
    }

    private List<FileReadResult> directReadAsInMemory(
        FileStatus fileStatus, StructType schema, Optional<Predicate> predicate) {
      try {
        CloseableIterator<FileReadResult> closeableIterator =
            parquetHandler.readParquetFiles(
                singletonCloseableIterator(fileStatus), schema, predicate);

        return asInMemoryList(closeableIterator);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private static class CacheableJsonHandler implements JsonHandler {
    private final JsonHandler jsonHandler;

    CacheableJsonHandler(JsonHandler jsonHandler) {
      this.jsonHandler = jsonHandler;
    }

    @Override
    public ColumnarBatch parseJson(
        ColumnVector jsonStringVector,
        StructType outputSchema,
        Optional<ColumnVector> selectionVector) {
      return jsonHandler.parseJson(jsonStringVector, outputSchema, selectionVector);
    }

    @Override
    public CloseableIterator<ColumnarBatch> readJsonFiles(
        CloseableIterator<FileStatus> fileIter,
        StructType physicalSchema,
        Optional<Predicate> predicate)
        throws IOException {
      if (predicate.isPresent()) {
        // No cache if predicate is present, since hard to hit the predicate cache.
        return jsonHandler.readJsonFiles(fileIter, physicalSchema, predicate);
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

      AtomicBoolean cacheHit = new AtomicBoolean(true);
      List<ColumnarBatch> res =
          JSON_CACHE.computeIfAbsent(
              JsonFileKey.of(fileStatus, schema),
              key -> {
                cacheHit.set(false);
                return directReadAsInMemory(key.fileStatus, key.schema);
              });
      System.out.println("--> JSON READ --> Cache Key missed or hit ? " + cacheHit.get());

      return asCloseableIterator(res);
    }

    private List<ColumnarBatch> directReadAsInMemory(FileStatus fileStatus, StructType schema) {
      try {
        CloseableIterator<ColumnarBatch> closeableIterator =
            jsonHandler.readJsonFiles(
                singletonCloseableIterator(fileStatus), schema, Optional.empty());

        return asInMemoryList(closeableIterator);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void writeJsonFileAtomically(
        String filePath, CloseableIterator<Row> data, boolean overwrite) throws IOException {
      jsonHandler.writeJsonFileAtomically(filePath, data, overwrite);
    }
  }

  private static class ParquetKey {
    private final FileStatus fileStatus;
    private final StructType schema;
    private final Optional<Predicate> predicate;

    ParquetKey(FileStatus fileStatus, StructType schema, Optional<Predicate> predicate) {
      this.fileStatus = fileStatus;
      this.schema = schema;
      this.predicate = predicate;
    }

    @Override
    public int hashCode() {
      return Objects.hash(fileStatus, schema, predicate);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof ParquetKey) {
        ParquetKey other = (ParquetKey) o;
        return Objects.equals(fileStatus, other.fileStatus)
            && Objects.equals(schema, other.schema)
            && Objects.equals(predicate, other.predicate);
      }

      return false;
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

    @Override
    public int hashCode() {
      return Objects.hash(fileStatus, schema);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof JsonFileKey) {
        JsonFileKey other = (JsonFileKey) o;
        return Objects.equals(fileStatus, other.fileStatus) && Objects.equals(schema, other.schema);
      }
      return false;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder
          .append("path=")
          .append(fileStatus.getPath())
          .append(",")
          .append("schema=")
          .append(schema);
      return builder.toString();
    }
  }

  private static <T> List<T> asInMemoryList(CloseableIterator<T> iterator) {

    // Make it to be an in-memory array list.
    List<T> list = new ArrayList<>();
    try (CloseableIterator<T> ignored = iterator) {
      while (iterator.hasNext()) {
        list.add(iterator.next());
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    // Wrap it as a CloseableIterator.
    return list;
  }

  private static <T> CloseableIterator<T> asCloseableIterator(List<T> list) {
    return new CloseableIterator<T>() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        return index < list.size();
      }

      @Override
      public T next() {
        return list.get(index++);
      }

      @Override
      public void close() {}
    };
  }

  private static <T> CloseableIterator<T> emptyCloseableIterator() {
    return new CloseableIterator<T>() {
      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public T next() {
        throw new NoSuchElementException();
      }

      @Override
      public void close() {}
    };
  }
}
