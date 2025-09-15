/*
 * Copyright (2021) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.engine

import io.delta.kernel.Table
import io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO
import io.delta.kernel.defaults.utils.{TestUtils, WriteUtils}
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.replay.CacheableEngine

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class CacheableEngineSuite extends AnyFunSuite with WriteUtils with TestUtils {

  protected def withTempDirAndEngine(f: (String, Engine) => Unit): Unit = {
    val hadoopFileIO = new HadoopFileIO(new Configuration() {
      {
        // Set the batch sizes to small so that we get to test the multiple batch scenarios.
        set("delta.kernel.default.parquet.reader.batch-size", "2");
        set("delta.kernel.default.json.reader.batch-size", "2");
      }
    })

    val engine = DefaultEngine.create(hadoopFileIO)

    withTempDir { dir =>
      f(dir.getAbsolutePath, engine)
    }
  }

  test("Test the cacheable engine suite.") {
    withTempDirAndEngine { (path: String, engine: Engine) =>
      for (_ <- 0 to 9) {
        spark.range(10).write.format("delta").mode("append").save(path)
      }
      assert(100 == spark.read.format("delta").load(path).count())

      val cacheableEngine = new CacheableEngine(engine);
      val table = Table.forPath(cacheableEngine, path)

      val snapshot = table.getLatestSnapshot(cacheableEngine)
      val scan = snapshot.getScanBuilder.build

      // Try to access those metadata once.
      def accessAllMetadata(): Unit = {
        using(scan.getScanFiles(cacheableEngine)) { it =>
          var fileCount = 0
          while (it.hasNext) {
            val batch = it.next
            using(batch.getRows) { fileIter =>
              while (fileIter.hasNext) {
                fileIter.next()
                fileCount += 1
              }
            }
          }
          assert(fileCount == 10)
        }
      }

      // For the first time.
      accessAllMetadata()

      // For the second time.
      accessAllMetadata()
    }
  }

  def using[R <: AutoCloseable, A](resource: R)(f: R => A): A =
    try f(resource)
    finally if (resource != null) resource.close()
}
