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

package org.apache.spark.sql.delta

import io.delta.kernel.{Operation, Transaction}
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.internal.InternalScanFileUtils
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.utils.CloseableIterable

import org.apache.spark.sql.{QueryTest, Row => SparkRow}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for DELETE operations using kernel-spark connector.
 */
class KernelSparkDeleteSuite 
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  test("kernel-spark DELETE - delete all rows using Kernel API") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      val hadoopConf = spark.sessionState.newHadoopConf()
      
      // Step 1: Create table and insert data
      spark.range(10).toDF("id").write.format("delta").save(path)
      
      // Verify data exists
      checkAnswer(
        spark.read.format("delta").load(path),
        spark.range(10).toDF("id")
      )
      
      // Step 2: Get all AddFiles using Kernel Scan API
      val engine = DefaultEngine.create(hadoopConf)
      val table = io.delta.kernel.Table.forPath(engine, path)
      val snapshot = table.getLatestSnapshot(engine)
      
      val scanBuilder = snapshot.getScanBuilder()
      val scan = scanBuilder.build()
      val scanFilesIter = scan.getScanFiles(engine)
      
      // Step 3: Extract AddFile Rows from scan files
      val addFilesList = new java.util.ArrayList[Row]()
      try {
        while (scanFilesIter.hasNext) {
          val scanFileBatch = scanFilesIter.next()
          val data = scanFileBatch.getData
          val rowsIter = data.getRows()
          
          try {
            // Extract AddFile Row from each row in the batch
            while (rowsIter.hasNext) {
              val scanFileRow = rowsIter.next()
              // Get AddFile struct from the scan file row (ordinal 0)
              if (!scanFileRow.isNullAt(InternalScanFileUtils.ADD_FILE_ORDINAL)) {
                val addFileRow = scanFileRow.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL)
                if (addFileRow != null) {
                  addFilesList.add(addFileRow)
                }
              }
            }
          } finally {
            rowsIter.close()
          }
        }
      } finally {
        scanFilesIter.close()
      }
      
      // Step 4: Generate DELETE actions from AddFiles
      val addFilesIter = Utils.toCloseableIterator(addFilesList.iterator())
      val deleteActionsIter = Transaction.generateDeleteActions(engine, addFilesIter)
      
      val deleteActionsList = new java.util.ArrayList[Row]()
      try {
        while (deleteActionsIter.hasNext) {
          deleteActionsList.add(deleteActionsIter.next())
        }
      } finally {
        deleteActionsIter.close()
      }
      
      val deleteActions = CloseableIterable.inMemoryIterable(
        Utils.toCloseableIterator(deleteActionsList.iterator())
      )
      
      // Step 5: Commit transaction
      val txnBuilder = snapshot.buildUpdateTableTransaction("kernel-spark", Operation.WRITE)
      val txn = txnBuilder.build(engine)
      val commitResult = txn.commit(engine, deleteActions)
      
      // Step 6: Verify all data is deleted
      checkAnswer(
        spark.read.format("delta").load(path),
        Seq.empty[SparkRow]
      )
      
      // Step 7: Verify transaction log
      val deltaLog = DeltaLog.forTable(spark, path)
      assert(deltaLog.update().version === commitResult.getVersion)
      assert(deltaLog.update().allFiles.count() === 0)
    }
  }

  test("kernel-spark DELETE - SQL DELETE statement (delete all)") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      
      // Step 1: Create table and insert data
      spark.range(10).toDF("id").write.format("delta").save(path)
      
      // Verify data exists
      checkAnswer(
        spark.read.format("delta").load(path),
        spark.range(10).toDF("id")
      )
      
      // Step 2: Execute DELETE using SQL
      // Note: Currently deleteWhere ignores filters and deletes all files
      spark.sql(s"DELETE FROM delta.`$path` WHERE id >= 0")
      
      // Step 3: Verify all data is deleted
      checkAnswer(
        spark.read.format("delta").load(path),
        Seq.empty[SparkRow]
      )
      
      // Step 4: Verify transaction log
      val deltaLog = DeltaLog.forTable(spark, path)
      assert(deltaLog.update().version === 1) // version 0: create, version 1: delete
      assert(deltaLog.update().allFiles.count() === 0)
    }
  }
}

