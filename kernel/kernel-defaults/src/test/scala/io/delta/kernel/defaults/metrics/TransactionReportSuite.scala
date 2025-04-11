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
package io.delta.kernel.defaults.metrics

import java.util.{Collections, Objects, Optional}

import scala.collection.JavaConverters._

import io.delta.kernel._
import io.delta.kernel.data.Row
import io.delta.kernel.engine._
import io.delta.kernel.internal.TableConfig
import io.delta.kernel.internal.actions.{GenerateIcebergCompatActionUtils, SingleAction}
import io.delta.kernel.internal.data.TransactionStateRow
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.metrics.Timer
import io.delta.kernel.internal.stats.FileSizeHistogram
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.metrics.{FileSizeHistogramResult, SnapshotReport, TransactionMetricsResult, TransactionReport}
import io.delta.kernel.types.{IntegerType, StructType}
import io.delta.kernel.utils.{CloseableIterable, CloseableIterator, DataFileStatus}
import io.delta.kernel.utils.CloseableIterable.inMemoryIterable

import org.scalatest.funsuite.AnyFunSuite

class TransactionReportSuite extends AnyFunSuite with MetricsReportTestUtils {

  /**
   * Creates a [[Transaction]] using `getTransaction`, requests actions to commit using
   * `generateCommitActions`, and commits them to the transaction. Uses a custom engine for all
   * of these operations that collects any emitted metrics reports. Exactly 1 [[TransactionReport]]
   * is expected to be emitted, and at most one [[SnapshotReport]]. Also times and returns the
   * duration it takes for [[Transaction#commit]] to finish.
   *
   * @param createTransaction given an engine return a started [[Transaction]]
   * @param generateCommitActions given a [[Transaction]] and engine generates actions to commit
   * @param expectException whether we expect committing to throw an exception, which if so, is
   *                        caught and returned with the other results
   * @return (TransactionReport, durationToCommit, SnapshotReportIfPresent, ExceptionIfThrown)
   */
  def getTransactionAndSnapshotReport(
      createTransaction: Engine => Transaction,
      generateCommitActions: (Transaction, Engine) => CloseableIterable[Row],
      expectException: Boolean,
      validateTransactionMetrics: (TransactionMetricsResult, Long) => Unit)
      : (TransactionReport, Long, Option[SnapshotReport], Option[Exception]) = {
    val timer = new Timer()

    val (metricsReports, exception) = collectMetricsReports(
      engine => {
        val transaction = createTransaction(engine)
        val actionsToCommit = generateCommitActions(transaction, engine)
        val txnCommitResult = timer.time(() =>
          transaction.commit(engine, actionsToCommit)) // Time the actual operation
        // Validate the txn metrics returned in txnCommitResult
        validateTransactionMetrics(txnCommitResult.getTransactionMetrics, timer.totalDurationNs())
      },
      expectException)

    val transactionReports = metricsReports.filter(_.isInstanceOf[TransactionReport])
    assert(transactionReports.length == 1, "Expected exactly 1 TransactionReport")
    val snapshotReports = metricsReports.filter(_.isInstanceOf[SnapshotReport])
    assert(snapshotReports.length <= 1, "Expected at most 1 SnapshotReport")
    (
      transactionReports.head.asInstanceOf[TransactionReport],
      timer.totalDurationNs(),
      snapshotReports.headOption.map(_.asInstanceOf[SnapshotReport]),
      exception)
  }

  /**
   * Builds a transaction using `buildTransaction` for the table at the provided path. Commits
   * to the transaction the actions generated by `generateCommitActions` and collects any emitted
   * [[TransactionReport]]. Checks that the report is as expected
   *
   * @param generateCommitActions function to generate commit actions from a transaction and engine
   * @param path table path to commit to
   * @param expectException whether we expect committing to throw an exception
   * @param expectedBaseSnapshotVersion expected snapshot version for the transaction
   * @param expectedNumAddFiles expected number of add files recorded in the metrics
   * @param expectedNumRemoveFiles expected number of remove files recorded in the metrics
   * @param expectedNumTotalActions expected number of total actions recorded in the metrics
   * @param expectedCommitVersion expected commit version if not `expectException`
   * @param expectedNumAttempts expected number of commit attempts
   * @param buildTransaction function to build a transaction from a transaction builder
   * @param engineInfo engine info to create the transaction with
   * @param operation operation to create the transaction with
   */
  // scalastyle:off
  def checkTransactionReport(
      generateCommitActions: (Transaction, Engine) => CloseableIterable[Row],
      path: String,
      expectException: Boolean,
      expectedBaseSnapshotVersion: Long,
      expectedNumAddFiles: Long = 0,
      expectedNumRemoveFiles: Long = 0,
      expectedNumTotalActions: Long = 0,
      expectedCommitVersion: Option[Long] = None,
      expectedNumAttempts: Long = 1,
      expectedTotalAddFilesSizeInBytes: Long = 0,
      expectedTotalRemoveFilesSizeInBytes: Long = 0,
      expectedFileSizeHistogramResult: Option[FileSizeHistogramResult] = None,
      buildTransaction: (TransactionBuilder, Engine) => Transaction = (tb, e) => tb.build(e),
      engineInfo: String = "test-engine-info",
      operation: Operation = Operation.MANUAL_UPDATE): Unit = {
    // scalastyle:on
    assert(expectException == expectedCommitVersion.isEmpty)
    def validateTransactionMetrics(txnMetrics: TransactionMetricsResult, duration: Long): Unit = {
      // Since we cannot know the actual duration of commit we sanity check that they are > 0 and
      // less than the total operation duration
      assert(txnMetrics.getTotalCommitDurationNs > 0)
      assert(txnMetrics.getTotalCommitDurationNs < duration)

      assert(txnMetrics.getNumCommitAttempts == expectedNumAttempts)
      assert(txnMetrics.getNumAddFiles == expectedNumAddFiles)
      assert(txnMetrics.getTotalAddFilesSizeInBytes == expectedTotalAddFilesSizeInBytes)
      assert(txnMetrics.getNumRemoveFiles == expectedNumRemoveFiles)
      assert(txnMetrics.getNumTotalActions == expectedNumTotalActions)
      assert(txnMetrics.getTotalRemoveFilesSizeInBytes == expectedTotalRemoveFilesSizeInBytes)

      // For now since we don't support writing fileSizeHistogram yet we only expect this to be
      // present on the first write to a table. We will update these tests when we add write
      // support.
      expectedFileSizeHistogramResult match {
        case Some(expectedHistogram) =>
          assert(txnMetrics.getTableFileSizeHistogram.isPresent)
          txnMetrics.getTableFileSizeHistogram.toScala.foreach { foundHistogram =>
            assert(expectedHistogram.getSortedBinBoundaries sameElements
              foundHistogram.getSortedBinBoundaries)
            assert(expectedHistogram.getFileCounts sameElements foundHistogram.getFileCounts)
            assert(expectedHistogram.getTotalBytes sameElements foundHistogram.getTotalBytes)
          }
        case None => assert(!txnMetrics.getTableFileSizeHistogram.isPresent)
      }
    }

    val (transactionReport, duration, snapshotReportOpt, exception) =
      getTransactionAndSnapshotReport(
        engine =>
          buildTransaction(
            Table.forPath(engine, path).createTransactionBuilder(engine, engineInfo, operation),
            engine),
        generateCommitActions,
        expectException,
        validateTransactionMetrics)

    // Verify contents
    assert(transactionReport.getTablePath == defaultEngine.getFileSystemClient.resolvePath(path))
    assert(transactionReport.getOperationType == "Transaction")
    exception match {
      case Some(e) =>
        assert(transactionReport.getException().isPresent &&
          Objects.equals(transactionReport.getException().get(), e))
      case None => assert(!transactionReport.getException().isPresent)
    }
    assert(transactionReport.getReportUUID != null)
    assert(transactionReport.getOperation == operation.toString)
    assert(transactionReport.getEngineInfo == engineInfo)

    assert(transactionReport.getBaseSnapshotVersion == expectedBaseSnapshotVersion)
    if (expectedBaseSnapshotVersion < 0) {
      // This was for a new table, there is no corresponding SnapshotReport
      assert(!transactionReport.getSnapshotReportUUID.isPresent)
    } else {
      assert(snapshotReportOpt.exists { snapshotReport =>
        snapshotReport.getVersion.toScala.contains(expectedBaseSnapshotVersion) &&
        transactionReport.getSnapshotReportUUID.toScala.contains(snapshotReport.getReportUUID)
      })
    }
    assert(transactionReport.getCommittedVersion.toScala == expectedCommitVersion)
    validateTransactionMetrics(transactionReport.getTransactionMetrics, duration)
  }

  def generateAppendActions(fileStatusIter: CloseableIterator[DataFileStatus])(
      trans: Transaction,
      engine: Engine): CloseableIterable[Row] = {
    val transState = trans.getTransactionState(engine)
    CloseableIterable.inMemoryIterable(
      Transaction.generateAppendActions(
        engine,
        transState,
        fileStatusIter,
        Transaction.getWriteContext(engine, transState, Collections.emptyMap())))
  }

  def generateRemoveActions(fileStatusIter: CloseableIterator[DataFileStatus])(
      trans: Transaction,
      engine: Engine): CloseableIterable[Row] = {
    // For now we use GenerateIcebergCompatActionUtils to generate the remove rows since this is the
    // only current API support in Kernel for generating removes; in the future when we support a
    // more general API for removes we should use that here
    inMemoryIterable(fileStatusIter.map { fileStatus =>
      SingleAction.createRemoveFileSingleAction(
        GenerateIcebergCompatActionUtils.convertRemoveDataFileStatus(
          TransactionStateRow.getPhysicalSchema(trans.getTransactionState(engine)),
          new Path(TransactionStateRow.getTablePath(trans.getTransactionState(engine))).toUri,
          fileStatus,
          Collections.emptyMap(), // partitionValues
          true // dataChange
        ))
    })
  }

  def incrementFileSizeHistogram(
      histogram: FileSizeHistogram,
      fileStatusIter: CloseableIterator[DataFileStatus]): FileSizeHistogram = {
    fileStatusIter.forEach(fs => histogram.insert(fs.getSize))
    histogram
  }

  test("TransactionReport: Basic append to existing table + update metadata") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      // Set up delta table with version 0
      spark.range(10).write.format("delta").mode("append").save(path)

      // Commit 1 AddFiles
      checkTransactionReport(
        generateCommitActions = generateAppendActions(fileStatusIter1),
        path,
        expectException = false,
        expectedBaseSnapshotVersion = 0,
        expectedNumAddFiles = 1,
        expectedNumTotalActions = 2, // commitInfo + addFile
        expectedCommitVersion = Some(1),
        expectedTotalAddFilesSizeInBytes = 100)

      // Commit 2 AddFiles
      checkTransactionReport(
        generateCommitActions = generateAppendActions(fileStatusIter2),
        path,
        expectException = false,
        expectedBaseSnapshotVersion = 1,
        expectedNumAddFiles = 2,
        expectedNumTotalActions = 3, // commitInfo + addFile
        expectedCommitVersion = Some(2),
        engineInfo = "foo",
        expectedTotalAddFilesSizeInBytes = 200,
        operation = Operation.WRITE)

      // Update metadata only
      checkTransactionReport(
        generateCommitActions = (_, _) => CloseableIterable.emptyIterable(),
        path,
        expectException = false,
        expectedBaseSnapshotVersion = 2,
        expectedNumTotalActions = 2, // metadata, commitInfo
        expectedCommitVersion = Some(3),
        buildTransaction = (transBuilder, engine) => {
          transBuilder
            .withTableProperties(engine, Map(TableConfig.CHECKPOINT_INTERVAL.getKey -> "2").asJava)
            .build(engine)
        })
    }
  }

  test("TransactionReport: Create new empty table and then append") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      checkTransactionReport(
        generateCommitActions = (_, _) => CloseableIterable.emptyIterable(),
        path,
        expectException = false,
        expectedBaseSnapshotVersion = -1,
        expectedNumTotalActions = 3, // protocol, metadata, commitInfo
        expectedCommitVersion = Some(0),
        expectedFileSizeHistogramResult = Some(
          FileSizeHistogram.createDefaultHistogram().captureFileSizeHistogramResult()),
        buildTransaction = (transBuilder, engine) => {
          transBuilder
            .withSchema(engine, new StructType().add("id", IntegerType.INTEGER))
            .build(engine)
        })

      // Commit 2 AddFiles
      checkTransactionReport(
        generateCommitActions = generateAppendActions(fileStatusIter2),
        path,
        expectException = false,
        expectedBaseSnapshotVersion = 0,
        expectedNumAddFiles = 2,
        expectedNumTotalActions = 3, // commitInfo + addFile
        expectedCommitVersion = Some(1),
        expectedTotalAddFilesSizeInBytes = 200)
    }
  }

  test("TransactionReport: Create new non-empty table with insert") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      checkTransactionReport(
        generateCommitActions = generateAppendActions(fileStatusIter1),
        path,
        expectException = false,
        expectedBaseSnapshotVersion = -1,
        expectedNumAddFiles = 1,
        expectedNumTotalActions = 4, // protocol, metadata, commitInfo
        expectedCommitVersion = Some(0),
        expectedTotalAddFilesSizeInBytes = 100,
        expectedFileSizeHistogramResult = Some(
          incrementFileSizeHistogram(
            FileSizeHistogram.createDefaultHistogram(),
            fileStatusIter1).captureFileSizeHistogramResult()),
        buildTransaction = (transBuilder, engine) => {
          transBuilder
            .withSchema(engine, new StructType().add("id", IntegerType.INTEGER))
            .build(engine)
        })
    }
  }

  test("TransactionReport: remove files from a table") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      // Create a table and insert 1 file into it
      checkTransactionReport(
        generateCommitActions = generateAppendActions(fileStatusIter1),
        path,
        expectException = false,
        expectedBaseSnapshotVersion = -1,
        expectedNumAddFiles = 1,
        expectedNumTotalActions = 4, // protocol, metadata, commitInfo, addFile
        expectedCommitVersion = Some(0),
        expectedTotalAddFilesSizeInBytes = 100,
        expectedFileSizeHistogramResult = Some(
          incrementFileSizeHistogram(
            FileSizeHistogram.createDefaultHistogram(),
            fileStatusIter1).captureFileSizeHistogramResult()),
        buildTransaction = (transBuilder, engine) => {
          transBuilder
            .withSchema(engine, new StructType().add("id", IntegerType.INTEGER))
            .build(engine)
        })

      // Remove the 1 file and insert 2 new ones
      checkTransactionReport(
        generateCommitActions = (txn, engine) =>
          inMemoryIterable(generateAppendActions(fileStatusIter2)(txn, engine).iterator().combine(
            generateRemoveActions(fileStatusIter1)(txn, engine).iterator())),
        path,
        expectException = false,
        expectedBaseSnapshotVersion = 0,
        expectedNumAddFiles = 2,
        expectedNumRemoveFiles = 1,
        expectedNumTotalActions = 4, // commitInfo, removeFile, 2 addFile
        expectedCommitVersion = Some(1),
        expectedTotalAddFilesSizeInBytes = 200,
        expectedTotalRemoveFilesSizeInBytes = 100)

      // Remove the two files inserted
      checkTransactionReport(
        generateCommitActions = generateRemoveActions(fileStatusIter2),
        path,
        expectException = false,
        expectedBaseSnapshotVersion = 1,
        expectedNumRemoveFiles = 2,
        expectedNumTotalActions = 3, // commitInfo, 2 removeFile
        expectedCommitVersion = Some(2),
        expectedTotalRemoveFilesSizeInBytes = 200)
    }
  }

  test("TransactionReport: retry with a concurrent append") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      // Set up delta table with version 0
      spark.range(10).write.format("delta").mode("append").save(path)

      checkTransactionReport(
        generateCommitActions = (trans, engine) => {
          spark.range(10).write.format("delta").mode("append").save(path)
          generateAppendActions(fileStatusIter1)(trans, engine)
        },
        path,
        expectException = false,
        expectedBaseSnapshotVersion = 0,
        expectedNumAddFiles = 1,
        expectedNumTotalActions = 2, // commitInfo + removeFile
        expectedCommitVersion = Some(2),
        expectedNumAttempts = 2,
        expectedTotalAddFilesSizeInBytes = 100,
        // This should always be empty on retries until we support updating based on concurrent txn
        expectedFileSizeHistogramResult = None)
    }
  }

  test("TransactionReport: fail due to conflicting write") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      // Set up delta table with version 0
      spark.range(10).write.format("delta").mode("append").save(path)

      checkTransactionReport(
        generateCommitActions = (trans, engine) => {
          spark.sql("ALTER TABLE delta.`" + path + "` ADD COLUMN newCol INT")
          generateAppendActions(fileStatusIter1)(trans, engine)
        },
        path,
        expectException = true,
        expectedBaseSnapshotVersion = 0)
    }
  }

  test("TransactionReport: fail due to too many tries") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      // Set up delta table with version 0
      spark.range(10).write.format("delta").mode("append").save(path)

      // This writes a concurrent append everytime the iterable is asked for an iterator. This means
      // there should be a conflicting transaction committed everytime Kernel tries to commit
      def actionsIterableWithConcurrentAppend(
          trans: Transaction,
          engine: Engine): CloseableIterable[Row] = {
        val transState = trans.getTransactionState(engine)
        val writeContext = Transaction.getWriteContext(engine, transState, Collections.emptyMap())

        new CloseableIterable[Row] {

          override def iterator(): CloseableIterator[Row] = {
            spark.range(10).write.format("delta").mode("append").save(path)
            Transaction.generateAppendActions(engine, transState, fileStatusIter1, writeContext)
          }

          override def close(): Unit = ()
        }
      }

      checkTransactionReport(
        generateCommitActions = actionsIterableWithConcurrentAppend,
        path,
        expectException = true,
        expectedBaseSnapshotVersion = 0,
        expectedNumAttempts = 6, // 1 first try + 6 retries
        buildTransaction = (builder, engine) => builder.withMaxRetries(5).build(engine))
    }
  }

  /////////////////////
  // Test Constants //
  ////////////////////

  private def fileStatusIter1 = Utils.toCloseableIterator(
    Seq(new DataFileStatus("/path/to/file", 100, 100, Optional.empty())).iterator.asJava)

  private def fileStatusIter2 = Utils.toCloseableIterator(
    Seq(
      new DataFileStatus("/path/to/file1", 100, 100, Optional.empty()),
      new DataFileStatus("/path/to/file2", 100, 100, Optional.empty())).iterator.asJava)
}
