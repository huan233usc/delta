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
package io.delta.kernel.defaults

import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel._
import io.delta.kernel.defaults.internal.parquet.ParquetSuiteBase
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions._
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.{SnapshotImpl, TableConfig, TableImpl, TransactionBuilderImpl, TransactionImpl}
import io.delta.kernel.internal.actions.{DomainMetadata, Protocol, SingleAction}
import io.delta.kernel.internal.rowtracking.RowTrackingMetadataDomain
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.RowId.{RowTrackingMetadataDomain => SparkRowTrackingMetadataDomain}
import org.apache.spark.sql.delta.actions.{DomainMetadata => SparkDomainMetadata}
import org.apache.spark.sql.delta.test.DeltaTestImplicits.OptimisticTxnTestHelper

import org.apache.hadoop.fs.Path

class DomainMetadataSuite extends DeltaTableWriteSuiteBase with ParquetSuiteBase {

  private def assertDomainMetadata(
      snapshot: SnapshotImpl,
      expectedValue: Map[String, DomainMetadata]): Unit = {
    // Check using internal API
    assert(expectedValue === snapshot.getDomainMetadataMap.asScala)
    // Verify public API
    expectedValue.foreach { case (key, domainMetadata) =>
      snapshot.getDomainMetadata(key).toScala match {
        case Some(config) =>
          assert(!domainMetadata.isRemoved && config == domainMetadata.getConfiguration)
        case None =>
          assert(domainMetadata.isRemoved)
      }
    }
  }

  private def assertDomainMetadata(
      table: Table,
      engine: Engine,
      expectedValue: Map[String, DomainMetadata]): Unit = {
    // Get the latest snapshot of the table
    val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
    assertDomainMetadata(snapshot, expectedValue)
  }

  private def createTxnWithDomainMetadatas(
      engine: Engine,
      tablePath: String,
      domainMetadatas: Seq[DomainMetadata],
      useInternalApi: Boolean = false): Transaction = {

    var txnBuilder = createWriteTxnBuilder(TableImpl.forPath(engine, tablePath))

    if (useInternalApi) {
      val txn = txnBuilder.build(engine).asInstanceOf[TransactionImpl]
      txn.addDomainMetadatas(domainMetadatas.asJava)
      txn
    } else {
      domainMetadatas.foreach { dm =>
        if (dm.isRemoved) {
          txnBuilder = txnBuilder.withDomainMetadataRemoved(dm.getDomain())
        } else {
          txnBuilder = txnBuilder.withDomainMetadata(dm.getDomain(), dm.getConfiguration())
        }
      }
      txnBuilder.build(engine)
    }
  }

  private def commitDomainMetadataAndVerify(
      engine: Engine,
      tablePath: String,
      domainMetadatas: Seq[DomainMetadata],
      expectedValue: Map[String, DomainMetadata],
      useInternalApi: Boolean = false): Unit = {
    // Create the transaction with domain metadata and commit
    val txn = createTxnWithDomainMetadatas(engine, tablePath, domainMetadatas, useInternalApi)
    txn.commit(engine, emptyIterable())

    // Verify the final state includes the expected domain metadata
    val table = Table.forPath(engine, tablePath)
    assertDomainMetadata(table, engine, expectedValue)
  }

  // TODO we probably don't always need this since domain metadata is now automatically enabled
  private def setDomainMetadataSupport(engine: Engine, tablePath: String): Unit = {
    val protocol = new Protocol(
      3, // minReaderVersion
      7, // minWriterVersion
      Collections.emptySet(), // readerFeatures
      Set("domainMetadata").asJava // writerFeatures
    )

    val protocolAction = SingleAction.createProtocolSingleAction(protocol.toRow)
    val txn = createTxn(engine, tablePath, isNewTable = false, testSchema, Seq.empty)
    txn.commit(engine, inMemoryIterable(toCloseableIterator(Seq(protocolAction).asJava.iterator())))
  }

  private def createTableWithDomainMetadataSupported(engine: Engine, tablePath: String): Unit = {
    // Create an empty table
    createTxn(engine, tablePath, isNewTable = true, testSchema, Seq.empty)
      .commit(engine, emptyIterable())

    // Set writer version and writer feature to support domain metadata
    setDomainMetadataSupport(engine, tablePath)
  }

  private def validateDomainMetadataConflictResolution(
      engine: Engine,
      tablePath: String,
      currentTxn1DomainMetadatas: Seq[DomainMetadata],
      winningTxn2DomainMetadatas: Seq[DomainMetadata],
      winningTxn3DomainMetadatas: Seq[DomainMetadata],
      expectedConflict: Boolean): Unit = {
    // Create table with domain metadata support
    createTableWithDomainMetadataSupported(engine, tablePath)
    val table = Table.forPath(engine, tablePath)

    /**
     * Txn1: i.e. the current transaction that comes later than winning transactions.
     * Txn2: i.e. the winning transaction that was committed first.
     * Txn3: i.e. the winning transaction that was committed secondly.
     *
     * Note tx is the timestamp.
     *
     * t1 ------------------------ Txn1 starts.
     * t2 ------- Txn2 starts.
     * t3 ------- Txn2 commits.
     * t4 ------- Txn3 starts.
     * t5 ------- Txn3 commits.
     * t6 ------------------------ Txn1 commits (SUCCESS or FAIL).
     */
    val txn1 = createTxnWithDomainMetadatas(engine, tablePath, currentTxn1DomainMetadatas)

    val txn2 = createTxnWithDomainMetadatas(engine, tablePath, winningTxn2DomainMetadatas)
    txn2.commit(engine, emptyIterable())

    val txn3 = createTxnWithDomainMetadatas(engine, tablePath, winningTxn3DomainMetadatas)
    txn3.commit(engine, emptyIterable())

    if (expectedConflict) {
      // We expect the commit of txn1 to fail because of the conflicting DM actions
      val ex = intercept[KernelException] {
        txn1.commit(engine, emptyIterable())
      }
      assert(
        ex.getMessage.contains(
          "A concurrent writer added a domainMetadata action for the same domain"))
    } else {
      // We expect the commit of txn1 to succeed
      txn1.commit(engine, emptyIterable())
      // Verify the final state includes merged domain metadata
      val expectedMetadata =
        (winningTxn2DomainMetadatas ++ winningTxn3DomainMetadatas ++ currentTxn1DomainMetadatas)
          .groupBy(_.getDomain)
          .mapValues(_.last)
      assertDomainMetadata(table, engine, expectedMetadata)
    }
  }

  test("create table w/o domain metadata") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)

      // Create an empty table
      createTxn(engine, tablePath, isNewTable = true, testSchema, Seq.empty)
        .commit(engine, emptyIterable())

      // Verify that the table doesn't have any domain metadata
      assertDomainMetadata(table, engine, Map.empty)
    }
  }

  test("table w/o domain metadata support fails domain metadata commits") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create an empty table
      // Its minWriterVersion is 2 and doesn't have 'domainMetadata' in its writerFeatures
      createTxn(engine, tablePath, isNewTable = true, testSchema, Seq.empty)
        .commit(engine, emptyIterable())

      val dm1 = new DomainMetadata("domain1", "", false)
      // We use the internal API because our public API will automatically upgrade the protocol
      val txn1 = createTxnWithDomainMetadatas(engine, tablePath, List(dm1), useInternalApi = true)

      // We expect the commit to fail because the table doesn't support domain metadata
      val e = intercept[KernelException] {
        txn1.commit(engine, emptyIterable())
      }
      assert(
        e.getMessage
          .contains(
            "Cannot commit DomainMetadata action(s) because the feature 'domainMetadata' "
              + "is not supported on this table."))

      // Set writer version and writer feature to support domain metadata
      setDomainMetadataSupport(engine, tablePath)

      // Commit domain metadata again and expect success
      commitDomainMetadataAndVerify(
        engine,
        tablePath,
        domainMetadatas = Seq(dm1),
        expectedValue = Map("domain1" -> dm1))
    }
  }

  test("multiple DomainMetadatas for the same domain should fail in single transaction") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithDomainMetadataSupported(engine, tablePath)

      val dm1_1 = new DomainMetadata("domain1", """{"key1":"1"}""", false)
      val dm2 = new DomainMetadata("domain2", "", false)
      val dm1_2 = new DomainMetadata("domain1", """{"key1":"10"}"""", false)

      // use internal API because public API overrides multiple domains with the same identifier
      val txn = createTxnWithDomainMetadatas(
        engine,
        tablePath,
        List(dm1_1, dm2, dm1_2),
        useInternalApi = true)

      val e = intercept[IllegalArgumentException] {
        txn.commit(engine, emptyIterable())
      }
      assert(
        e.getMessage.contains(
          "Multiple actions detected for domain 'domain1' in single transaction"))
    }
  }

  test("latest domain metadata overwriting existing ones") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithDomainMetadataSupported(engine, tablePath)

      val dm1 = new DomainMetadata("domain1", """{"key1":"1"}, {"key2":"2"}""", false)
      val dm2 = new DomainMetadata("domain2", "", false)
      val dm3 = new DomainMetadata("domain3", """{"key3":"3"}""", false)

      val dm1_2 = new DomainMetadata("domain1", """{"key1":"10"}""", false)
      val dm3_2 = new DomainMetadata("domain3", """{"key3":"30"}""", false)

      Seq(
        (Seq(dm1), Map("domain1" -> dm1)),
        (Seq(dm2, dm3, dm1_2), Map("domain1" -> dm1_2, "domain2" -> dm2, "domain3" -> dm3)),
        (Seq(dm3_2), Map("domain1" -> dm1_2, "domain2" -> dm2, "domain3" -> dm3_2))).foreach {
        case (domainMetadatas, expectedValue) =>
          commitDomainMetadataAndVerify(engine, tablePath, domainMetadatas, expectedValue)
      }
    }
  }

  test("domain metadata persistence across log replay") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithDomainMetadataSupported(engine, tablePath)

      val dm1 = new DomainMetadata("domain1", """{"key1":"1"}, {"key2":"2"}""", false)
      val dm2 = new DomainMetadata("domain2", "", false)

      commitDomainMetadataAndVerify(
        engine,
        tablePath,
        domainMetadatas = Seq(dm1, dm2),
        expectedValue = Map("domain1" -> dm1, "domain2" -> dm2))

      // Restart the table and verify the domain metadata
      val table2 = Table.forPath(engine, tablePath)
      assertDomainMetadata(table2, engine, Map("domain1" -> dm1, "domain2" -> dm2))
    }
  }

  test("only the latest domain metadata per domain is stored in checkpoints") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      createTableWithDomainMetadataSupported(engine, tablePath)

      val dm1 = new DomainMetadata("domain1", """{"key1":"1"}, {"key2":"2"}""", false)
      val dm2 = new DomainMetadata("domain2", "", false)
      val dm3 = new DomainMetadata("domain3", """{"key3":"3"}""", false)
      val dm1_2 = new DomainMetadata("domain1", """{"key1":"10"}""", false)
      val dm3_2 = new DomainMetadata("domain3", """{"key3":"3"}""", true)

      Seq(
        (Seq(dm1), Map("domain1" -> dm1)),
        (Seq(dm2), Map("domain1" -> dm1, "domain2" -> dm2)),
        (Seq(dm3), Map("domain1" -> dm1, "domain2" -> dm2, "domain3" -> dm3)),
        (
          Seq(dm1_2, dm3_2),
          Map("domain1" -> dm1_2, "domain2" -> dm2, "domain3" -> dm3_2))).foreach {
        case (domainMetadatas, expectedValue) =>
          commitDomainMetadataAndVerify(engine, tablePath, domainMetadatas, expectedValue)
      }

      // Checkpoint the table
      val latestVersion = table.getLatestSnapshot(engine).getVersion()
      table.checkpoint(engine, latestVersion)

      // Verify that only the latest domain metadata is persisted in the checkpoint
      val table2 = Table.forPath(engine, tablePath)
      assertDomainMetadata(
        table2,
        engine,
        Map("domain1" -> dm1_2, "domain2" -> dm2, "domain3" -> dm3_2))
    }
  }

  test("Conflict resolution - one of three concurrent txns has DomainMetadata") {
    withTempDirAndEngine { (tablePath, engine) =>
      /**
       * Txn1: include DomainMetadata action.
       * Txn2: does NOT include DomainMetadata action.
       * Txn3: does NOT include DomainMetadata action.
       *
       * t1 ------------------------ Txn1 starts.
       * t2 ------- Txn2 starts.
       * t3 ------- Txn2 commits.
       * t4 ------- Txn3 starts.
       * t5 ------- Txn3 commits.
       * t6 ------------------------ Txn1 commits (SUCCESS).
       */
      val dm1 = new DomainMetadata("domain1", "", false)

      validateDomainMetadataConflictResolution(
        engine,
        tablePath,
        currentTxn1DomainMetadatas = Seq(dm1),
        winningTxn2DomainMetadatas = Seq.empty,
        winningTxn3DomainMetadatas = Seq.empty,
        expectedConflict = false)
    }
  }

  test(
    "Conflict resolution - three concurrent txns have DomainMetadata w/o conflicting domains") {
    withTempDirAndEngine { (tablePath, engine) =>
      /**
       * Txn1: include DomainMetadata action for "domain1".
       * Txn2: include DomainMetadata action for "domain2".
       * Txn3: include DomainMetadata action for "domain3".
       *
       * t1 ------------------------ Txn1 starts.
       * t2 ------- Txn2 starts.
       * t3 ------- Txn2 commits.
       * t4 ------- Txn3 starts.
       * t5 ------- Txn3 commits.
       * t6 ------------------------ Txn1 commits (SUCCESS).
       */
      val dm1 = new DomainMetadata("domain1", "", false)
      val dm2 = new DomainMetadata("domain2", "", false)
      val dm3 = new DomainMetadata("domain3", "", false)

      validateDomainMetadataConflictResolution(
        engine,
        tablePath,
        currentTxn1DomainMetadatas = Seq(dm1),
        winningTxn2DomainMetadatas = Seq(dm2),
        winningTxn3DomainMetadatas = Seq(dm3),
        expectedConflict = false)
    }
  }

  test(
    "Conflict resolution - three concurrent txns have DomainMetadata w/ conflicting domains") {
    withTempDirAndEngine { (tablePath, engine) =>
      /**
       * Txn1: include DomainMetadata action for "domain1".
       * Txn2: include DomainMetadata action for "domain2".
       * Txn3: include DomainMetadata action for "domain1".
       *
       * t1 ------------------------ Txn1 starts.
       * t2 ------- Txn2 starts.
       * t3 ------- Txn2 commits.
       * t4 ------- Txn3 starts.
       * t5 ------- Txn3 commits.
       * t6 ------------------------ Txn1 commits (FAIL).
       */
      val dm1 = new DomainMetadata("domain1", "", false)
      val dm2 = new DomainMetadata("domain2", "", false)
      val dm3 = new DomainMetadata("domain1", "", false)

      validateDomainMetadataConflictResolution(
        engine,
        tablePath,
        currentTxn1DomainMetadatas = Seq(dm1),
        winningTxn2DomainMetadatas = Seq(dm2),
        winningTxn3DomainMetadatas = Seq(dm3),
        expectedConflict = true)
    }
  }

  test(
    "Conflict resolution - three concurrent txns have DomainMetadata w/ conflict domains - 2") {
    withTempDirAndEngine { (tablePath, engine) =>
      /**
       * Txn1: include DomainMetadata action for "domain1".
       * Txn2: include DomainMetadata action for "domain1".
       * Txn3: include DomainMetadata action for "domain2".
       *
       * t1 ------------------------ Txn1 starts.
       * t2 ------- Txn2 starts.
       * t3 ------- Txn2 commits.
       * t4 ------- Txn3 starts.
       * t5 ------- Txn3 commits.
       * t6 ------------------------ Txn1 commits (FAIL).
       */
      val dm1 = new DomainMetadata("domain1", "", false)
      val dm2 = new DomainMetadata("domain1", "", false)
      val dm3 = new DomainMetadata("domain2", "", false)

      validateDomainMetadataConflictResolution(
        engine,
        tablePath,
        currentTxn1DomainMetadatas = Seq(dm1),
        winningTxn2DomainMetadatas = Seq(dm2),
        winningTxn3DomainMetadatas = Seq(dm3),
        expectedConflict = true)
    }
  }

  test("Integration test - create a table with Spark and read its domain metadata using Kernel") {
    withTempDir(dir => {
      val tbl = "tbl"
      withTable(tbl) {
        val tablePath = dir.getCanonicalPath
        // Create table with domain metadata enabled
        spark.sql(s"CREATE TABLE $tbl (id LONG) USING delta LOCATION '$tablePath'")
        spark.sql(
          s"ALTER TABLE $tbl SET TBLPROPERTIES(" +
            s"'delta.feature.domainMetadata' = 'enabled'," +
            s"'delta.checkpointInterval' = '3')")

        // Manually commit domain metadata actions. This will create 02.json
        val deltaLog = DeltaLog.forTable(spark, new Path(tablePath))
        deltaLog
          .startTransaction()
          .commitManually(
            List(
              SparkDomainMetadata("testDomain1", "{\"key1\":\"1\"}", removed = false),
              SparkDomainMetadata("testDomain2", "", removed = false),
              SparkDomainMetadata("testDomain3", "", removed = false)): _*)

        // This will create 03.json and 03.checkpoint
        spark.range(0, 2).write.format("delta").mode("append").save(tablePath)

        // Manually commit domain metadata actions. This will create 04.json
        deltaLog
          .startTransaction()
          .commitManually(
            List(
              SparkDomainMetadata("testDomain1", "{\"key1\":\"10\"}", removed = false),
              SparkDomainMetadata("testDomain2", "", removed = true)): _*)

        // Use Delta Kernel to read the table's domain metadata and verify the result.
        // We will need to read 1 checkpoint file and 1 log file to replay the table.
        // The state of the domain metadata should be:
        // testDomain1: "{\"key1\":\"10\"}", removed = false  (from 03.checkpoint)
        // testDomain2: "", removed = true                    (from 03.checkpoint)
        // testDomain3: "", removed = false                   (from 04.json)

        val dm1 = new DomainMetadata("testDomain1", """{"key1":"10"}""", false)
        val dm2 = new DomainMetadata("testDomain2", "", true)
        val dm3 = new DomainMetadata("testDomain3", "", false)

        val snapshot = latestSnapshot(tablePath).asInstanceOf[SnapshotImpl]
        assertDomainMetadata(
          snapshot,
          Map("testDomain1" -> dm1, "testDomain2" -> dm2, "testDomain3" -> dm3))
      }
    })
  }

  test("Integration test - create a table using Kernel and read its domain metadata using Spark") {
    withTempDirAndEngine { (tablePath, engine) =>
      val tbl = "tbl"
      withTable(tbl) {
        // Create table with domain metadata enabled
        createTableWithDomainMetadataSupported(engine, tablePath)

        // Manually commit three domain metadata actions
        val dm1 = new DomainMetadata("testDomain1", """{"key1":"1"}""", false)
        val dm2 = new DomainMetadata("testDomain2", "", false)
        val dm3 = new DomainMetadata("testDomain3", "", false)
        commitDomainMetadataAndVerify(
          engine,
          tablePath,
          domainMetadatas = Seq(dm1, dm2, dm3),
          expectedValue = Map("testDomain1" -> dm1, "testDomain2" -> dm2, "testDomain3" -> dm3))

        appendData(
          engine,
          tablePath,
          data = Seq(Map.empty[String, Literal] -> dataBatches1))

        // Checkpoint the table so domain metadata is distributed to both checkpoint and log files
        val table = Table.forPath(engine, tablePath)
        val latestVersion = table.getLatestSnapshot(engine).getVersion()
        table.checkpoint(engine, latestVersion)

        // Manually commit two domain metadata actions
        val dm1_2 = new DomainMetadata("testDomain1", """{"key1":"10"}""", false)
        val dm2_2 = new DomainMetadata("testDomain2", "", true)
        commitDomainMetadataAndVerify(
          engine,
          tablePath,
          domainMetadatas = Seq(dm1_2, dm2_2),
          expectedValue = Map("testDomain1" -> dm1_2, "testDomain2" -> dm2_2, "testDomain3" -> dm3))

        // Use Spark to read the table's domain metadata and verify the result
        val deltaLog = DeltaLog.forTable(spark, new Path(tablePath))
        val domainMetadata = deltaLog.snapshot.domainMetadata.groupBy(_.domain).map {
          case (name, domains) =>
            assert(domains.size == 1)
            name -> domains.head
        }
        // Note that in Delta-Spark, the deltaLog.snapshot.domainMetadata does not include
        // domain metadata that are removed.
        assert(
          domainMetadata === Map(
            "testDomain1" -> SparkDomainMetadata(
              "testDomain1",
              """{"key1":"10"}""",
              removed = false),
            "testDomain3" -> SparkDomainMetadata("testDomain3", "", removed = false)))
      }
    }
  }

  test("RowTrackingMetadataDomain can be committed and read") {
    withTempDirAndEngine((tablePath, engine) => {
      val rowTrackingMetadataDomain = new RowTrackingMetadataDomain(10)
      val dmAction = rowTrackingMetadataDomain.toDomainMetadata

      // The configuration string should be a JSON serialization of the rowTrackingMetadataDomain
      assert(dmAction.getDomain === rowTrackingMetadataDomain.getDomainName)
      assert(dmAction.getConfiguration === """{"rowIdHighWaterMark":10}""")

      // Commit the DomainMetadata action and verify
      createTableWithDomainMetadataSupported(engine, tablePath)
      commitDomainMetadataAndVerify(
        engine,
        tablePath,
        domainMetadatas = Seq(dmAction),
        expectedValue = Map(rowTrackingMetadataDomain.getDomainName -> dmAction),
        useInternalApi = true // cannot commit system-controlled domains through public API
      )

      // Read the RowTrackingMetadataDomain from the table and verify
      val table = Table.forPath(engine, tablePath)
      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      val rowTrackingMetadataDomainFromSnapshot =
        RowTrackingMetadataDomain.fromSnapshot(snapshot)

      assert(rowTrackingMetadataDomainFromSnapshot.isPresent)
      assert(rowTrackingMetadataDomain === rowTrackingMetadataDomainFromSnapshot.get)
    })
  }

  test("RowTrackingMetadataDomain Integration test - Write with Spark and read with Kernel") {
    withTempDirAndEngine((tablePath, engine) => {
      val tbl = "tbl"
      withTable(tbl) {
        // Create table with domain metadata enabled using Spark
        spark.sql(s"CREATE TABLE $tbl (id LONG) USING delta LOCATION '$tablePath'")
        spark.sql(
          s"ALTER TABLE $tbl SET TBLPROPERTIES(" +
            s"'delta.feature.domainMetadata' = 'enabled'," +
            s"'delta.feature.rowTracking' = 'supported')")

        // Append 100 rows to the table, with fresh row IDs from 0 to 99
        // The `delta.rowTracking.rowIdHighWaterMark` should be 99
        spark.range(0, 20).write.format("delta").mode("append").save(tablePath)
        spark.range(20, 100).write.format("delta").mode("append").save(tablePath)

        // Read the RowTrackingMetadataDomain from the table using Kernel
        val table = Table.forPath(engine, tablePath)
        val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
        val rowTrackingMetadataDomainRead = RowTrackingMetadataDomain.fromSnapshot(snapshot)

        assert(rowTrackingMetadataDomainRead.isPresent)
        assert(rowTrackingMetadataDomainRead.get.getRowIdHighWaterMark === 99)
      }
    })
  }

  test("RowTrackingMetadataDomain Integration test - Write with Kernel and read with Spark") {
    withTempDirAndEngine { (tablePath, engine) =>
      val tbl = "tbl"
      withTable(tbl) {
        // Create table and manually make changes to the row tracking metadata domain using Kernel
        createTableWithDomainMetadataSupported(engine, tablePath)
        val dmAction = new RowTrackingMetadataDomain(10).toDomainMetadata
        commitDomainMetadataAndVerify(
          engine,
          tablePath,
          domainMetadatas = Seq(dmAction),
          expectedValue = Map(dmAction.getDomain -> dmAction),
          useInternalApi = true // cannot commit system-controlled domains through public API
        )

        // Use Spark to read the table's row tracking metadata domain and verify the result
        val deltaLog = DeltaLog.forTable(spark, new Path(tablePath))
        val rowTrackingMetadataDomainRead =
          SparkRowTrackingMetadataDomain.fromSnapshot(deltaLog.snapshot)
        assert(rowTrackingMetadataDomainRead.exists(_.rowIdHighWaterMark === 10))
      }
    }
  }

  test("basic txnBuilder.withDomainMetadata API tests") {
    // withDomainMetadata is tested thoroughly elsewhere in this suite, here we just test API
    // specific behaviors

    // Cannot set system-controlled domain metadata
    Seq("delta.foo", "DELTA.foo").foreach { domain =>
      val e = intercept[IllegalArgumentException] {
        createWriteTxnBuilder(Table.forPath(defaultEngine, "fake/path"))
          .withDomainMetadata(domain, "misc config")
      }
      assert(e.getMessage.contains("Setting a system-controlled domain is not allowed"))
    }

    // Setting the same domain more than once uses the latest pair
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithDomainMetadataSupported(engine, tablePath)

      val dm1_1 = new DomainMetadata("domain1", """{"key1":"1"}""", false)
      val dm1_2 = new DomainMetadata("domain1", """{"key1":"10"}"""", false)

      commitDomainMetadataAndVerify(engine, tablePath, List(dm1_1, dm1_2), Map("domain1" -> dm1_2))
    }
  }

  test("basic txnBuilder.withDomainMetadataRemoved API tests") {
    // withDomainMetadataRemoved is tested thoroughly elsewhere in this suite, here we just test API
    // specific behaviors

    // Cannot remove system-controlled domain metadata
    Seq("delta.foo", "DELTA.foo").foreach { domain =>
      val e = intercept[IllegalArgumentException] {
        createWriteTxnBuilder(Table.forPath(defaultEngine, "fake/path"))
          .withDomainMetadataRemoved(domain)
      }

      assert(e.getMessage.contains("Removing a system-controlled domain is not allowed"))
    }

    // Can remove same domain more than once in same txn
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithDomainMetadataSupported(engine, tablePath)

      // Set up table with domain "domain1
      val dm1 = new DomainMetadata("domain1", """{"key1":"1"}""", false)
      commitDomainMetadataAndVerify(engine, tablePath, List(dm1), Map("domain1" -> dm1))

      val dm1_removed = dm1.removed()
      commitDomainMetadataAndVerify(
        engine,
        tablePath,
        List(dm1_removed, dm1_removed, dm1_removed),
        Map("domain1" -> dm1_removed))
    }
  }

  test("txnBuilder.withDomainMetadataRemoved removing a non-existent domain") {
    // Remove domain that does not exist and has never existed
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithDomainMetadataSupported(engine, tablePath)

      intercept[DomainDoesNotExistException] {
        createWriteTxnBuilder(Table.forPath(defaultEngine, tablePath))
          .withDomainMetadataRemoved("foo")
          .build(defaultEngine)
      }
    }

    // Remove domain that exists as a tombstone
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithDomainMetadataSupported(engine, tablePath)

      // Set up table with domain "domain1"
      val dm1 = new DomainMetadata("domain1", """{"key1":"1"}""", false)
      commitDomainMetadataAndVerify(engine, tablePath, List(dm1), Map("domain1" -> dm1))

      // Remove domain1 so it exists as a tombstone
      val dm1_removed = dm1.removed()
      commitDomainMetadataAndVerify(
        engine,
        tablePath,
        List(dm1_removed),
        Map("domain1" -> dm1_removed))

      // Removing it again should fail since it doesn't exist
      intercept[DomainDoesNotExistException] {
        commitDomainMetadataAndVerify(
          engine,
          tablePath,
          List(dm1_removed),
          Map("domain1" -> dm1_removed))
      }
    }
  }

  test("Using add and remove with the same domain in the same txn") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithDomainMetadataSupported(engine, tablePath)
      // We forbid adding + removing a domain with the same identifier in a transaction to avoid
      // any ambiguous behavior
      // For example, is the expected behavior
      // a) we don't write any domain metadata, and it's a no-op (remove cancels out the add)
      // b) we remove the previous domain from the read snapshot, and add the new one as the current
      //    domain metadata

      {
        val e = intercept[IllegalArgumentException] {
          createWriteTxnBuilder(Table.forPath(defaultEngine, tablePath))
            .withDomainMetadata("foo", "fake config")
            .withDomainMetadataRemoved("foo")
        }
        assert(e.getMessage.contains("Cannot remove a domain that is added in this transaction"))
      }
      {
        val e = intercept[IllegalArgumentException] {
          createWriteTxnBuilder(Table.forPath(defaultEngine, tablePath))
            .withDomainMetadataRemoved("foo")
            .withDomainMetadata("foo", "fake config")
        }
        assert(e.getMessage.contains("Cannot add a domain that is removed in this transaction"))
      }
    }
  }

  test("basic snapshot.getDomainMetadataConfiguration API tests") {
    // getDomainMetadataConfiguration is tested thoroughly elsewhere in this suite, here we just
    // test the API directly to be safe

    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithDomainMetadataSupported(engine, tablePath)

      // Non-existent domain is not returned
      assert(!latestSnapshot(tablePath).getDomainMetadata("foo").isPresent)

      // Commit domain foo
      val fooDm = new DomainMetadata("foo", "foo!", false)
      commitDomainMetadataAndVerify(engine, tablePath, List(fooDm), Map("foo" -> fooDm))
      assert( // Check here even though already verified in commitDomainMetadataAndVerify
        latestSnapshot(tablePath).getDomainMetadata("foo").toScala.contains("foo!"))

      // Remove domain foo (so tombstone exists but should not be returned)
      val fooDm_removed = fooDm.removed()
      commitDomainMetadataAndVerify(
        engine,
        tablePath,
        List(fooDm_removed),
        Map("foo" -> fooDm_removed))
      // Already checked in commitDomainMetadataAndVerify but check again
      assert(!latestSnapshot(tablePath).getDomainMetadata("foo").isPresent)
    }
  }

  /* --------------- Automatic upgrade table feature tests -------------- */

  private def verifyDomainMetadataFeatureSupport(
      engine: Engine,
      tablePath: String,
      isSupported: Boolean = true): Unit = {
    val snapshotImpl = Table.forPath(engine, tablePath).getLatestSnapshot(engine)
      .asInstanceOf[SnapshotImpl]
    assert(TableFeatures.isDomainMetadataSupported(snapshotImpl.getProtocol) == isSupported)
  }

  /**
   * For the given tablePath, commit a transaction that sets a domain metadata using
   * withDomainMetadata when building the transaction (which should automatically upgrade the table
   * protocol). Verifies that the domain metadata table feature is unsupported before committing
   * (for an existing table) and is supported after committing.
   *
   * @param isNewTable if true, sets a schema during transaction building to create a new table
   * @param tableProperties if provided sets the table properties during transaction building
   */
  private def verifyAutomaticUpgrade(
      engine: Engine,
      tablePath: String,
      isNewTable: Boolean = false,
      tableProperties: Option[Map[String, String]] = None): Unit = {
    if (!isNewTable) {
      // Verify it's not supported yet
      verifyDomainMetadataFeatureSupport(engine, tablePath, isSupported = false)
    }
    // Commit to table using withDomainMetadata and upgrade protocol
    var txnBuilder = createWriteTxnBuilder(Table.forPath(engine, tablePath))
      .withDomainMetadata("foo", "configuration string")
    if (isNewTable) {
      txnBuilder = txnBuilder.withSchema(engine, testSchema)
    }
    if (tableProperties.nonEmpty) {
      txnBuilder = txnBuilder.withTableProperties(engine, tableProperties.get.asJava)
    }
    txnBuilder.build(engine).commit(engine, emptyIterable())
    // Check the feature is now supported
    verifyDomainMetadataFeatureSupport(engine, tablePath)
  }

  test("automatically enable DomainMetadata when using withDomainMetadata - new table") {
    // New table using the withDomainMetadata API
    withTempDirAndEngine { (tablePath, engine) =>
      verifyAutomaticUpgrade(engine, tablePath, isNewTable = true)
    }
  }

  test("automatically enable DomainMetadata when using withDomainMetadata - existing " +
    "table with legacy protocol") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create table with legacy protocol
      createTxn(tablePath = tablePath, isNewTable = true, schema = testSchema, partCols = Seq())
        .commit(engine, emptyIterable())
      verifyAutomaticUpgrade(engine, tablePath)
    }
  }

  test("automatically enable DomainMetadata when using withDomainMetadata - existing " +
    "table with TF protocol") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create table with table feature protocol
      createTxn(
        tablePath = tablePath,
        isNewTable = true,
        schema = testSchema,
        partCols = Seq(),
        // Enable inCommitTimestamps to bump the protocol
        tableProperties = Map(TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true"))
        .commit(engine, emptyIterable())
      verifyAutomaticUpgrade(engine, tablePath)
    }
  }

  test("automatically enable DomainMetadata when using withDomainMetadata - existing " +
    "table, upgrade two TF in same txn") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create table with legacy protocol
      createTxn(tablePath = tablePath, isNewTable = true, schema = testSchema, partCols = Seq())
        .commit(engine, emptyIterable())
      verifyAutomaticUpgrade(
        engine,
        tablePath,
        // Enable inCommitTimestamps as well
        tableProperties = Some(Map(TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true")))
    }
  }

  test("removing a domain on a table without DomainMetadata support") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create table with legacy protocol
      createTxn(tablePath = tablePath, isNewTable = true, schema = testSchema, partCols = Seq())
        .commit(engine, emptyIterable())
      intercept[DomainDoesNotExistException] {
        createWriteTxnBuilder(Table.forPath(engine, tablePath))
          .withDomainMetadataRemoved("foo")
          .build(engine)
      }
    }
  }
}
