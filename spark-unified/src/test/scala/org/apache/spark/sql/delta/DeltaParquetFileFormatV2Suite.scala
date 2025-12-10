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

import io.delta.kernel.Table
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.internal.{InternalScanFileUtils, SnapshotImpl}
import io.delta.kernel.internal.actions.DeletionVectorDescriptor
import io.delta.kernel.spark.read.DeltaParquetFileFormatV2
import io.delta.kernel.spark.utils.SchemaUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus => HadoopFileStatus, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.{FileIndex, FileStatusWithMetadata, PartitionDirectory}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * V2 implementation of DeltaParquetFileFormat tests using DeltaParquetFileFormatV2.
 * This uses Kernel's Protocol and Metadata instead of delta-spark's.
 */
class DeltaParquetFileFormatV2Suite extends DeltaParquetFileFormatBasicSuiteBase {

  override def createFileFormatTestContext(
      tablePath: String,
      readIsRowDeletedCol: Boolean,
      readRowIndexCol: Boolean,
      enableDVs: Boolean): FileFormatTestContext = {

    // Use Kernel-based FileIndex
    val kernelFileIndex = new KernelFileIndex(spark, tablePath, hadoopConf)
    val kernelMetadata = kernelFileIndex.getMetadata
    val kernelProtocol = kernelFileIndex.getProtocol

    // Get schema from Kernel and convert to Spark
    var readingSchema = SchemaUtils.convertKernelSchemaToSparkSchema(kernelMetadata.getSchema)
    if (readIsRowDeletedCol) {
      readingSchema = readingSchema.add(DeltaParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD)
    }
    if (readRowIndexCol) {
      readingSchema = readingSchema.add(DeltaParquetFileFormat.ROW_INDEX_STRUCT_FIELD)
    }

    val deltaParquetFormat = new DeltaParquetFileFormatV2(
      kernelProtocol,
      kernelMetadata,
      false, // nullableRowTrackingConstantFields
      false, // nullableRowTrackingGeneratedFields
      false, // optimizationsEnabled
      if (enableDVs) scala.Option(tablePath) else scala.Option.empty,
      false) // isCDCRead

    FileFormatTestContext(kernelFileIndex, deltaParquetFormat, readingSchema)
  }

  /**
   * A FileIndex implementation backed by Kernel's Snapshot for testing purposes.
   * This allows DeltaParquetFileFormatV2 tests to use Kernel APIs without depending on DeltaLog.
   * Supports deletion vectors by passing DV metadata via FileStatusWithMetadata.
   */
  private class KernelFileIndex(
      spark: SparkSession,
      tablePath: String,
      hadoopConf: Configuration) extends FileIndex {

    private val engine = DefaultEngine.create(hadoopConf)
    private val table = Table.forPath(engine, tablePath)
    private val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]

    def getProtocol: io.delta.kernel.internal.actions.Protocol = snapshot.getProtocol
    def getMetadata: io.delta.kernel.internal.actions.Metadata = snapshot.getMetadata

    override def rootPaths: Seq[Path] = Seq(new Path(tablePath))

    override def listFiles(
        partitionFilters: Seq[Expression],
        dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
      // Get all files from kernel scan
      val scan = snapshot.getScanBuilder().build()
      val scanFileIter = scan.getScanFiles(engine)

      val files = new scala.collection.mutable.ArrayBuffer[FileStatusWithMetadata]()

      try {
        while (scanFileIter.hasNext) {
          val batch = scanFileIter.next()
          val rows = batch.getRows
          while (rows.hasNext) {
            val row = rows.next()
            files += fileStatusWithMetadataFromScanRow(row)
          }
          rows.close()
        }
      } finally {
        scanFileIter.close()
      }

      // Return single partition with all files (non-partitioned table for simplicity)
      Seq(PartitionDirectory(
        org.apache.spark.sql.catalyst.InternalRow.empty,
        files.toArray))
    }

    /**
     * Creates FileStatusWithMetadata from a Kernel scan file row, including DV metadata.
     * Similar to TahoeLogFileIndex.fileStatusWithMetadataFromAddFile.
     */
    private def fileStatusWithMetadataFromScanRow(scanRow: Row): FileStatusWithMetadata = {
      val fileStatus = InternalScanFileUtils.getAddFileStatus(scanRow)

      val fs = new HadoopFileStatus(
        fileStatus.getSize,
        false,
        1,
        128 * 1024 * 1024,
        fileStatus.getModificationTime,
        new Path(fileStatus.getPath))

      val metadata = mutable.Map.empty[String, Any]

      // Add deletion vector metadata if present
      val dvDescriptor = InternalScanFileUtils.getDeletionVectorDescriptorFromRow(scanRow)
      if (dvDescriptor != null) {
        metadata.put(
          DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED,
          serializeDVToBase64(dvDescriptor))
        metadata.put(
          DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE,
          RowIndexFilterType.IF_CONTAINED)
      }

      FileStatusWithMetadata(fs, metadata.toMap)
    }

    /** Serialize Kernel's DeletionVectorDescriptor to base64 format. */
    private def serializeDVToBase64(dv: DeletionVectorDescriptor): String = {
      // Convert Kernel DV to delta-spark DV format for serialization
      val sparkDV = org.apache.spark.sql.delta.actions.DeletionVectorDescriptor(
        storageType = dv.getStorageType,
        pathOrInlineDv = dv.getPathOrInlineDv,
        offset = if (dv.getOffset.isPresent) Some(dv.getOffset.get().intValue()) else None,
        sizeInBytes = dv.getSizeInBytes,
        cardinality = dv.getCardinality
      )
      sparkDV.serializeToBase64()
    }

    override def inputFiles: Array[String] = {
      listFiles(Seq.empty, Seq.empty).flatMap(_.files.map(_.getPath.toString)).toArray
    }

    override def refresh(): Unit = {
      // No-op for test purposes
    }

    override def sizeInBytes: Long = {
      listFiles(Seq.empty, Seq.empty).flatMap(_.files.map(_.getLen)).sum
    }

    override def partitionSchema: StructType = {
      val fullSchema = SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getMetadata.getSchema)
      val partitionCols = snapshot.getMetadata.getPartitionColNames.asScala.toSet
      StructType(fullSchema.filter(f => partitionCols.contains(f.name)))
    }
  }
}
