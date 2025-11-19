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

package io.delta.kernel.spark.read

import io.delta.kernel.internal.actions.{Format, Metadata, Protocol}
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.types.{StringType, StructType => KernelStructType}

import org.apache.spark.sql.delta.{ProtocolMetadataAdapter, ProtocolMetadataAdapterSuiteBase}
import org.apache.spark.sql.types.StructType

import java.util.Optional
import scala.collection.JavaConverters._

/**
 * Unit tests for ProtocolAndMetadataAdapterV2.
 *
 * This test suite extends ProtocolMetadataAdapterSuiteBase to ensure that the V2 adapter
 * (kernel-based) implementation is compatible with the V1 adapter (delta-spark based).
 */
class ProtocolAndMetadataAdapterV2Suite extends ProtocolMetadataAdapterSuiteBase {

  /**
   * Creates a ProtocolAndMetadataAdapterV2 instance for testing.
   *
   * @param minReaderVersion Protocol reader version
   * @param minWriterVersion Protocol writer version
   * @param readerFeatures Optional set of reader features
   * @param writerFeatures Optional set of writer features
   * @param schema Table schema (Spark StructType)
   * @param configuration Table properties/configuration
   */
  override protected def createWrapper(
      minReaderVersion: Int = 1,
      minWriterVersion: Int = 2,
      readerFeatures: Option[Set[String]] = None,
      writerFeatures: Option[Set[String]] = None,
      schema: StructType = new StructType().add("id", org.apache.spark.sql.types.IntegerType),
      configuration: Map[String, String] = Map.empty): ProtocolMetadataAdapter = {

    // Create kernel Protocol
    val protocol = new Protocol(
      minReaderVersion,
      minWriterVersion,
      readerFeatures.map(_.asJava).getOrElse(java.util.Collections.emptySet()),
      writerFeatures.map(_.asJava).getOrElse(java.util.Collections.emptySet())
    )

    // Convert Spark schema to Kernel schema
    val kernelSchema = convertSparkToKernelSchema(schema)
    val schemaString = kernelSchema.toJson

    // Create kernel Metadata
    val metadata = new Metadata(
      "test-id",
      Optional.of("test-table"),
      Optional.of("test description"),
      new Format("parquet", java.util.Collections.emptyMap()),
      schemaString,
      kernelSchema,
      VectorUtils.stringArrayValue(Array.empty[String]),
      Optional.of(System.currentTimeMillis()),
      VectorUtils.stringStringMapValue(configuration.asJava)
    )

    // Create and return the V2 adapter
    new ProtocolAndMetadataAdapterV2(protocol, metadata)
  }

  /**
   * Converts a Spark StructType to a Kernel StructType for testing.
   * This is a simplified conversion that handles basic cases.
   */
  private def convertSparkToKernelSchema(sparkSchema: StructType): KernelStructType = {
    var kernelSchema = new KernelStructType()
    
    sparkSchema.fields.foreach { field =>
      // Map Spark types to Kernel types (simplified for testing)
      val kernelType = field.dataType match {
        case org.apache.spark.sql.types.IntegerType => 
          io.delta.kernel.types.IntegerType.INTEGER
        case org.apache.spark.sql.types.LongType => 
          io.delta.kernel.types.LongType.LONG
        case org.apache.spark.sql.types.StringType => 
          StringType.STRING
        case org.apache.spark.sql.types.StructType(nestedFields) =>
          convertSparkToKernelSchema(org.apache.spark.sql.types.StructType(nestedFields))
        case other => 
          // For other types, default to STRING for testing
          StringType.STRING
      }
      
      kernelSchema = kernelSchema.add(field.name, kernelType, field.nullable)
    }
    
    kernelSchema
  }

  // Additional V2-specific tests can be added here
  test("ProtocolAndMetadataAdapterV2 specific: serialization") {
    val wrapper = createWrapper()
    
    // Test that the adapter is serializable
    val bytes = new java.io.ByteArrayOutputStream()
    val out = new java.io.ObjectOutputStream(bytes)
    out.writeObject(wrapper)
    out.close()
    
    val in = new java.io.ObjectInputStream(
      new java.io.ByteArrayInputStream(bytes.toByteArray))
    val deserialized = in.readObject().asInstanceOf[ProtocolAndMetadataAdapterV2]
    in.close()
    
    // Verify deserialized object works correctly
    assert(deserialized.columnMappingMode === wrapper.columnMappingMode)
    assert(deserialized.getReferenceSchema === wrapper.getReferenceSchema)
  }
}

