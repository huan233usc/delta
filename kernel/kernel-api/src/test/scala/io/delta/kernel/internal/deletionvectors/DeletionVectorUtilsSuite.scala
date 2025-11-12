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
package io.delta.kernel.internal.deletionvectors

import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

import io.delta.kernel.internal.actions.{Format, Metadata, Protocol}
import io.delta.kernel.internal.types.DataTypeJsonSerDe
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.types.{StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

/**
 * Suite to test utility methods in DeletionVectorUtils class.
 */
class DeletionVectorUtilsSuite extends AnyFunSuite {

  ///////////////////////////////////////////////////////////////////////////////////////////////
  // Helper methods
  ///////////////////////////////////////////////////////////////////////////////////////////////

  private def createProtocol(
      readerFeatures: java.util.Set[String] =
        Collections.emptySet(),
      writerFeatures: java.util.Set[String] =
        Collections.emptySet()): Protocol = {
    new Protocol(3, 7, readerFeatures, writerFeatures)
  }

  private def createMetadata(
      configuration: Map[String, String] = Map.empty,
      schema: StructType = new StructType(),
      formatProvider: String = "parquet"): Metadata = {
    new Metadata(
      "test-id",
      Optional.empty(),
      Optional.empty(),
      new Format(formatProvider, Collections.emptyMap()),
      DataTypeJsonSerDe.serializeDataType(schema),
      schema,
      VectorUtils.buildArrayValue(Collections.emptyList(), StringType.STRING),
      Optional.empty(),
      VectorUtils.stringStringMapValue(configuration.asJava))
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////
  // Tests for DeletionVectorUtils.isReadable
  ///////////////////////////////////////////////////////////////////////////////////////////////

  test("isReadable returns false when deletion vectors are not supported by protocol") {
    val protocol = createProtocol() // No deletionVectors feature
    val metadata = createMetadata()

    assert(!DeletionVectorUtils.isReadable(protocol, metadata))
  }

  test("isReadable returns false when format is not parquet") {
    val readerFeatures = new java.util.HashSet[String]()
    readerFeatures.add("deletionVectors")
    val writerFeatures = new java.util.HashSet[String]()
    writerFeatures.add("deletionVectors")
    val protocol = createProtocol(readerFeatures, writerFeatures)
    val metadata = createMetadata(formatProvider = "delta") // Non-parquet format

    assert(!DeletionVectorUtils.isReadable(protocol, metadata))
  }

  test("isReadable returns true when deletion vectors are supported and format is parquet") {
    val readerFeatures = new java.util.HashSet[String]()
    readerFeatures.add("deletionVectors")
    val writerFeatures = new java.util.HashSet[String]()
    writerFeatures.add("deletionVectors")
    val protocol = createProtocol(readerFeatures, writerFeatures)
    val metadata = createMetadata(formatProvider = "parquet")

    assert(DeletionVectorUtils.isReadable(protocol, metadata))
  }

  test("isReadable is case-insensitive for format provider") {
    val readerFeatures = new java.util.HashSet[String]()
    readerFeatures.add("deletionVectors")
    val writerFeatures = new java.util.HashSet[String]()
    writerFeatures.add("deletionVectors")
    val protocol = createProtocol(readerFeatures, writerFeatures)

    // Test different case variations
    assert(DeletionVectorUtils.isReadable(protocol, createMetadata(formatProvider = "PARQUET")))
    assert(DeletionVectorUtils.isReadable(protocol, createMetadata(formatProvider = "Parquet")))
    assert(DeletionVectorUtils.isReadable(protocol, createMetadata(formatProvider = "parquet")))
  }
}
