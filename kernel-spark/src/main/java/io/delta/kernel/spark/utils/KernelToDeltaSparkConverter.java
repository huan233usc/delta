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
package io.delta.kernel.spark.utils;

import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.SingleAction;

/** Utility class to convert Kernel Protocol and Metadata to Delta-Spark Protocol and Metadata. */
public class KernelToDeltaSparkConverter {

  /** Convert Kernel Protocol to Delta-Spark Protocol via JSON */
  public static io.delta.kernel.shaded.org.apache.spark.sql.delta.actions.Protocol convertProtocol(
      io.delta.kernel.internal.actions.Protocol kernelProtocol) {

    // Convert kernel Protocol to Row
    Row protocolRow = kernelProtocol.toRow();

    // Wrap in a SingleAction Row using kernel's SingleAction helper
    Row singleActionRow = SingleAction.createProtocolSingleAction(protocolRow);

    // Convert SingleAction Row to JSON
    String singleActionJson = JsonUtils.rowToJson(singleActionRow);

    // Deserialize using Action.fromJson
    io.delta.kernel.shaded.org.apache.spark.sql.delta.actions.Action action =
        io.delta.kernel.shaded.org.apache.spark.sql.delta.actions.Action.fromJson(singleActionJson);
    return action.wrap().protocol();
  }

  /** Convert Kernel Metadata to Delta-Spark Metadata via JSON */
  public static io.delta.kernel.shaded.org.apache.spark.sql.delta.actions.Metadata convertMetadata(
      io.delta.kernel.internal.actions.Metadata kernelMetadata) {
    // Convert kernel Metadata to Row
    Row metadataRow = kernelMetadata.toRow();

    // Wrap in a SingleAction Row using kernel's SingleAction helper
    Row singleActionRow = SingleAction.createMetadataSingleAction(metadataRow);

    // Convert SingleAction Row to JSON
    String singleActionJson = JsonUtils.rowToJson(singleActionRow);

    // Deserialize using Action.fromJson
    io.delta.kernel.shaded.org.apache.spark.sql.delta.actions.Action action =
        io.delta.kernel.shaded.org.apache.spark.sql.delta.actions.Action.fromJson(singleActionJson);
    return action.wrap().metaData();
  }

  /** Convert both Protocol and Metadata from a Snapshot */
  public static ProtocolAndMetadata convertFromSnapshot(SnapshotImpl snapshot) {
    return new ProtocolAndMetadata(
        convertProtocol(snapshot.getProtocol()), convertMetadata(snapshot.getMetadata()));
  }

  /** Container class for Protocol and Metadata pair */
  public static class ProtocolAndMetadata {
    public final io.delta.kernel.shaded.org.apache.spark.sql.delta.actions.Protocol protocol;
    public final io.delta.kernel.shaded.org.apache.spark.sql.delta.actions.Metadata metadata;

    public ProtocolAndMetadata(
        io.delta.kernel.shaded.org.apache.spark.sql.delta.actions.Protocol protocol,
        io.delta.kernel.shaded.org.apache.spark.sql.delta.actions.Metadata metadata) {
      this.protocol = protocol;
      this.metadata = metadata;
    }
  }
}
