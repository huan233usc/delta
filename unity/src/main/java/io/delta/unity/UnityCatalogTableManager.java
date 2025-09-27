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

package io.delta.unity;

import io.delta.kernel.Snapshot;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.files.ParsedDeltaData;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.spark.catalog.utils.DeltaTableManager;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.VersionNotFoundException;

/**
 * Unity Catalog implementation of DeltaTableManager. Located in delta-unity module for clean
 * separation of UC-specific logic. Handles UC API → CCv2 protocol translation.
 */
public class UnityCatalogTableManager implements DeltaTableManager {

  /** Prefix for Spark SQL catalog configurations. */
  private static final String SPARK_SQL_CATALOG_PREFIX = "spark.sql.catalog.";

  /** Connector class name for filtering relevant Unity Catalog catalogs. */
  private static final String UNITY_CATALOG_CONNECTOR_CLASS =
      "io.unitycatalog.spark.UCSingleCatalog";

  /** Suffix for the URI configuration of a catalog. */
  private static final String URI_SUFFIX = "uri";

  /** Suffix for the token configuration of a catalog. */
  private static final String TOKEN_SUFFIX = "token";

  private UCCatalogManagedClient ccv2Client;
  private String tablePath;
  private String ucTableId;
  private Engine kernelEngine;
  private final AtomicReference<Snapshot> cachedSnapshot;

  // Required no-arg constructor for dynamic loading
  public UnityCatalogTableManager() {
    this.cachedSnapshot = new AtomicReference<>();
  }

  @Override
  public void initialize(String tablePath, Map<String, String> properties) {
    this.tablePath = tablePath;

    // Extract UC-specific properties
    this.ucTableId = properties.get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
    if (ucTableId == null || ucTableId.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "UC table ID not found in properties. "
              + "Expected key: "
              + UCCommitCoordinatorClient.UC_TABLE_ID_KEY);
    }

    String catalogName = extractCatalogName(properties);
    UriAndToken credentials = extractUCCredentials(catalogName, properties);

    // Initialize UC client and engine
    UCClient ucClient = new UCTokenBasedRestClient(credentials.uri, credentials.token);
    this.ccv2Client = new UCCatalogManagedClient(ucClient);
    this.kernelEngine = createEngine(properties);
  }

  @Override
  public Snapshot unsafeVolatileSnapshot() {
    Snapshot cached = cachedSnapshot.get();
    return cached != null ? cached : update();
  }

  @Override
  public Snapshot update() {
    Snapshot snapshot =
        ccv2Client.loadSnapshot(kernelEngine, ucTableId, tablePath, Optional.empty());
    cachedSnapshot.set(snapshot);
    return snapshot;
  }

  @Override
  public DeltaHistoryManager.Commit getActiveCommitAtTime(
      Timestamp timeStamp,
      Boolean canReturnLastCommit,
      Boolean mustBeRecreatable,
      Boolean canReturnEarliestCommit) {

    // Get commits from UC via CCv2
    GetCommitsResponse response =
        ccv2Client.getRatifiedCommitsFromUC(ucTableId, tablePath, Optional.empty());

    List<ParsedDeltaData> parsedData =
        UCCatalogManagedClient.getSortedKernelParsedDeltaDataFromRatifiedCommits(
                ucTableId, response.getCommits())
            .stream()
            .filter(x -> x instanceof ParsedDeltaData && x.isFile())
            .map(ParsedDeltaData.class::cast)
            .collect(Collectors.toList());

    return DeltaHistoryManager.getActiveCommitAtTimestamp(
        kernelEngine,
        (SnapshotImpl) update(),
        ((SnapshotImpl) update()).getLogPath(),
        timeStamp.getTime(),
        mustBeRecreatable,
        canReturnLastCommit,
        canReturnEarliestCommit,
        parsedData);
  }

  @Override
  public void checkVersionExists(Long version, Boolean mustBeRecreatable, Boolean allowOutOfRange)
      throws AnalysisException {

    // Get ratified commits to determine version bounds
    SnapshotImpl snapshot = (SnapshotImpl) update();
    GetCommitsResponse response =
        ccv2Client.getRatifiedCommitsFromUC(
            ucTableId, tablePath, Optional.of(snapshot.getVersion()));

    List<ParsedDeltaData> parsedDeltas =
        UCCatalogManagedClient.getSortedKernelParsedDeltaDataFromRatifiedCommits(
                ucTableId, response.getCommits())
            .stream()
            .filter(x -> x instanceof ParsedDeltaData && x.isFile())
            .map(ParsedDeltaData.class::cast)
            .collect(Collectors.toList());

    Optional<Long> earliestRatifiedVersion =
        parsedDeltas.stream().map(ParsedLogData::getVersion).min(Long::compare);

    long earliest =
        mustBeRecreatable
            ? DeltaHistoryManager.getEarliestRecreatableCommit(
                kernelEngine, snapshot.getLogPath(), earliestRatifiedVersion)
            : DeltaHistoryManager.getEarliestDeltaFile(
                kernelEngine, snapshot.getLogPath(), earliestRatifiedVersion);

    long latest = snapshot.getVersion();

    if (version < earliest || ((version > latest) && !allowOutOfRange)) {
      throw new VersionNotFoundException(version, earliest, latest);
    }
  }

  @Override
  public void close() {
    // Clean up resources
    if (ccv2Client != null) {
      try {
        // Close UC client connections if needed
        // UCCatalogManagedClient doesn't currently have close() method
        // but we can clear our reference
        ccv2Client = null;
      } catch (Exception e) {
        // Log error but don't fail close operation
        System.err.println("Error closing UC client: " + e.getMessage());
      }
    }

    // Clear cached snapshot
    cachedSnapshot.set(null);
  }

  @Override
  public String getManagerType() {
    return "Unity Catalog";
  }

  // Private helper methods for UC-specific configuration extraction
  private String extractCatalogName(Map<String, String> properties) {
    // Try to extract catalog name from various sources

    // First, try from explicit catalog name property
    String catalogName = properties.get("catalog.name");
    if (catalogName != null && !catalogName.trim().isEmpty()) {
      return catalogName;
    }

    // Next, try to infer from spark.sql.catalog.* properties
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(SPARK_SQL_CATALOG_PREFIX) && key.endsWith("." + URI_SUFFIX)) {
        // Extract catalog name: spark.sql.catalog.{catalogName}.uri -> {catalogName}
        String prefix = key.substring(SPARK_SQL_CATALOG_PREFIX.length());
        int dotIndex = prefix.lastIndexOf("." + URI_SUFFIX);
        if (dotIndex > 0) {
          return prefix.substring(0, dotIndex);
        }
      }
    }

    // Fallback: try to get default catalog name
    String defaultCatalog = properties.get("spark.sql.catalog.spark_catalog");
    if (defaultCatalog != null) {
      return "spark_catalog";
    }

    throw new IllegalArgumentException(
        "Cannot determine Unity Catalog name from properties. "
            + "Please ensure 'catalog.name' property is set or Unity Catalog is properly configured.");
  }

  private UriAndToken extractUCCredentials(String catalogName, Map<String, String> properties) {
    String uriKey = SPARK_SQL_CATALOG_PREFIX + catalogName + "." + URI_SUFFIX;
    String tokenKey = SPARK_SQL_CATALOG_PREFIX + catalogName + "." + TOKEN_SUFFIX;

    String uri = properties.get(uriKey);
    String token = properties.get(tokenKey);

    if (uri == null || uri.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "UC URI not found for catalog: " + catalogName + ". Expected property: " + uriKey);
    }

    if (token == null || token.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "UC token not found for catalog: " + catalogName + ". Expected property: " + tokenKey);
    }

    return new UriAndToken(uri, token);
  }

  private Engine createEngine(Map<String, String> properties) {
    // Create Hadoop configuration from properties
    Configuration hadoopConf = SparkSession.active().sessionState().newHadoopConf();

    // Apply storage and Hadoop properties
    properties.entrySet().stream()
        .filter(
            entry ->
                entry.getKey().startsWith("hadoop.")
                    || (!entry.getKey().startsWith("spark.sql.catalog.")
                        && !entry.getKey().startsWith("delta.")
                        && !entry.getKey().equals("table.path")
                        && !entry.getKey().equals("catalog.name")
                        && !entry.getKey().equals(UCCommitCoordinatorClient.UC_TABLE_ID_KEY)))
        .forEach(
            entry -> {
              String key =
                  entry.getKey().startsWith("hadoop.")
                      ? entry.getKey().substring(7) // Remove "hadoop." prefix
                      : entry.getKey();
              hadoopConf.set(key, entry.getValue());
            });

    return DefaultEngine.create(hadoopConf);
  }

  private static class UriAndToken {
    final String uri;
    final String token;

    UriAndToken(String uri, String token) {
      this.uri = uri;
      this.token = token;
    }
  }
}
