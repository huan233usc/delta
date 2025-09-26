package io.delta.kernel.spark.catalog.utils;

import io.delta.kernel.Snapshot;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.files.ParsedDeltaData;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.AnalysisException;
import io.delta.unity.UCCatalogManagedClient;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.LookupCatalog;
import org.apache.spark.sql.delta.VersionNotFoundException;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark. sql. connector. catalog. LookupCatalog. CatalogAndIdentifier;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;

public class UnityCatalogTableManager extends AbstractDeltaTableManager {

    /** Prefix for Spark SQL catalog configurations. */
    private static final String SPARK_SQL_CATALOG_PREFIX = "spark.sql.catalog.";

    /** Connector class name for filtering relevant Unity Catalog catalogs. */
    private static final String UNITY_CATALOG_CONNECTOR_CLASS = "io.unitycatalog.spark.UCSingleCatalog";

    /** Suffix for the URI configuration of a catalog. */
    private static final String URI_SUFFIX = "uri";

    /** Suffix for the token configuration of a catalog. */
    private static final String TOKEN_SUFFIX = "token";

    private final UCCatalogManagedClient ucCatalogManagedClient;
    private final String ucTableId;

    public static UnityCatalogTableManager create(CatalogTable catalogTable) {
        SparkSession spark = SparkSession.active();
        String ucTableId = extractUcTableId(catalogTable);
        Option<Tuple2<CatalogPlugin, Identifier>> pluginAndIdentifier = 
            CatalogAndIdentifier.unapply(catalogTable.identifier().nameParts());
        
        String catalogName = extractCatalogName(pluginAndIdentifier, catalogTable);
        validateUnityCatalog(spark, catalogName);
        UriAndToken uriAndToken = getAndValidateUriAndToken(spark, catalogName);
        
        Engine engine = createEngine(catalogTable);
        UCClient ucClient = createUCClient(uriAndToken.uri, uriAndToken.token);
        UCCatalogManagedClient ucCatalogManagedClient = new UCCatalogManagedClient(ucClient);
        
        return new UnityCatalogTableManager(ucCatalogManagedClient,
                catalogTable.location().toString(), ucTableId, engine);
    }

    /**
     * Extracts the UC table ID from catalog table properties.
     */
    private static String extractUcTableId(CatalogTable catalogTable) {
        return catalogTable.storage().properties().get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY).get();
    }

    /**
     * Extracts the catalog name from the pluginAndIdentifier result.
     */
    private static String extractCatalogName(Option<Tuple2<CatalogPlugin, Identifier>> pluginAndIdentifier, 
                                           CatalogTable catalogTable) {
        if (pluginAndIdentifier.isDefined()) {
            CatalogPlugin catalogPlugin = pluginAndIdentifier.get()._1();
            return catalogPlugin.name();
        } else {
            throw new IllegalArgumentException("Unable to determine catalog for table: " + 
                catalogTable.identifier().toString());
        }
    }

    /**
     * Validates that the specified catalog is configured as a Unity Catalog.
     */
    private static void validateUnityCatalog(SparkSession spark, String catalogName) {
        String connectorConfigKey = SPARK_SQL_CATALOG_PREFIX + catalogName;
        String connectorClass = spark.conf().getOption(connectorConfigKey).getOrElse(() -> null);
        
        if (!UNITY_CATALOG_CONNECTOR_CLASS.equals(connectorClass)) {
            throw new IllegalArgumentException("Catalog " + catalogName + 
                " is not configured as a Unity Catalog. Expected connector class: " +
                UNITY_CATALOG_CONNECTOR_CLASS + ", but found: " + connectorClass);
        }
    }

    /**
     * Gets and validates URI and token configuration for the Unity Catalog.
     */
    private static UriAndToken getAndValidateUriAndToken(SparkSession spark, String catalogName) {
        String uriConfigKey = SPARK_SQL_CATALOG_PREFIX + catalogName + "." + URI_SUFFIX;
        String tokenConfigKey = SPARK_SQL_CATALOG_PREFIX + catalogName + "." + TOKEN_SUFFIX;
        
        String uri = spark.conf().getOption(uriConfigKey).getOrElse(() -> null);
        String token = spark.conf().getOption(tokenConfigKey).getOrElse(() -> null);
        
        // Validate URI is configured
        if (uri == null || uri.trim().isEmpty()) {
            throw new IllegalArgumentException("URI not configured for Unity Catalog " + catalogName + 
                ". Please set " + uriConfigKey);
        }
        
        // Validate token is configured
        if (token == null || token.trim().isEmpty()) {
            throw new IllegalArgumentException("Token not configured for Unity Catalog " + catalogName + 
                ". Please set " + tokenConfigKey);
        }
        
        // Validate URI format
        validateUriFormat(uri, catalogName);
        
        return new UriAndToken(uri, token);
    }

    /**
     * Validates the URI format.
     */
    private static void validateUriFormat(String uri, String catalogName) {
        try {
            new java.net.URI(uri);
        } catch (java.net.URISyntaxException e) {
            throw new IllegalArgumentException("Invalid URI configured for Unity Catalog " + catalogName + 
                ": " + uri, e);
        }
    }

    /**
     * Creates the Delta Kernel engine with Hadoop configuration.
     */
    private static Engine createEngine(CatalogTable catalogTable) {
        Configuration baseHadoopConf = SparkSession.active().sessionState().newHadoopConf();
        java.util.Map<String, String> storageProps =
                JavaConverters.mapAsJavaMapConverter(catalogTable.storage().properties()).asJava();
        for (Map.Entry<String, String> e : storageProps.entrySet()) {
            baseHadoopConf.set(e.getKey(), e.getValue());
        }
        return DefaultEngine.create(baseHadoopConf);
    }

    /**
     * Creates a Unity Catalog client with the provided URI and token.
     */
    private static UCClient createUCClient(String uri, String token) {
        return new UCTokenBasedRestClient(uri, token);
    }

    /**
     * Simple data class to hold URI and token pair.
     */
    private static class UriAndToken {
        final String uri;
        final String token;

        UriAndToken(String uri, String token) {
            this.uri = uri;
            this.token = token;
        }
    }

    private UnityCatalogTableManager(UCCatalogManagedClient ucCatalogManagedClient, String tablePath, String ucTableId, Engine kernelEngine) {
        super(tablePath, kernelEngine);
        this.ucCatalogManagedClient = ucCatalogManagedClient;
        this.ucTableId = ucTableId;
    }

    @Override
    public Snapshot update() {
        Snapshot snapshot = ucCatalogManagedClient.loadSnapshot(kernelEngine, ucTableId, tablePath, Optional.empty());
        return cacheAndReturn(snapshot);
    }

    @Override
    public DeltaHistoryManager.Commit getActiveCommitAtTime(Timestamp timeStamp, Boolean canReturnLastCommit, Boolean mustBeRecreatable, Boolean canReturnEarliestCommit) {
        SnapshotImpl snapshot = (SnapshotImpl)update();
        // TODO: better way to get parsed log delta data and not having two rpc calls
        GetCommitsResponse response = ucCatalogManagedClient.getRatifiedCommitsFromUC(
                ucTableId, tablePath, Optional.of(snapshot.getVersion()));
        List<ParsedLogData> logData =
                UCCatalogManagedClient.getSortedKernelParsedDeltaDataFromRatifiedCommits(ucTableId, response.getCommits());
        List<ParsedDeltaData> parsedDeltaData =
                logData .stream()
                        .filter(x -> x instanceof ParsedDeltaData && x.isFile())
                        .map(ParsedDeltaData.class::cast)
                        .collect(Collectors.toList());
        return DeltaHistoryManager.getActiveCommitAtTimestamp(
                kernelEngine,
                snapshot,
                snapshot.getLogPath(),
                timeStamp.getTime(),
                mustBeRecreatable,
                canReturnLastCommit,
                canReturnEarliestCommit,
                parsedDeltaData);
    }

    @Override
    public void checkVersionExists(Long version, Boolean mustBeRecreatable, Boolean allowOutOfRange) throws AnalysisException {
        SnapshotImpl snapshot = (SnapshotImpl) update();
        // TODO: better way to get parsed log delta data and not having two rpc calls
        GetCommitsResponse response = ucCatalogManagedClient.getRatifiedCommitsFromUC(
                ucTableId, tablePath, Optional.of(snapshot.getVersion()));
        List<ParsedLogData> logData =
                UCCatalogManagedClient.getSortedKernelParsedDeltaDataFromRatifiedCommits(ucTableId, response.getCommits());
        List<ParsedDeltaData> parsedDeltaDatas=
                logData .stream()
                        .filter(x -> x instanceof ParsedDeltaData && x.isFile())
                        .map(ParsedDeltaData.class::cast)
                        .collect(Collectors.toList());
        Optional<Long> earliestRatifiedCommitVersion =
                parsedDeltaDatas.stream().map(ParsedLogData::getVersion).min(Long::compare);
        long earliest = mustBeRecreatable ? DeltaHistoryManager.getEarliestRecreatableCommit(
                kernelEngine, snapshot.getLogPath(), earliestRatifiedCommitVersion
        ) : DeltaHistoryManager.getEarliestDeltaFile(kernelEngine, snapshot.getLogPath(), earliestRatifiedCommitVersion);

        long latest = snapshot.getVersion();
        if (version < earliest || ((version > latest) && !allowOutOfRange)) {
            throw new VersionNotFoundException(version, earliest, latest);
        }
    }
}
