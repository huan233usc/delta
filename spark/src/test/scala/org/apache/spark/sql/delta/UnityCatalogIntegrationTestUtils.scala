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

import io.unitycatalog.server.UnityCatalogServer
import io.unitycatalog.server.persist.utils.HibernateConfigurator
import io.unitycatalog.server.utils.ServerProperties
import java.util.Properties
import org.apache.spark.sql.SparkSession

/**
 * Utilities for testing Delta Lake integration with Unity Catalog from master branch.
 * 
 * This trait provides helper methods to:
 * 1. Start and stop an embedded Unity Catalog server for testing
 * 2. Configure Spark to use Unity Catalog connector
 * 3. Set up test catalogs and schemas
 */
trait UnityCatalogIntegrationTestUtils {
  
  // Unity Catalog server instance and configuration
  private var ucServer: UnityCatalogServer = _
  private var ucServerPort: Int = _
  private var hibernateConfigurator: HibernateConfigurator = _
  
  protected val UC_CATALOG = "unity"
  protected val UC_SCHEMA = "default"
  
  /**
   * Starts an embedded Unity Catalog server on a random port for testing.
   * This should be called in beforeAll() or setUp().
   */
  def startUCServer(): Unit = {
    // Get a random available port
    ucServerPort = findRandomPort()
    
    // Set up server properties
    val serverProperties = new Properties()
    serverProperties.setProperty("server.env", "test")
    val initServerProperties = new ServerProperties(serverProperties)
    
    // Create Hibernate configurator
    hibernateConfigurator = new HibernateConfigurator(initServerProperties)
    
    // Build and start the server
    ucServer = UnityCatalogServer.builder()
      .port(ucServerPort)
      .serverProperties(initServerProperties)
      .build()
    ucServer.start()
    
    println(s"Unity Catalog server started on port $ucServerPort")
  }
  
  /**
   * Stops the Unity Catalog server and cleans up resources.
   * This should be called in afterAll() or tearDown().
   */
  def stopUCServer(): Unit = {
    if (ucServer != null) {
      // Clear the database
      cleanupDatabase()
      
      // Stop the server
      ucServer.stop()
      println("Unity Catalog server stopped")
    }
  }
  
  /**
   * Gets the Unity Catalog server URL.
   */
  def getUCServerUrl(): String = s"http://localhost:$ucServerPort"
  
  /**
   * Creates a SparkSession configured to use Unity Catalog.
   * The session will use the Unity Catalog connector and point to the embedded UC server.
   */
  def createUCSparkSession(catalogName: String = UC_CATALOG): SparkSession = {
    // Create a temporary warehouse directory
    val warehouseDir = java.nio.file.Files.createTempDirectory("uc-warehouse").toString
    
    SparkSession.builder()
      .master("local[*]")
      .appName("Unity Catalog Integration Test")
      // Configure Unity Catalog
      .config(s"spark.sql.catalog.$catalogName", "io.unitycatalog.spark.UCSingleCatalog")
      .config(s"spark.sql.catalog.$catalogName.uri", getUCServerUrl())
      .config(s"spark.sql.catalog.$catalogName.token", "")
      .config(s"spark.sql.catalog.$catalogName.warehouse", warehouseDir)
      // Configure Delta Lake
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
  }
  
  /**
   * Creates a test catalog in Unity Catalog using the UC client API.
   */
  def createUCCatalog(catalogName: String): Unit = {
    import io.unitycatalog.client.ApiClient
    import io.unitycatalog.client.api.CatalogsApi
    import io.unitycatalog.client.model.CreateCatalog
    import java.net.URI
    
    val uri = new URI(getUCServerUrl())
    val apiClient = new ApiClient()
      .setHost(uri.getHost)
      .setPort(uri.getPort)
      .setScheme(uri.getScheme)
    val catalogsApi = new CatalogsApi(apiClient)
    
    try {
      val catalog = catalogsApi.createCatalog(new CreateCatalog().name(catalogName).comment("Test catalog"))
      println(s"Created catalog: ${catalog.getName}")
    } catch {
      case e: Exception => 
        println(s"Catalog $catalogName might already exist or failed to create: ${e.getMessage}")
    }
  }
  
  /**
   * Creates a test schema in Unity Catalog using the UC client API.
   */
  def createUCSchema(catalogName: String, schemaName: String): Unit = {
    import io.unitycatalog.client.ApiClient
    import io.unitycatalog.client.api.SchemasApi
    import io.unitycatalog.client.model.CreateSchema
    import java.net.URI
    
    val uri = new URI(getUCServerUrl())
    val apiClient = new ApiClient()
      .setHost(uri.getHost)
      .setPort(uri.getPort)
      .setScheme(uri.getScheme)
    val schemasApi = new SchemasApi(apiClient)
    
    try {
      val schema = schemasApi.createSchema(
        new CreateSchema().name(schemaName).catalogName(catalogName))
      println(s"Created schema: ${schema.getFullName}")
    } catch {
      case e: Exception => 
        println(s"Schema $catalogName.$schemaName might already exist or failed to create: ${e.getMessage}")
    }
  }
  
  /**
   * Finds a random available port for the UC server.
   */
  private def findRandomPort(): Int = {
    val socket = new java.net.ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }
  
  /**
   * Cleans up the Unity Catalog database.
   */
  private def cleanupDatabase(): Unit = {
    try {
      // Use reflection to avoid compile-time dependency on Hibernate classes
      val hibernateClass = hibernateConfigurator.getClass
      val getSessionFactoryMethod = hibernateClass.getMethod("getSessionFactory")
      val sessionFactory = getSessionFactoryMethod.invoke(hibernateConfigurator)
      val sessionFactoryClass = sessionFactory.getClass
      
      val openSessionMethod = sessionFactoryClass.getMethod("openSession")
      val session = openSessionMethod.invoke(sessionFactory)
      val sessionClass = session.getClass
      
      val beginTransactionMethod = sessionClass.getMethod("beginTransaction")
      val tx = beginTransactionMethod.invoke(session)
      
      // Delete from all tables
      val createMutationQueryMethod = sessionClass.getMethod("createMutationQuery", classOf[String])
      val tables = Seq(
        "FunctionParameterInfoDAO",
        "FunctionInfoDAO",
        "VolumeInfoDAO",
        "ColumnInfoDAO",
        "TableInfoDAO",
        "SchemaInfoDAO",
        "CatalogInfoDAO",
        "UserDAO"
      )
      
      tables.foreach { table =>
        val query = createMutationQueryMethod.invoke(session, s"delete from $table")
        val executeUpdateMethod = query.getClass.getMethod("executeUpdate")
        executeUpdateMethod.invoke(query)
      }
      
      val commitMethod = tx.getClass.getMethod("commit")
      commitMethod.invoke(tx)
      
      val closeMethod = sessionClass.getMethod("close")
      closeMethod.invoke(session)
    } catch {
      case e: Exception =>
        println(s"Error cleaning up database: ${e.getMessage}")
    }
  }
}
