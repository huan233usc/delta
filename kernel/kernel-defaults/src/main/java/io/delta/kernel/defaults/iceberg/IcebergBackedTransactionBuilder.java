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
package io.delta.kernel.defaults.iceberg;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Operation;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.types.StructType;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.iceberg.*;

public class IcebergBackedTransactionBuilder implements TransactionBuilder {

  private final org.apache.iceberg.Table icebergTable;
  private final String engineInfo;
  private final Operation operation;

  // Builder state - just collecting configuration, not applying changes
  private Optional<StructType> schema = Optional.empty();
  private Optional<List<String>> partitionColumns = Optional.empty();
  private Optional<List<Column>> clusteringColumns = Optional.empty();
  private Optional<String> applicationId = Optional.empty();
  private Optional<Long> transactionVersion = Optional.empty();
  private Optional<Map<String, String>> tableProperties = Optional.empty();
  private Optional<Set<String>> propertiesToRemove = Optional.empty();

  // Configuration
  private int maxRetries = 200;

  public IcebergBackedTransactionBuilder(
      org.apache.iceberg.Table icebergTable, String engineInfo, Operation operation) {
    this.icebergTable = requireNonNull(icebergTable, "icebergTable cannot be null");
    this.engineInfo = requireNonNull(engineInfo, "engineInfo cannot be null");
    this.operation = requireNonNull(operation, "operation cannot be null");
  }

  @Override
  public TransactionBuilder withSchema(Engine engine, StructType schema) {
    this.schema = Optional.of(requireNonNull(schema, "schema cannot be null"));
    return this;
  }

  @Override
  public TransactionBuilder withPartitionColumns(Engine engine, List<String> partitionColumns) {
    if (partitionColumns != null && !partitionColumns.isEmpty()) {
      this.partitionColumns = Optional.of(new ArrayList<>(partitionColumns));
    }
    return this;
  }

  @Override
  public TransactionBuilder withClusteringColumns(Engine engine, List<Column> clusteringColumns) {
    if (clusteringColumns != null) {
      this.clusteringColumns = Optional.of(new ArrayList<>(clusteringColumns));
    }
    return this;
  }

  @Override
  public TransactionBuilder withTransactionId(
      Engine engine, String applicationId, long transactionVersion) {
    this.applicationId = Optional.of(requireNonNull(applicationId, "applicationId cannot be null"));
    this.transactionVersion = Optional.of(transactionVersion);
    return this;
  }

  @Override
  public TransactionBuilder withTableProperties(Engine engine, Map<String, String> properties) {
    if (properties != null && !properties.isEmpty()) {
      this.tableProperties = Optional.of(new HashMap<>(properties));
    }
    return this;
  }

  @Override
  public TransactionBuilder withTablePropertiesRemoved(Set<String> propertyKeys) {
    if (propertyKeys != null && !propertyKeys.isEmpty()) {
      this.propertiesToRemove = Optional.of(new HashSet<>(propertyKeys));
    }
    return this;
  }

  @Override
  public TransactionBuilder withMaxRetries(int maxRetries) {
    if (maxRetries < 0) {
      throw new IllegalArgumentException("maxRetries must be >= 0");
    }
    this.maxRetries = maxRetries;
    return this;
  }

  @Override
  public TransactionBuilder withLogCompactionInverval(int logCompactionInterval) {
    return this;
  }

  @Override
  public TransactionBuilder withDomainMetadataSupported() {
    return this;
  }

  @Override
  public Transaction build(Engine engine) {
    validateInputs();

    // Get current table metadata
    TableMetadata tableMetadata = getCurrentTableMetadata();

    // Create configuration object to pass to transaction
    IcebergBackedTransaction.TransactionConfiguration config =
        new IcebergBackedTransaction.TransactionConfiguration(
            schema, partitionColumns, clusteringColumns, tableProperties, propertiesToRemove);

    // Create the transaction with configuration - no changes applied here
    return new IcebergBackedTransaction(
        icebergTable, engineInfo, operation, tableMetadata, maxRetries, config);
  }

  private void validateInputs() {
    // Validate that properties to set and remove don't overlap
    if (tableProperties.isPresent() && propertiesToRemove.isPresent()) {
      Set<String> overlap =
          tableProperties.get().keySet().stream()
              .filter(propertiesToRemove.get()::contains)
              .collect(Collectors.toSet());
      if (!overlap.isEmpty()) {
        throw new IllegalArgumentException("Cannot set and remove the same properties: " + overlap);
      }
    }

    // Validate transaction ID components
    if (applicationId.isPresent() != transactionVersion.isPresent()) {
      throw new IllegalArgumentException(
          "Both applicationId and transactionVersion must be set together");
    }

    // Validate schema is provided for CREATE_TABLE
    if (operation == Operation.CREATE_TABLE && !schema.isPresent()) {
      throw new KernelException("Schema is required for creating a new table");
    }
  }

  private TableMetadata getCurrentTableMetadata() {
    if (icebergTable instanceof BaseTable) {
      return ((BaseTable) icebergTable).operations().current();
    }
    throw new KernelException("Unable to access table metadata for non-BaseTable instances");
  }
}
