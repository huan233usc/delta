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

package io.delta.kernel.spark.catalog.utils;

/**
 * Unified exception class for DeltaTableManager operations.
 * Provides consistent error handling across all catalog implementations.
 */
public class DeltaTableManagerException extends RuntimeException {
    
    public enum ErrorType {
        CATALOG_UNAVAILABLE("Catalog service is unavailable"),
        SNAPSHOT_NOT_FOUND("Requested snapshot not found"),
        VERSION_NOT_FOUND("Requested version not found"),
        PERMISSION_DENIED("Access permission denied"),
        NETWORK_ERROR("Network communication error"),
        INITIALIZATION_ERROR("Initialization failed"),
        INVALID_CONFIGURATION("Invalid configuration provided"),
        TABLE_NOT_FOUND("Table not found"),
        UNSUPPORTED_OPERATION("Operation not supported");
        
        private final String description;
        
        ErrorType(String description) {
            this.description = description;
        }
        
        public String getDescription() {
            return description;
        }
    }
    
    private final ErrorType errorType;
    private final String catalogType;
    
    public DeltaTableManagerException(ErrorType errorType, String catalogType, String message) {
        super(formatMessage(errorType, catalogType, message));
        this.errorType = errorType;
        this.catalogType = catalogType;
    }
    
    public DeltaTableManagerException(ErrorType errorType, String catalogType, String message, Throwable cause) {
        super(formatMessage(errorType, catalogType, message), cause);
        this.errorType = errorType;
        this.catalogType = catalogType;
    }
    
    public DeltaTableManagerException(ErrorType errorType, String catalogType, Throwable cause) {
        super(formatMessage(errorType, catalogType, errorType.getDescription()), cause);
        this.errorType = errorType;
        this.catalogType = catalogType;
    }
    
    private static String formatMessage(ErrorType errorType, String catalogType, String message) {
        return String.format("[%s] %s: %s", 
            catalogType != null ? catalogType : "Unknown", 
            errorType.name(), 
            message);
    }
    
    public ErrorType getErrorType() {
        return errorType;
    }
    
    public String getCatalogType() {
        return catalogType;
    }
    
    /**
     * Check if this exception represents a retryable error.
     */
    public boolean isRetryable() {
        switch (errorType) {
            case CATALOG_UNAVAILABLE:
            case NETWORK_ERROR:
                return true;
            case PERMISSION_DENIED:
            case INVALID_CONFIGURATION:
            case VERSION_NOT_FOUND:
            case TABLE_NOT_FOUND:
            case UNSUPPORTED_OPERATION:
                return false;
            default:
                return false;
        }
    }
    
    // Convenience factory methods
    public static DeltaTableManagerException catalogUnavailable(String catalogType, String message) {
        return new DeltaTableManagerException(ErrorType.CATALOG_UNAVAILABLE, catalogType, message);
    }
    
    public static DeltaTableManagerException catalogUnavailable(String catalogType, Throwable cause) {
        return new DeltaTableManagerException(ErrorType.CATALOG_UNAVAILABLE, catalogType, cause);
    }
    
    public static DeltaTableManagerException snapshotNotFound(String catalogType, String message) {
        return new DeltaTableManagerException(ErrorType.SNAPSHOT_NOT_FOUND, catalogType, message);
    }
    
    public static DeltaTableManagerException versionNotFound(String catalogType, long version) {
        return new DeltaTableManagerException(ErrorType.VERSION_NOT_FOUND, catalogType, 
            "Version " + version + " not found");
    }
    
    public static DeltaTableManagerException permissionDenied(String catalogType, String message) {
        return new DeltaTableManagerException(ErrorType.PERMISSION_DENIED, catalogType, message);
    }
    
    public static DeltaTableManagerException networkError(String catalogType, Throwable cause) {
        return new DeltaTableManagerException(ErrorType.NETWORK_ERROR, catalogType, cause);
    }
    
    public static DeltaTableManagerException initializationError(String catalogType, String message) {
        return new DeltaTableManagerException(ErrorType.INITIALIZATION_ERROR, catalogType, message);
    }
    
    public static DeltaTableManagerException initializationError(String catalogType, Throwable cause) {
        return new DeltaTableManagerException(ErrorType.INITIALIZATION_ERROR, catalogType, cause);
    }
    
    public static DeltaTableManagerException invalidConfiguration(String catalogType, String message) {
        return new DeltaTableManagerException(ErrorType.INVALID_CONFIGURATION, catalogType, message);
    }
    
    public static DeltaTableManagerException tableNotFound(String catalogType, String tablePath) {
        return new DeltaTableManagerException(ErrorType.TABLE_NOT_FOUND, catalogType, 
            "Table not found at path: " + tablePath);
    }
    
    public static DeltaTableManagerException unsupportedOperation(String catalogType, String operation) {
        return new DeltaTableManagerException(ErrorType.UNSUPPORTED_OPERATION, catalogType, 
            "Operation not supported: " + operation);
    }
}
