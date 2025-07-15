using System.ComponentModel.DataAnnotations;

namespace ReportBuilder.Request;

/// <summary>
/// Advanced analytics query request for executing SQL on Azure Data Lake Storage files.
/// Provides comprehensive query capabilities with memory optimization and performance tuning.
/// </summary>
public class AnalyticsQueryRequest
{
    /// <summary>
    /// Gets or sets the SQL query to execute against the ADLS dataset.
    /// Must be valid DuckDB-compatible SQL syntax.
    /// </summary>
    [Required]
    public string SqlQuery { get; set; } = string.Empty;
    
    /// <summary>
    /// Gets or sets the Azure Data Lake Storage URI for the data file.
    /// Should be a valid ADLS URI pointing to a supported file format.
    /// </summary>
    [Required]
    public string AdlsFileUri { get; set; } = string.Empty;
    
    /// <summary>
    /// Gets or sets the optional SAS token for authenticated access to ADLS.
    /// Required if the ADLS resource requires authentication.
    /// </summary>
    public string? SasToken { get; set; }
    
    /// <summary>
    /// Gets or sets the file format for the ADLS data.
    /// Auto-detection is used by default. Default: Auto.
    /// </summary>
    public FileFormat FileFormat { get; set; } = FileFormat.Auto;
    
    /// <summary>
    /// Gets or sets the list of columns to project from the dataset.
    /// Empty list means all columns will be included.
    /// </summary>
    public List<string> ProjectionColumns { get; set; } = new();
    
    /// <summary>
    /// Gets or sets an optional pre-filter to apply before query execution.
    /// Helps optimize query performance by reducing data load.
    /// </summary>
    public string? PreFilter { get; set; }
    
    /// <summary>
    /// Gets or sets the maximum number of rows to return.
    /// Null means no limit will be applied.
    /// </summary>
    public int? Limit { get; set; }
    
    /// <summary>
    /// Gets or sets the page size for streaming results.
    /// Controls memory usage and streaming performance. Default: 1000.
    /// </summary>
    public int PageSize { get; set; } = 1000;
    
    /// <summary>
    /// Gets or sets the query timeout in seconds.
    /// Query will be cancelled if it exceeds this duration. Default: 300 seconds (5 minutes).
    /// </summary>
    public int TimeoutSeconds { get; set; } = 300;
    
    /// <summary>
    /// Gets or sets whether memory optimization is enabled.
    /// Applies various memory-saving techniques during query execution. Default: true.
    /// </summary>
    public bool EnableMemoryOptimization { get; set; } = true;
    
    /// <summary>
    /// Gets or sets the desired response format for query results.
    /// Determines how results are serialized and delivered. Default: Json.
    /// </summary>
    public ResponseFormat ResponseFormat { get; set; } = ResponseFormat.Json;
}

/// <summary>
/// Advanced analytics JOIN request for executing SQL joins across multiple ADLS datasets.
/// Supports complex join operations with memory optimization and performance tuning.
/// </summary>
public class AnalyticsJoinRequest
{
    /// <summary>
    /// Gets or sets the SQL query defining the join operation.
    /// Must reference the datasets using their configured aliases.
    /// </summary>
    [Required]
    public string SqlQuery { get; set; } = string.Empty;
    
    /// <summary>
    /// Gets or sets the left dataset reference for the join operation.
    /// Contains ADLS URI, format, and projection information.
    /// </summary>
    [Required]
    public AdlsFileReference LeftDataset { get; set; } = new();
    
    /// <summary>
    /// Gets or sets the right dataset reference for the join operation.
    /// Contains ADLS URI, format, and projection information.
    /// </summary>
    [Required]
    public AdlsFileReference RightDataset { get; set; } = new();
    
    /// <summary>
    /// Gets or sets an optional pre-filter to apply before join execution.
    /// Can significantly improve join performance by reducing dataset sizes.
    /// </summary>
    public string? PreFilter { get; set; }
    
    /// <summary>
    /// Gets or sets the maximum number of joined rows to return.
    /// Null means no limit will be applied to the join result.
    /// </summary>
    public int? Limit { get; set; }
    
    /// <summary>
    /// Gets or sets the page size for streaming join results.
    /// Controls memory usage during result delivery. Default: 1000.
    /// </summary>
    public int PageSize { get; set; } = 1000;
    
    /// <summary>
    /// Gets or sets the join operation timeout in seconds.
    /// Join will be cancelled if it exceeds this duration. Default: 300 seconds.
    /// </summary>
    public int TimeoutSeconds { get; set; } = 300;
    
    /// <summary>
    /// Gets or sets whether memory optimization is enabled for the join.
    /// Applies join-specific memory optimizations. Default: true.
    /// </summary>
    public bool EnableMemoryOptimization { get; set; } = true;
}

public class AdlsArrowStreamRequest
{
    [Required]
    public string AdlsFileUri { get; set; } = string.Empty;
    
    public string? SasToken { get; set; }
    
    [Required]
    public string SqlQuery { get; set; } = string.Empty;
    
    public int ChunkSize { get; set; } = 65536; // 64KB chunks
    
    public long? StartOffset { get; set; }
    
    public long? EndOffset { get; set; }
    
    public int PageSize { get; set; } = 1000;
    
    public int TimeoutSeconds { get; set; } = 300;
    
    public bool ValidateSchema { get; set; } = true;
    
    public int MaxRetries { get; set; } = 3;
}

public class AdlsFileReference
{
    [Required]
    public string AdlsFileUri { get; set; } = string.Empty;
    
    public string? SasToken { get; set; }
    
    public FileFormat FileFormat { get; set; } = FileFormat.Auto;
    
    public List<string> ProjectionColumns { get; set; } = new();
    
    public string? Alias { get; set; }
}

public class QueryStatusRequest
{
    public string? QueryId { get; set; }
    
    public bool IncludeMetrics { get; set; } = true;
}

public class SchemaRequest
{
    [Required]
    public string AdlsFileUri { get; set; } = string.Empty;
    
    public string? SasToken { get; set; }
    
    public FileFormat FileFormat { get; set; } = FileFormat.Auto;
    
    public int? SampleRows { get; set; } = 5;
}

/// <summary>
/// Enumeration of supported file formats for data processing.
/// Determines how data files are parsed and loaded into the query engine.
/// </summary>
public enum FileFormat
{
    /// <summary>
    /// Automatic format detection based on file extension and content analysis.
    /// </summary>
    Auto,
    
    /// <summary>
    /// Comma-Separated Values format with configurable delimiters and headers.
    /// </summary>
    Csv,
    
    /// <summary>
    /// Apache Parquet columnar storage format for high-performance analytics.
    /// </summary>
    Parquet,
    
    /// <summary>
    /// Apache Arrow in-memory columnar format for zero-copy data exchange.
    /// </summary>
    Arrow
}

/// <summary>
/// Enumeration of supported response formats for query results.
/// Determines how query results are serialized and delivered to clients.
/// </summary>
public enum ResponseFormat
{
    /// <summary>
    /// JavaScript Object Notation format for web-friendly data exchange.
    /// </summary>
    Json,
    
    /// <summary>
    /// Apache Parquet format for efficient columnar data storage and transfer.
    /// </summary>
    Parquet,
    
    /// <summary>
    /// Apache Arrow IPC format for high-performance columnar data streaming.
    /// </summary>
    Arrow,
    
    /// <summary>
    /// Comma-Separated Values format for compatibility with spreadsheet applications.
    /// </summary>
    Csv
}