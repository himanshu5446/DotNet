namespace ReportBuilder.Response;

/// <summary>
/// Response model for basic query operations with streaming data and performance metrics.
/// Provides execution status, memory usage tracking, and asynchronous data enumeration.
/// </summary>
public class QueryResponse
{
    /// <summary>
    /// Gets or sets whether the query execution was successful.
    /// False indicates an error occurred during query processing.
    /// </summary>
    public bool Success { get; set; }
    
    /// <summary>
    /// Gets or sets the error message if query execution failed.
    /// Null when Success is true, contains details when Success is false.
    /// </summary>
    public string? ErrorMessage { get; set; }
    
    /// <summary>
    /// Gets or sets the initial memory usage in bytes before query execution.
    /// Used for tracking memory consumption during query processing.
    /// </summary>
    public long InitialMemoryUsage { get; set; }
    
    /// <summary>
    /// Gets or sets the final memory usage in bytes after query completion.
    /// Combined with InitialMemoryUsage to calculate memory delta.
    /// </summary>
    public long FinalMemoryUsage { get; set; }
    
    /// <summary>
    /// Gets or sets the total number of rows returned by the query.
    /// May be estimated for streaming scenarios where counting is expensive.
    /// </summary>
    public int TotalRows { get; set; }
    
    /// <summary>
    /// Gets or sets the asynchronous enumerable containing query result rows.
    /// Each row is represented as a dictionary of column names to values.
    /// </summary>
    public IAsyncEnumerable<Dictionary<string, object?>>? Data { get; set; }
}

/// <summary>
/// Error response model for failed query operations with diagnostic information.
/// Provides structured error details and system state for troubleshooting.
/// </summary>
public class QueryErrorResponse
{
    /// <summary>
    /// Gets or sets the success status, always false for error responses.
    /// Consistent with successful response models for client compatibility.
    /// </summary>
    public bool Success { get; set; } = false;
    
    /// <summary>
    /// Gets or sets the descriptive error message explaining the failure.
    /// Contains specific details about what went wrong during query execution.
    /// </summary>
    public string ErrorMessage { get; set; } = string.Empty;
    
    /// <summary>
    /// Gets or sets the memory usage in bytes at the time of error.
    /// Helps diagnose memory-related issues and system state during failure.
    /// </summary>
    public long MemoryUsage { get; set; }
}