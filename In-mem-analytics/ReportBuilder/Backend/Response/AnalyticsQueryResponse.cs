namespace ReportBuilder.Response;

public class AnalyticsQueryResponse
{
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
    public string QueryId { get; set; } = Guid.NewGuid().ToString();
    public long InitialMemoryUsage { get; set; }
    public long FinalMemoryUsage { get; set; }
    public long PeakMemoryUsage { get; set; }
    public int TotalRows { get; set; }
    public List<string> Columns { get; set; } = new();
    public string? QueryExecuted { get; set; }
    public long FileSize { get; set; }
    public string? FilePath { get; set; }
    public QueryPerformanceMetrics? Performance { get; set; }
    public IAsyncEnumerable<Dictionary<string, object?>>? Data { get; set; }
    public List<string> Warnings { get; set; } = new();
    public DateTime StartTime { get; set; } = DateTime.UtcNow;
    public DateTime? EndTime { get; set; }
    public TimeSpan? Duration => EndTime.HasValue ? EndTime.Value - StartTime : null;
}

public class AnalyticsQueryErrorResponse
{
    public bool Success { get; set; } = false;
    public string ErrorMessage { get; set; } = string.Empty;
    public string QueryId { get; set; } = string.Empty;
    public long MemoryUsage { get; set; }
    public string? ErrorType { get; set; }
    public List<string> ValidationErrors { get; set; } = new();
    public List<string> Suggestions { get; set; } = new();
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    public int? ConcurrentQueries { get; set; }
    public bool ShouldRetry { get; set; } = false;
}

public class ArrowStreamResponse
{
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
    public string QueryId { get; set; } = Guid.NewGuid().ToString();
    public long InitialMemoryUsage { get; set; }
    public long FinalMemoryUsage { get; set; }
    public long PeakMemoryUsage { get; set; }
    public int TotalRows { get; set; }
    public int ChunksProcessed { get; set; }
    public long TotalBytesRead { get; set; }
    public ArrowSchemaInfo? SchemaInfo { get; set; }
    public IAsyncEnumerable<Dictionary<string, object?>>? Data { get; set; }
    public List<string> Warnings { get; set; } = new();
    public DateTime StartTime { get; set; } = DateTime.UtcNow;
    public DateTime? EndTime { get; set; }
    public TimeSpan? Duration => EndTime.HasValue ? EndTime.Value - StartTime : null;
}

public class QueryStatusResponse
{
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
    public List<ActiveQuery> ActiveQueries { get; set; } = new();
    public QuerySystemStats? SystemStats { get; set; }
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}

public class SchemaResponse
{
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
    public List<ColumnInfo> Columns { get; set; } = new();
    public long FileSize { get; set; }
    public string? FilePath { get; set; }
    public long EstimatedRows { get; set; }
    public List<Dictionary<string, object?>> SampleData { get; set; } = new();
    public FileMetadata? Metadata { get; set; }
}

public class QueryPerformanceMetrics
{
    public TimeSpan QueryExecutionTime { get; set; }
    public TimeSpan FileLoadTime { get; set; }
    public TimeSpan StreamingTime { get; set; }
    public long MemoryPeakDelta { get; set; }
    public int GarbageCollections { get; set; }
    public bool OptimizationsApplied { get; set; }
    public List<string> OptimizationDetails { get; set; } = new();
}

public class ArrowSchemaInfo
{
    public int FieldCount { get; set; }
    public List<ArrowFieldInfo> Fields { get; set; } = new();
    public Dictionary<string, string> Metadata { get; set; } = new();
    public bool SchemaMatches { get; set; } = true;
    public string? SchemaMismatchReason { get; set; }
}

public class ArrowFieldInfo
{
    public string Name { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public bool IsNullable { get; set; }
    public Dictionary<string, string> Metadata { get; set; } = new();
}

public class ActiveQuery
{
    public string QueryId { get; set; } = string.Empty;
    public string SqlQuery { get; set; } = string.Empty;
    public DateTime StartTime { get; set; }
    public TimeSpan Duration => DateTime.UtcNow - StartTime;
    public long CurrentMemoryUsage { get; set; }
    public int RowsProcessed { get; set; }
    public string Status { get; set; } = string.Empty;
    public string? FilePath { get; set; }
}

public class QuerySystemStats
{
    public int ActiveConnections { get; set; }
    public int QueuedQueries { get; set; }
    public long TotalMemoryUsage { get; set; }
    public long AvailableMemory { get; set; }
    public double CpuUsage { get; set; }
    public int MaxConcurrentQueries { get; set; }
    public TimeSpan AverageQueryTime { get; set; }
}

public class ColumnInfo
{
    public string Name { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public bool IsNullable { get; set; }
    public object? SampleValue { get; set; }
    public long? DistinctCount { get; set; }
}

public class FileMetadata
{
    public string Format { get; set; } = string.Empty;
    public string? Compression { get; set; }
    public DateTime LastModified { get; set; }
    public string? ETag { get; set; }
    public Dictionary<string, object> Properties { get; set; } = new();
}