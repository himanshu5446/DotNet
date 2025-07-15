using ReportBuilder.Request;

namespace ReportBuilder.Response;

public class UdfQueryResponse
{
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
    public UdfQueryMetadata Query { get; set; } = new();
    public UdfPerformanceMetrics Performance { get; set; } = new();
    public List<Dictionary<string, object?>>? Data { get; set; }
    public IAsyncEnumerable<Dictionary<string, object?>>? StreamData { get; set; }
    public List<string> Warnings { get; set; } = new();
}

public class UdfQueryMetadata
{
    public string OriginalQuery { get; set; } = string.Empty;
    public string CsvFilePath { get; set; } = string.Empty;
    public long FileSizeBytes { get; set; }
    public long TotalRows { get; set; }
    public int ColumnCount { get; set; }
    public List<string> Columns { get; set; } = new();
    public List<string> RegisteredUdfs { get; set; } = new();
    public DateTime QueryStartTime { get; set; }
    public DateTime? QueryEndTime { get; set; }
    public TimeSpan? Duration => QueryEndTime.HasValue ? QueryEndTime.Value - QueryStartTime : null;
}

public class UdfPerformanceMetrics
{
    public long InitialMemoryUsage { get; set; }
    public long PeakMemoryUsage { get; set; }
    public long FinalMemoryUsage { get; set; }
    public long MemoryDelta { get; set; }
    public bool MemoryLimitExceeded { get; set; }
    public TimeSpan CsvLoadTime { get; set; }
    public TimeSpan QueryExecutionTime { get; set; }
    public TimeSpan TotalTime { get; set; }
    public long ProcessedRows { get; set; }
    public double RowsPerSecond { get; set; }
    public int GarbageCollections { get; set; }
    public List<UdfBatchMetrics> BatchMetrics { get; set; } = new();
}

public class UdfBatchMetrics
{
    public long BatchStartRow { get; set; }
    public long BatchEndRow { get; set; }
    public int BatchSize { get; set; }
    public TimeSpan ProcessingTime { get; set; }
    public double RowsPerSecond { get; set; }
    public long MemoryUsage { get; set; }
    public long MemoryDelta { get; set; }
    public DateTime Timestamp { get; set; }
}

public class UdfTextNormalizationResponse
{
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
    public UdfNormalizationMetadata Metadata { get; set; } = new();
    public UdfPerformanceMetrics Performance { get; set; } = new();
    public IAsyncEnumerable<Dictionary<string, object?>>? StreamData { get; set; }
    public List<string> Warnings { get; set; } = new();
}

public class UdfNormalizationMetadata
{
    public string TextColumnName { get; set; } = string.Empty;
    public UdfNormalizationType NormalizationType { get; set; }
    public string GeneratedQuery { get; set; } = string.Empty;
    public long InputRowCount { get; set; }
    public long OutputRowCount { get; set; }
    public int NullValues { get; set; }
    public int EmptyValues { get; set; }
    public int ProcessedValues { get; set; }
    public Dictionary<string, int> CharacterCounts { get; set; } = new();
}

public class UdfErrorResponse
{
    public bool Success { get; set; } = false;
    public string ErrorMessage { get; set; } = string.Empty;
    public string? ErrorType { get; set; }
    public long MemoryUsage { get; set; }
    public List<string> ValidationErrors { get; set; } = new();
    public List<string> Suggestions { get; set; } = new();
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    public UdfQueryMetadata? QueryMetadata { get; set; }
}