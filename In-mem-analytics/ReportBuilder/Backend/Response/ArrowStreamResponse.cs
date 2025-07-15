namespace ReportBuilder.Response;

public class ArrowStreamMetadata
{
    public string OriginalQuery { get; set; } = string.Empty;
    public string ExecutedQuery { get; set; } = string.Empty;
    public string FilePath { get; set; } = string.Empty;
    public string FileFormat { get; set; } = string.Empty;
    public long FileSize { get; set; }
    public List<ArrowColumnInfo> Columns { get; set; } = new();
    public DateTime QueryStartTime { get; set; }
    public DateTime? QueryEndTime { get; set; }
    public TimeSpan? Duration => QueryEndTime.HasValue ? QueryEndTime.Value - QueryStartTime : null;
    public long EstimatedRows { get; set; }
}

public class ArrowColumnInfo
{
    public string Name { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public bool IsNullable { get; set; }
    public int Index { get; set; }
}

public class ArrowStreamPerformance
{
    public long InitialMemoryUsage { get; set; }
    public long FinalMemoryUsage { get; set; }
    public long PeakMemoryUsage { get; set; }
    public long MemoryDelta { get; set; }
    public TimeSpan QueryExecutionTime { get; set; }
    public TimeSpan DataLoadTime { get; set; }
    public TimeSpan StreamingTime { get; set; }
    public long BytesStreamed { get; set; }
    public double StreamingRateMBps { get; set; }
    public bool MemoryOptimized { get; set; }
    public int GarbageCollections { get; set; }
}

public class ArrowStreamErrorResponse
{
    public bool Success { get; set; } = false;
    public string ErrorMessage { get; set; } = string.Empty;
    public string? ErrorType { get; set; }
    public long MemoryUsage { get; set; }
    public List<string> ValidationErrors { get; set; } = new();
    public List<string> Suggestions { get; set; } = new();
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    public ArrowStreamMetadata? QueryMetadata { get; set; }
}