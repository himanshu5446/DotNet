namespace ReportBuilder.Response;

public class PaginatedResponse<T>
{
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
    public IAsyncEnumerable<T>? Data { get; set; }
    public PaginationMetadata Pagination { get; set; } = new();
    public QueryMetadata Query { get; set; } = new();
    public PerformanceMetadata Performance { get; set; } = new();
    public List<string> Warnings { get; set; } = new();
}

public class PaginationMetadata
{
    public int CurrentPage { get; set; }
    public int PageSize { get; set; }
    public long? TotalRows { get; set; }
    public int? TotalPages { get; set; }
    public bool HasNextPage { get; set; }
    public bool HasPreviousPage { get; set; }
    public int StartRow { get; set; }
    public int EndRow { get; set; }
    public int RowsInCurrentPage { get; set; }
}

public class QueryMetadata
{
    public string OriginalQuery { get; set; } = string.Empty;
    public string ExecutedQuery { get; set; } = string.Empty;
    public string FilePath { get; set; } = string.Empty;
    public string FileFormat { get; set; } = string.Empty;
    public long FileSize { get; set; }
    public List<string> Columns { get; set; } = new();
    public DateTime QueryStartTime { get; set; }
    public DateTime? QueryEndTime { get; set; }
    public TimeSpan? Duration => QueryEndTime.HasValue ? QueryEndTime.Value - QueryStartTime : null;
}

public class PerformanceMetadata
{
    public long InitialMemoryUsage { get; set; }
    public long FinalMemoryUsage { get; set; }
    public long PeakMemoryUsage { get; set; }
    public long MemoryDelta { get; set; }
    public TimeSpan QueryExecutionTime { get; set; }
    public TimeSpan? CountQueryTime { get; set; }
    public TimeSpan DataLoadTime { get; set; }
    public bool MemoryOptimized { get; set; }
    public int GarbageCollections { get; set; }
}

public class PaginatedQueryErrorResponse
{
    public bool Success { get; set; } = false;
    public string ErrorMessage { get; set; } = string.Empty;
    public string? ErrorType { get; set; }
    public PaginationMetadata? Pagination { get; set; }
    public long MemoryUsage { get; set; }
    public List<string> ValidationErrors { get; set; } = new();
    public List<string> Suggestions { get; set; } = new();
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}