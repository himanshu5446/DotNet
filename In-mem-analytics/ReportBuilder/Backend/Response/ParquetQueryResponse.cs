namespace ReportBuilder.Response;

public class ParquetQueryResponse
{
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
    public long InitialMemoryUsage { get; set; }
    public long FinalMemoryUsage { get; set; }
    public long MaxMemoryUsage { get; set; }
    public long PeakMemoryUsage { get; set; }
    public int TotalRows { get; set; }
    public List<string> Columns { get; set; } = new();
    public string? QueryExecuted { get; set; }
    public long FileSize { get; set; }
    public string? FilePath { get; set; }
    public QueryOptimizations? Optimizations { get; set; }
    public IAsyncEnumerable<Dictionary<string, object?>>? Data { get; set; }
    public List<string> Warnings { get; set; } = new();
}

public class ParquetQueryErrorResponse
{
    public bool Success { get; set; } = false;
    public string ErrorMessage { get; set; } = string.Empty;
    public long MemoryUsage { get; set; }
    public long FileSize { get; set; }
    public List<string> InvalidColumns { get; set; } = new();
    public List<string> AvailableColumns { get; set; } = new();
    public string? FilterError { get; set; }
    public List<string> Suggestions { get; set; } = new();
}

public class ParquetSchemaResponse
{
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
    public long FileSize { get; set; }
    public string? FilePath { get; set; }
    public List<ParquetColumn> Columns { get; set; } = new();
    public long EstimatedRows { get; set; }
    public List<Dictionary<string, object?>> SampleData { get; set; } = new();
    public ParquetFileStats? FileStats { get; set; }
}

public class ParquetColumn
{
    public string Name { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public bool IsNullable { get; set; }
    public string? CompressionType { get; set; }
}

public class ParquetFileStats
{
    public long TotalRows { get; set; }
    public long CompressedSize { get; set; }
    public long UncompressedSize { get; set; }
    public int RowGroups { get; set; }
    public string? Compression { get; set; }
}

public class QueryOptimizations
{
    public bool ProjectionPushdown { get; set; }
    public bool FilterPushdown { get; set; }
    public bool ColumnPruning { get; set; }
    public int ProjectedColumns { get; set; }
    public int TotalColumns { get; set; }
    public string? OptimizationSummary { get; set; }
}