namespace ReportBuilder.Response;

public class JoinQueryResponse
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
    public long LeftFileSize { get; set; }
    public long RightFileSize { get; set; }
    public IAsyncEnumerable<Dictionary<string, object?>>? Data { get; set; }
    public DatabricksSuggestion? DatabricksSuggestion { get; set; }
}

public class JoinQueryErrorResponse
{
    public bool Success { get; set; } = false;
    public string ErrorMessage { get; set; } = string.Empty;
    public long MemoryUsage { get; set; }
    public long LeftFileSize { get; set; }
    public long RightFileSize { get; set; }
    public List<string> MissingColumns { get; set; } = new();
    public List<string> LeftColumns { get; set; } = new();
    public List<string> RightColumns { get; set; } = new();
    public DatabricksSuggestion? DatabricksSuggestion { get; set; }
}

public class DatabricksSuggestion
{
    public bool ShouldUseDatabricks { get; set; }
    public string Reason { get; set; } = string.Empty;
    public long EstimatedMemoryRequired { get; set; }
    public string SuggestedSqlCode { get; set; } = string.Empty;
    public List<string> OptimizationTips { get; set; } = new();
}