namespace ReportBuilder.Response;

public class CsvQueryResponse
{
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
    public long InitialMemoryUsage { get; set; }
    public long FinalMemoryUsage { get; set; }
    public long MaxMemoryUsage { get; set; }
    public int TotalRows { get; set; }
    public List<string> Columns { get; set; } = new();
    public string? QueryExecuted { get; set; }
    public IAsyncEnumerable<Dictionary<string, object?>>? Data { get; set; }
}

public class CsvQueryErrorResponse
{
    public bool Success { get; set; } = false;
    public string ErrorMessage { get; set; } = string.Empty;
    public long MemoryUsage { get; set; }
    public List<string> MissingColumns { get; set; } = new();
    public List<string> AvailableColumns { get; set; } = new();
}