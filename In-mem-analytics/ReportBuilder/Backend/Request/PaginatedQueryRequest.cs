using System.ComponentModel.DataAnnotations;

namespace ReportBuilder.Request;

public class PaginatedQueryRequest
{
    [Required]
    public string SqlQuery { get; set; } = string.Empty;
    
    [Required]
    public string FilePath { get; set; } = string.Empty;
    
    [Required]
    public PaginatedFileFormat FileFormat { get; set; } = PaginatedFileFormat.Auto;
    
    [Range(1, int.MaxValue, ErrorMessage = "Page number must be greater than 0")]
    public int PageNumber { get; set; } = 1;
    
    [Range(1, 10000, ErrorMessage = "Page size must be between 1 and 10000")]
    public int PageSize { get; set; } = 1000;
    
    public bool IncludeTotalCount { get; set; } = false;
    
    public bool EnableMemoryLogging { get; set; } = true;
    
    public int TimeoutSeconds { get; set; } = 120;
    
    public List<string> ProjectionColumns { get; set; } = new();
    
    public string? WhereClause { get; set; }
    
    public string? OrderBy { get; set; }
}

public enum PaginatedFileFormat
{
    Auto,
    Csv,
    Parquet,
    Arrow
}