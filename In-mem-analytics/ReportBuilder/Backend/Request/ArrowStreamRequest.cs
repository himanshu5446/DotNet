using System.ComponentModel.DataAnnotations;

namespace ReportBuilder.Request;

public class ArrowStreamRequest
{
    [Required]
    public string SqlQuery { get; set; } = string.Empty;

    [Required]
    public string FilePath { get; set; } = string.Empty;

    public ArrowFileFormat FileFormat { get; set; } = ArrowFileFormat.Auto;

    public int TimeoutSeconds { get; set; } = 300;

    public bool IncludeMetadata { get; set; } = true;

    public bool EnableMemoryLogging { get; set; } = false;

    public List<string> ProjectionColumns { get; set; } = new();

    public string? WhereClause { get; set; }

    public string? OrderBy { get; set; }

    public int? LimitRows { get; set; }
}

public enum ArrowFileFormat
{
    Auto,
    Csv,
    Parquet,
    Arrow
}