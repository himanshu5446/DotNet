using System.ComponentModel.DataAnnotations;

namespace ReportBuilder.Request;

public class UdfQueryRequest
{
    [Required]
    public string CsvFilePath { get; set; } = string.Empty;

    [Required]
    public string SqlQuery { get; set; } = string.Empty;

    public int BatchSize { get; set; } = 10000;

    public int TimeoutSeconds { get; set; } = 600;

    public bool EnableMemoryLogging { get; set; } = true;

    public bool EnablePerformanceLogging { get; set; } = true;

    public UdfResponseFormat ResponseFormat { get; set; } = UdfResponseFormat.Streaming;

    public int MaxRows { get; set; } = 0; // 0 = no limit

    public bool ForceGarbageCollection { get; set; } = true;
}

public enum UdfResponseFormat
{
    Streaming,
    Buffered,
    Count
}

public class UdfTextNormalizationRequest
{
    [Required]
    public string CsvFilePath { get; set; } = string.Empty;

    [Required]
    public string TextColumnName { get; set; } = string.Empty;

    public List<string> AdditionalColumns { get; set; } = new();

    public string? WhereClause { get; set; }

    public string? OrderBy { get; set; }

    public int BatchSize { get; set; } = 10000;

    public int TimeoutSeconds { get; set; } = 600;

    public bool EnableMemoryLogging { get; set; } = true;

    public UdfNormalizationType NormalizationType { get; set; } = UdfNormalizationType.Full;
}

public enum UdfNormalizationType
{
    Full,           // Complete normalization (lowercase, trim, remove special chars)
    LowerOnly,      // Just lowercase
    TrimOnly,       // Just trim whitespace
    RemoveSpecial,  // Remove special characters only
    Phone,          // Phone number normalization
    Email           // Email normalization
}