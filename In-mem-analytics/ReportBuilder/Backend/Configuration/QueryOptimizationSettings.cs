namespace ReportBuilder.Configuration;

/// <summary>
/// Configuration settings for SQL query optimization features and behavior.
/// Controls optimization rules, thresholds, and feature enablement.
/// </summary>
public class QueryOptimizationSettings
{
    /// <summary>
    /// Configuration section name for binding from appsettings.json.
    /// </summary>
    public const string SectionName = "QueryOptimizationSettings";

    /// <summary>
    /// Gets or sets whether query optimization middleware is enabled.
    /// When disabled, queries pass through without optimization analysis. Default: true.
    /// </summary>
    public bool EnableOptimization { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to automatically apply optimizations to queries.
    /// When true, queries are rewritten; when false, only suggestions are provided. Default: false.
    /// </summary>
    public bool AutoApplyOptimizations { get; set; } = false;

    /// <summary>
    /// Gets or sets whether to enable column projection optimization.
    /// Detects and suggests removal of unused columns from SELECT statements. Default: true.
    /// </summary>
    public bool EnableColumnProjection { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to enable filter pushdown optimization.
    /// Detects WHERE conditions that can be applied at file reading level. Default: true.
    /// </summary>
    public bool EnableFilterPushdown { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to add optimization headers to HTTP responses.
    /// Provides optimization suggestions in response headers for client analysis. Default: true.
    /// </summary>
    public bool AddOptimizationHeaders { get; set; } = true;

    /// <summary>
    /// Gets or sets the minimum estimated performance gain to suggest optimizations.
    /// Filters out low-impact suggestions. Range: 0.0 to 1.0. Default: 0.1 (10%).
    /// </summary>
    public double MinimumPerformanceGainThreshold { get; set; } = 0.1;

    /// <summary>
    /// Gets or sets the maximum query length in characters to analyze.
    /// Very large queries may be skipped for performance. Default: 10000.
    /// </summary>
    public int MaxQueryLengthForAnalysis { get; set; } = 10000;

    /// <summary>
    /// Gets or sets the maximum time in milliseconds to spend on query analysis.
    /// Prevents optimization from blocking requests. Default: 5000ms (5 seconds).
    /// </summary>
    public int MaxAnalysisTimeoutMs { get; set; } = 5000;

    /// <summary>
    /// Gets or sets file types that support filter pushdown optimization.
    /// Case-insensitive file extensions. Default: parquet.
    /// </summary>
    public List<string> FilterPushdownSupportedFormats { get; set; } = new() { "parquet" };

    /// <summary>
    /// Gets or sets file types that support column projection optimization.
    /// Case-insensitive file extensions. Default: parquet, csv.
    /// </summary>
    public List<string> ColumnProjectionSupportedFormats { get; set; } = new() { "parquet", "csv" };

    /// <summary>
    /// Gets or sets whether to log optimization analysis results.
    /// Useful for monitoring optimization patterns and effectiveness. Default: true.
    /// </summary>
    public bool EnableOptimizationLogging { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to cache optimization analysis results.
    /// Improves performance for repeated queries. Default: true.
    /// </summary>
    public bool EnableOptimizationCaching { get; set; } = true;

    /// <summary>
    /// Gets or sets the cache duration in minutes for optimization results.
    /// How long to cache analysis results for identical queries. Default: 30 minutes.
    /// </summary>
    public int OptimizationCacheDurationMinutes { get; set; } = 30;

    /// <summary>
    /// Gets or sets SQL keywords that indicate complex queries requiring careful optimization.
    /// Queries containing these keywords may receive additional validation. Default: JOIN, UNION, SUBQUERY.
    /// </summary>
    public List<string> ComplexQueryKeywords { get; set; } = new() { "JOIN", "UNION", "SUBQUERY", "WITH", "WINDOW" };

    /// <summary>
    /// Gets or sets whether to enable experimental optimization features.
    /// Advanced optimizations that may not be fully stable. Default: false.
    /// </summary>
    public bool EnableExperimentalOptimizations { get; set; } = false;

    /// <summary>
    /// Gets or sets the maximum number of optimization suggestions to return.
    /// Limits response size and focuses on top recommendations. Default: 10.
    /// </summary>
    public int MaxOptimizationSuggestions { get; set; } = 10;

    /// <summary>
    /// Gets or sets optimization rules for specific patterns.
    /// Customizable optimization rules and their configurations.
    /// </summary>
    public OptimizationRules Rules { get; set; } = new();
}

/// <summary>
/// Specific optimization rules and their configurations.
/// </summary>
public class OptimizationRules
{
    /// <summary>
    /// Gets or sets whether to suggest LIMIT clauses for queries without them.
    /// Helps prevent accidentally loading massive datasets. Default: true.
    /// </summary>
    public bool SuggestLimitForUnboundedQueries { get; set; } = true;

    /// <summary>
    /// Gets or sets the suggested LIMIT value for unbounded queries.
    /// Default limit to suggest when no LIMIT clause is present. Default: 10000.
    /// </summary>
    public int DefaultSuggestedLimit { get; set; } = 10000;

    /// <summary>
    /// Gets or sets whether to suggest indexes for frequently filtered columns.
    /// Analyzes WHERE clauses to recommend index creation. Default: true.
    /// </summary>
    public bool SuggestIndexesForFrequentFilters { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to warn about SELECT DISTINCT on large datasets.
    /// DISTINCT operations can be expensive on large datasets. Default: true.
    /// </summary>
    public bool WarnAboutDistinctOnLargeDatasets { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to suggest ORDER BY optimizations.
    /// Recommends efficient sorting strategies. Default: true.
    /// </summary>
    public bool SuggestOrderByOptimizations { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to suggest aggregation optimizations.
    /// Recommends efficient GROUP BY and aggregation patterns. Default: true.
    /// </summary>
    public bool SuggestAggregationOptimizations { get; set; } = true;

    /// <summary>
    /// Gets or sets column names that are commonly projected and should be suggested.
    /// When optimizing SELECT *, these columns are prioritized. Default: id, name, created_date, status.
    /// </summary>
    public List<string> CommonlyProjectedColumns { get; set; } = new() 
    { 
        "id", "name", "created_date", "updated_date", "status", "type", "category" 
    };

    /// <summary>
    /// Gets or sets column patterns that indicate temporal data suitable for time-based filtering.
    /// Used to suggest time-based partitioning and filtering. Default: date, time, timestamp patterns.
    /// </summary>
    public List<string> TemporalColumnPatterns { get; set; } = new() 
    { 
        "*date*", "*time*", "*timestamp*", "created_*", "updated_*", "modified_*" 
    };
}