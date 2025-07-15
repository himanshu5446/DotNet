using System.Text.RegularExpressions;
using System.Text.Json;

namespace ReportBuilder.Service;

/// <summary>
/// Interface for SQL query optimization services that analyze and suggest improvements for DuckDB queries.
/// Provides column projection optimization, filter pushdown detection, and performance recommendations.
/// </summary>
public interface ISqlQueryOptimizerService
{
    /// <summary>
    /// Analyzes a SQL query and provides optimization suggestions including column projection and filter pushdown.
    /// </summary>
    /// <param name="originalQuery">The original SQL query to analyze</param>
    /// <param name="tableName">The table name or file reference (e.g., 'input_data', file path)</param>
    /// <param name="availableColumns">List of available columns in the dataset, if known</param>
    /// <returns>Query optimization analysis with suggestions and optimized query</returns>
    QueryOptimizationResult AnalyzeQuery(string originalQuery, string tableName = "input_data", List<string>? availableColumns = null);

    /// <summary>
    /// Attempts to optimize a file reading operation (parquet_scan, read_csv_auto) with filter pushdown.
    /// </summary>
    /// <param name="filePath">Path to the file being read</param>
    /// <param name="query">The SQL query that will use this file</param>
    /// <returns>Optimized file reading command with pushed-down filters</returns>
    FileReadOptimizationResult OptimizeFileRead(string filePath, string query);

    /// <summary>
    /// Validates if a query can be safely optimized without changing its semantic meaning.
    /// </summary>
    /// <param name="originalQuery">Original query</param>
    /// <param name="optimizedQuery">Proposed optimized query</param>
    /// <returns>Validation result with safety assessment</returns>
    QueryValidationResult ValidateOptimization(string originalQuery, string optimizedQuery);
}

/// <summary>
/// Advanced SQL query optimizer service that analyzes DuckDB queries for performance improvements.
/// Implements column projection optimization, filter pushdown detection, and query rewriting capabilities.
/// </summary>
public class SqlQueryOptimizerService : ISqlQueryOptimizerService
{
    private readonly ILogger<SqlQueryOptimizerService> _logger;
    
    // Regex patterns for SQL parsing
    private static readonly Regex SelectPattern = new(@"SELECT\s+(.*?)\s+FROM", RegexOptions.IgnoreCase | RegexOptions.Singleline);
    private static readonly Regex WherePattern = new(@"WHERE\s+(.*?)(?:\s+ORDER\s+BY|\s+GROUP\s+BY|\s+HAVING|\s+LIMIT|$)", RegexOptions.IgnoreCase | RegexOptions.Singleline);
    private static readonly Regex FromPattern = new(@"FROM\s+(\w+|\([^)]+\))", RegexOptions.IgnoreCase);
    private static readonly Regex JoinPattern = new(@"\b(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?JOIN\b", RegexOptions.IgnoreCase);
    private static readonly Regex AggregatePattern = new(@"\b(?:COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT)\s*\(", RegexOptions.IgnoreCase);
    private static readonly Regex SubqueryPattern = new(@"\(\s*SELECT\b", RegexOptions.IgnoreCase);
    private static readonly Regex FileReadPattern = new(@"\b(?:parquet_scan|read_csv_auto|read_parquet)\s*\(\s*['""]([^'""]+)['""]", RegexOptions.IgnoreCase);

    public SqlQueryOptimizerService(ILogger<SqlQueryOptimizerService> logger)
    {
        _logger = logger;
    }

    public QueryOptimizationResult AnalyzeQuery(string originalQuery, string tableName = "input_data", List<string>? availableColumns = null)
    {
        try
        {
            _logger.LogDebug("Starting query analysis for optimization");
            
            var result = new QueryOptimizationResult
            {
                OriginalQuery = originalQuery.Trim(),
                TableName = tableName,
                AvailableColumns = availableColumns ?? new List<string>()
            };

            // Parse the SELECT clause
            var selectAnalysis = AnalyzeSelectClause(originalQuery);
            result.SelectAnalysis = selectAnalysis;

            // Parse the WHERE clause
            var whereAnalysis = AnalyzeWhereClause(originalQuery);
            result.WhereAnalysis = whereAnalysis;

            // Check for query complexity
            var complexityAnalysis = AnalyzeQueryComplexity(originalQuery);
            result.ComplexityAnalysis = complexityAnalysis;

            // Generate optimization suggestions
            result.Optimizations = GenerateOptimizations(result);

            // Create optimized query if possible
            if (result.Optimizations.Any(o => o.OptimizationType != OptimizationType.Information))
            {
                result.OptimizedQuery = GenerateOptimizedQuery(result);
            }

            _logger.LogInformation("Query analysis completed. Found {OptimizationCount} optimization opportunities", 
                result.Optimizations.Count);

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error analyzing query for optimization");
            return new QueryOptimizationResult
            {
                OriginalQuery = originalQuery,
                IsValid = false,
                ErrorMessage = $"Failed to analyze query: {ex.Message}"
            };
        }
    }

    public FileReadOptimizationResult OptimizeFileRead(string filePath, string query)
    {
        try
        {
            _logger.LogDebug("Optimizing file read operation for {FilePath}", filePath);
            
            var result = new FileReadOptimizationResult
            {
                OriginalFilePath = filePath,
                OriginalQuery = query
            };

            // Detect file type
            var extension = Path.GetExtension(filePath).ToLower();
            result.FileType = extension switch
            {
                ".parquet" => "parquet",
                ".csv" => "csv",
                _ => "unknown"
            };

            // Extract WHERE conditions that can be pushed down
            var whereMatch = WherePattern.Match(query);
            if (whereMatch.Success)
            {
                var whereClause = whereMatch.Groups[1].Value.Trim();
                result.PushdownFilters = ExtractPushdownFilters(whereClause);
            }

            // Extract column projections
            var selectMatch = SelectPattern.Match(query);
            if (selectMatch.Success)
            {
                var selectClause = selectMatch.Groups[1].Value.Trim();
                if (selectClause != "*")
                {
                    result.ProjectedColumns = ParseSelectColumns(selectClause);
                }
            }

            // Generate optimized file read command
            result.OptimizedFileRead = GenerateOptimizedFileRead(result);

            _logger.LogInformation("File read optimization completed for {FileType} file", result.FileType);
            
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error optimizing file read for {FilePath}", filePath);
            return new FileReadOptimizationResult
            {
                OriginalFilePath = filePath,
                OriginalQuery = query,
                IsOptimized = false,
                ErrorMessage = $"Failed to optimize file read: {ex.Message}"
            };
        }
    }

    public QueryValidationResult ValidateOptimization(string originalQuery, string optimizedQuery)
    {
        try
        {
            var result = new QueryValidationResult
            {
                OriginalQuery = originalQuery,
                OptimizedQuery = optimizedQuery,
                IsValid = true
            };

            // Basic validation checks
            var validationIssues = new List<string>();

            // Check if both queries have SELECT statements
            if (!ContainsSelect(originalQuery) || !ContainsSelect(optimizedQuery))
            {
                validationIssues.Add("One or both queries do not contain valid SELECT statements");
            }

            // Check for potential semantic changes
            if (HasAggregation(originalQuery) != HasAggregation(optimizedQuery))
            {
                validationIssues.Add("Aggregation behavior may have changed");
            }

            if (HasJoins(originalQuery) != HasJoins(optimizedQuery))
            {
                validationIssues.Add("JOIN behavior may have changed");
            }

            result.ValidationIssues = validationIssues;
            result.IsValid = !validationIssues.Any();
            result.EstimatedPerformanceGain = EstimatePerformanceGain(originalQuery, optimizedQuery);

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error validating query optimization");
            return new QueryValidationResult
            {
                OriginalQuery = originalQuery,
                OptimizedQuery = optimizedQuery,
                IsValid = false,
                ValidationIssues = new List<string> { $"Validation failed: {ex.Message}" }
            };
        }
    }

    private SelectClauseAnalysis AnalyzeSelectClause(string query)
    {
        var analysis = new SelectClauseAnalysis();
        
        var match = SelectPattern.Match(query);
        if (match.Success)
        {
            var selectClause = match.Groups[1].Value.Trim();
            analysis.RawSelectClause = selectClause;
            analysis.HasSelectStar = selectClause.Contains("*");
            
            if (!analysis.HasSelectStar)
            {
                analysis.SelectedColumns = ParseSelectColumns(selectClause);
            }
            
            analysis.HasAggregation = AggregatePattern.IsMatch(selectClause);
            analysis.HasSubqueries = SubqueryPattern.IsMatch(selectClause);
        }

        return analysis;
    }

    private WhereClauseAnalysis AnalyzeWhereClause(string query)
    {
        var analysis = new WhereClauseAnalysis();
        
        var match = WherePattern.Match(query);
        if (match.Success)
        {
            var whereClause = match.Groups[1].Value.Trim();
            analysis.RawWhereClause = whereClause;
            analysis.HasWhereClause = true;
            analysis.PushdownCandidates = ExtractPushdownFilters(whereClause);
        }

        return analysis;
    }

    private QueryComplexityAnalysis AnalyzeQueryComplexity(string query)
    {
        return new QueryComplexityAnalysis
        {
            HasJoins = JoinPattern.IsMatch(query),
            HasSubqueries = SubqueryPattern.IsMatch(query),
            HasAggregation = AggregatePattern.IsMatch(query),
            EstimatedComplexity = EstimateComplexity(query)
        };
    }

    private List<QueryOptimization> GenerateOptimizations(QueryOptimizationResult analysis)
    {
        var optimizations = new List<QueryOptimization>();

        // Column projection optimization
        if (analysis.SelectAnalysis?.HasSelectStar == true)
        {
            optimizations.Add(new QueryOptimization
            {
                OptimizationType = OptimizationType.ColumnProjection,
                Title = "Remove SELECT * for better performance",
                Description = "SELECT * reads all columns which may include unnecessary data. Consider specifying only needed columns.",
                EstimatedImpact = "High - Reduces I/O and memory usage",
                Suggestion = "Replace SELECT * with specific column names: SELECT col1, col2, col3 FROM ..."
            });
        }

        // Filter pushdown optimization
        if (analysis.WhereAnalysis?.PushdownCandidates?.Any() == true)
        {
            optimizations.Add(new QueryOptimization
            {
                OptimizationType = OptimizationType.FilterPushdown,
                Title = "Push filters down to file reading",
                Description = "WHERE conditions can be applied during file reading to reduce data loading.",
                EstimatedImpact = "Very High - Significantly reduces data processed",
                Suggestion = $"Apply filters at file level: {string.Join(", ", analysis.WhereAnalysis.PushdownCandidates)}"
            });
        }

        // Add informational suggestions
        if (!analysis.ComplexityAnalysis?.HasJoins == true && !analysis.ComplexityAnalysis?.HasSubqueries == true)
        {
            optimizations.Add(new QueryOptimization
            {
                OptimizationType = OptimizationType.Information,
                Title = "Query is suitable for optimization",
                Description = "This query has low complexity and can benefit from file-level optimizations.",
                EstimatedImpact = "Medium - Good candidate for filter pushdown and column projection"
            });
        }

        return optimizations;
    }

    private string? GenerateOptimizedQuery(QueryOptimizationResult analysis)
    {
        try
        {
            var query = analysis.OriginalQuery;
            
            // Apply column projection if SELECT * is found and we have available columns
            if (analysis.SelectAnalysis?.HasSelectStar == true && analysis.AvailableColumns.Any())
            {
                // For demo purposes, suggest first 10 columns or common column names
                var suggestedColumns = analysis.AvailableColumns.Take(10).ToList();
                if (!suggestedColumns.Any())
                {
                    suggestedColumns = new List<string> { "id", "name", "created_date", "status" }; // Common column examples
                }
                
                query = SelectPattern.Replace(query, $"SELECT {string.Join(", ", suggestedColumns)} FROM");
            }

            return query;
        }
        catch
        {
            return null;
        }
    }

    private List<string> ParseSelectColumns(string selectClause)
    {
        return selectClause.Split(',')
            .Select(col => col.Trim())
            .Where(col => !string.IsNullOrEmpty(col))
            .ToList();
    }

    private List<string> ExtractPushdownFilters(string whereClause)
    {
        var filters = new List<string>();
        
        // Simple regex patterns for common filterable conditions
        var filterPatterns = new[]
        {
            @"(\w+)\s*=\s*['""]?([^'""]+)['""]?",  // column = value
            @"(\w+)\s*>\s*(\d+)",                   // column > number
            @"(\w+)\s*<\s*(\d+)",                   // column < number
            @"(\w+)\s*IN\s*\([^)]+\)",              // column IN (values)
            @"(\w+)\s*LIKE\s*['""]([^'""]+)['""]"   // column LIKE pattern
        };

        foreach (var pattern in filterPatterns)
        {
            var matches = Regex.Matches(whereClause, pattern, RegexOptions.IgnoreCase);
            foreach (Match match in matches)
            {
                filters.Add(match.Value.Trim());
            }
        }

        return filters.Distinct().ToList();
    }

    private string GenerateOptimizedFileRead(FileReadOptimizationResult result)
    {
        var fileFunction = result.FileType switch
        {
            "parquet" => "parquet_scan",
            "csv" => "read_csv_auto", 
            _ => "read_csv_auto"
        };

        var optimizedRead = $"{fileFunction}('{result.OriginalFilePath}'";

        // Add column projection if available
        if (result.ProjectedColumns?.Any() == true)
        {
            optimizedRead += $", columns=[{string.Join(", ", result.ProjectedColumns.Select(c => $"'{c}'"))}]";
        }

        // Add filter pushdown if available (syntax varies by file type)
        if (result.PushdownFilters?.Any() == true && result.FileType == "parquet")
        {
            // Parquet supports filter pushdown
            optimizedRead += $", filter='{string.Join(" AND ", result.PushdownFilters)}'";
        }

        optimizedRead += ")";
        return optimizedRead;
    }

    private bool ContainsSelect(string query) => SelectPattern.IsMatch(query);
    private bool HasAggregation(string query) => AggregatePattern.IsMatch(query);
    private bool HasJoins(string query) => JoinPattern.IsMatch(query);

    private QueryComplexity EstimateComplexity(string query)
    {
        var score = 0;
        if (HasJoins(query)) score += 3;
        if (SubqueryPattern.IsMatch(query)) score += 2;
        if (HasAggregation(query)) score += 1;
        
        return score switch
        {
            0 => QueryComplexity.Simple,
            1 or 2 => QueryComplexity.Medium,
            _ => QueryComplexity.Complex
        };
    }

    private double EstimatePerformanceGain(string originalQuery, string optimizedQuery)
    {
        // Simple heuristic: estimate performance gain based on optimizations applied
        double gain = 0.0;
        
        if (SelectPattern.Match(originalQuery).Groups[1].Value.Contains("*") && 
            !SelectPattern.Match(optimizedQuery).Groups[1].Value.Contains("*"))
        {
            gain += 0.3; // 30% improvement from column projection
        }
        
        if (WherePattern.IsMatch(optimizedQuery))
        {
            gain += 0.5; // 50% improvement from filter pushdown
        }
        
        return Math.Min(gain, 0.8); // Cap at 80% estimated improvement
    }
}

// Response Models

public class QueryOptimizationResult
{
    public string OriginalQuery { get; set; } = string.Empty;
    public string? OptimizedQuery { get; set; }
    public string TableName { get; set; } = string.Empty;
    public List<string> AvailableColumns { get; set; } = new();
    public bool IsValid { get; set; } = true;
    public string? ErrorMessage { get; set; }
    
    public SelectClauseAnalysis? SelectAnalysis { get; set; }
    public WhereClauseAnalysis? WhereAnalysis { get; set; }
    public QueryComplexityAnalysis? ComplexityAnalysis { get; set; }
    public List<QueryOptimization> Optimizations { get; set; } = new();
}

public class SelectClauseAnalysis
{
    public string RawSelectClause { get; set; } = string.Empty;
    public bool HasSelectStar { get; set; }
    public List<string> SelectedColumns { get; set; } = new();
    public bool HasAggregation { get; set; }
    public bool HasSubqueries { get; set; }
}

public class WhereClauseAnalysis
{
    public bool HasWhereClause { get; set; }
    public string RawWhereClause { get; set; } = string.Empty;
    public List<string> PushdownCandidates { get; set; } = new();
}

public class QueryComplexityAnalysis
{
    public bool HasJoins { get; set; }
    public bool HasSubqueries { get; set; }
    public bool HasAggregation { get; set; }
    public QueryComplexity EstimatedComplexity { get; set; }
}

public class QueryOptimization
{
    public OptimizationType OptimizationType { get; set; }
    public string Title { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public string EstimatedImpact { get; set; } = string.Empty;
    public string? Suggestion { get; set; }
}

public class FileReadOptimizationResult
{
    public string OriginalFilePath { get; set; } = string.Empty;
    public string OriginalQuery { get; set; } = string.Empty;
    public string FileType { get; set; } = string.Empty;
    public List<string>? ProjectedColumns { get; set; }
    public List<string>? PushdownFilters { get; set; }
    public string OptimizedFileRead { get; set; } = string.Empty;
    public bool IsOptimized { get; set; } = true;
    public string? ErrorMessage { get; set; }
}

public class QueryValidationResult
{
    public string OriginalQuery { get; set; } = string.Empty;
    public string OptimizedQuery { get; set; } = string.Empty;
    public bool IsValid { get; set; }
    public List<string> ValidationIssues { get; set; } = new();
    public double EstimatedPerformanceGain { get; set; }
}

public enum OptimizationType
{
    ColumnProjection,
    FilterPushdown,
    IndexUsage,
    JoinOptimization,
    Information
}

public enum QueryComplexity
{
    Simple,
    Medium,
    Complex
}