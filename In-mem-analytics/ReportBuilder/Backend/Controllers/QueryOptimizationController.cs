using Microsoft.AspNetCore.Mvc;
using ReportBuilder.Service;
using System.ComponentModel.DataAnnotations;

namespace ReportBuilder.Controllers;

/// <summary>
/// Controller for SQL query optimization services that analyze and suggest improvements for DuckDB queries.
/// Provides endpoints for query analysis, optimization suggestions, and performance recommendations.
/// </summary>
[ApiController]
[Route("api/query-optimization")]
public class QueryOptimizationController : ControllerBase
{
    private readonly ISqlQueryOptimizerService _optimizerService;
    private readonly ILogger<QueryOptimizationController> _logger;

    /// <summary>
    /// Initializes a new instance of the QueryOptimizationController.
    /// </summary>
    /// <param name="optimizerService">Service for analyzing and optimizing SQL queries</param>
    /// <param name="logger">Logger for recording optimization events and metrics</param>
    public QueryOptimizationController(
        ISqlQueryOptimizerService optimizerService,
        ILogger<QueryOptimizationController> logger)
    {
        _optimizerService = optimizerService;
        _logger = logger;
    }

    /// <summary>
    /// Analyzes a SQL query and provides comprehensive optimization suggestions including column projection and filter pushdown.
    /// Returns detailed analysis with estimated performance improvements and optimized query variants.
    /// </summary>
    /// <param name="request">Query optimization request containing SQL and context information</param>
    /// <returns>Detailed optimization analysis with suggestions and optimized queries</returns>
    [HttpPost("analyze")]
    public IActionResult AnalyzeQuery([FromBody] QueryOptimizationRequest request)
    {
        try
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(new
                {
                    success = false,
                    errorMessage = "Invalid request model",
                    validationErrors = ModelState.Values
                        .SelectMany(v => v.Errors)
                        .Select(e => e.ErrorMessage)
                        .ToList()
                });
            }

            _logger.LogInformation("Analyzing query for optimization: {QueryId}", request.QueryId);

            // Perform query analysis
            var analysisResult = _optimizerService.AnalyzeQuery(
                request.SqlQuery, 
                request.TableName ?? "input_data",
                request.AvailableColumns);

            // Add file optimization if file path provided
            if (!string.IsNullOrEmpty(request.FilePath))
            {
                var fileOptimization = _optimizerService.OptimizeFileRead(request.FilePath, request.SqlQuery);
                analysisResult.FileOptimization = fileOptimization;
            }

            // Validate optimization if requested
            QueryValidationResult? validationResult = null;
            if (!string.IsNullOrEmpty(analysisResult.OptimizedQuery) && request.ValidateOptimization)
            {
                validationResult = _optimizerService.ValidateOptimization(
                    analysisResult.OriginalQuery, 
                    analysisResult.OptimizedQuery);
            }

            var response = new
            {
                success = true,
                queryId = request.QueryId,
                analysis = analysisResult,
                validation = validationResult,
                timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
                optimizationSummary = new
                {
                    totalOptimizations = analysisResult.Optimizations.Count,
                    highImpactOptimizations = analysisResult.Optimizations.Count(o => 
                        o.EstimatedImpact.Contains("High") || o.EstimatedImpact.Contains("Very High")),
                    hasOptimizedQuery = !string.IsNullOrEmpty(analysisResult.OptimizedQuery),
                    hasFileOptimization = analysisResult.FileOptimization?.IsOptimized == true,
                    estimatedPerformanceGain = validationResult?.EstimatedPerformanceGain ?? 0.0
                }
            };

            _logger.LogInformation("Query optimization analysis completed for {QueryId}. Found {OptimizationCount} opportunities", 
                request.QueryId, analysisResult.Optimizations.Count);

            return Ok(response);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error analyzing query for optimization: {QueryId}", request.QueryId);
            return StatusCode(500, new
            {
                success = false,
                queryId = request.QueryId,
                errorMessage = "Internal server error occurred during query analysis",
                timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
            });
        }
    }

    /// <summary>
    /// Provides quick optimization suggestions for a SQL query without detailed analysis.
    /// Useful for real-time optimization hints in query editors.
    /// </summary>
    /// <param name="request">Quick optimization request with minimal context</param>
    /// <returns>Simplified optimization suggestions and quick wins</returns>
    [HttpPost("quick-suggestions")]
    public IActionResult GetQuickSuggestions([FromBody] QuickOptimizationRequest request)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(request.SqlQuery))
            {
                return BadRequest(new { success = false, errorMessage = "SQL query is required" });
            }

            _logger.LogDebug("Generating quick optimization suggestions");

            var analysisResult = _optimizerService.AnalyzeQuery(request.SqlQuery);

            var suggestions = analysisResult.Optimizations
                .Where(o => o.OptimizationType != OptimizationType.Information)
                .Select(o => new
                {
                    type = o.OptimizationType.ToString(),
                    title = o.Title,
                    impact = o.EstimatedImpact,
                    suggestion = o.Suggestion
                })
                .ToList();

            var response = new
            {
                success = true,
                hasOptimizations = suggestions.Any(),
                suggestionCount = suggestions.Count,
                suggestions = suggestions.Take(5).ToList(), // Limit to top 5 for quick response
                quickWins = analysisResult.Optimizations
                    .Where(o => o.EstimatedImpact.Contains("High"))
                    .Select(o => o.Title)
                    .Take(3)
                    .ToList()
            };

            return Ok(response);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error generating quick optimization suggestions");
            return StatusCode(500, new
            {
                success = false,
                errorMessage = "Failed to generate optimization suggestions"
            });
        }
    }

    /// <summary>
    /// Optimizes file reading operations for DuckDB with filter pushdown and column projection.
    /// Analyzes queries that read from files and suggests optimized file access patterns.
    /// </summary>
    /// <param name="request">File optimization request with file path and query context</param>
    /// <returns>Optimized file reading commands and suggestions</returns>
    [HttpPost("optimize-file-read")]
    public IActionResult OptimizeFileRead([FromBody] FileOptimizationRequest request)
    {
        try
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(new
                {
                    success = false,
                    errorMessage = "Invalid request model",
                    validationErrors = ModelState.Values
                        .SelectMany(v => v.Errors)
                        .Select(e => e.ErrorMessage)
                        .ToList()
                });
            }

            _logger.LogInformation("Optimizing file read for {FilePath}", request.FilePath);

            var optimizationResult = _optimizerService.OptimizeFileRead(request.FilePath, request.SqlQuery);

            var response = new
            {
                success = true,
                fileOptimization = optimizationResult,
                recommendations = new
                {
                    useColumnProjection = optimizationResult.ProjectedColumns?.Any() == true,
                    useFilterPushdown = optimizationResult.PushdownFilters?.Any() == true,
                    estimatedDataReduction = CalculateDataReduction(optimizationResult),
                    supportedOptimizations = GetSupportedOptimizations(optimizationResult.FileType)
                },
                timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
            };

            _logger.LogInformation("File optimization completed for {FileType} file", optimizationResult.FileType);

            return Ok(response);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error optimizing file read for {FilePath}", request.FilePath);
            return StatusCode(500, new
            {
                success = false,
                errorMessage = "Internal server error occurred during file optimization",
                timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
            });
        }
    }

    /// <summary>
    /// Validates that an optimized query maintains semantic equivalence with the original query.
    /// Provides safety assessment and estimated performance improvements.
    /// </summary>
    /// <param name="request">Query validation request with original and optimized queries</param>
    /// <returns>Validation result with safety assessment and performance estimates</returns>
    [HttpPost("validate-optimization")]
    public IActionResult ValidateOptimization([FromBody] QueryValidationRequest request)
    {
        try
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(new
                {
                    success = false,
                    errorMessage = "Invalid request model",
                    validationErrors = ModelState.Values
                        .SelectMany(v => v.Errors)
                        .Select(e => e.ErrorMessage)
                        .ToList()
                });
            }

            _logger.LogDebug("Validating query optimization");

            var validationResult = _optimizerService.ValidateOptimization(
                request.OriginalQuery, 
                request.OptimizedQuery);

            var response = new
            {
                success = true,
                validation = validationResult,
                recommendation = validationResult.IsValid 
                    ? (validationResult.EstimatedPerformanceGain > 0.1 
                        ? "Recommended - Safe optimization with good performance gain" 
                        : "Optional - Safe optimization with modest performance gain")
                    : "Not Recommended - Potential semantic changes detected",
                riskAssessment = validationResult.ValidationIssues.Any() 
                    ? "Medium - Review validation issues" 
                    : "Low - No issues detected",
                timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
            };

            return Ok(response);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error validating query optimization");
            return StatusCode(500, new
            {
                success = false,
                errorMessage = "Internal server error occurred during validation",
                timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
            });
        }
    }

    /// <summary>
    /// Returns optimization statistics and patterns from previous analyses.
    /// Useful for understanding common optimization opportunities in the system.
    /// </summary>
    /// <returns>Optimization statistics and common patterns</returns>
    [HttpGet("statistics")]
    public IActionResult GetOptimizationStatistics()
    {
        try
        {
            // In a real implementation, this would fetch statistics from a data store
            var response = new
            {
                success = true,
                statistics = new
                {
                    totalQueriesAnalyzed = 0, // Would come from metrics
                    commonOptimizations = new[]
                    {
                        new { type = "ColumnProjection", frequency = "65%", avgPerformanceGain = "35%" },
                        new { type = "FilterPushdown", frequency = "45%", avgPerformanceGain = "55%" },
                        new { type = "IndexUsage", frequency = "25%", avgPerformanceGain = "25%" }
                    },
                    fileTypeOptimizations = new
                    {
                        parquet = new { supportsPushdown = true, supportsProjection = true, avgGain = "60%" },
                        csv = new { supportsPushdown = false, supportsProjection = true, avgGain = "30%" }
                    }
                },
                timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
            };

            return Ok(response);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving optimization statistics");
            return StatusCode(500, new
            {
                success = false,
                errorMessage = "Failed to retrieve optimization statistics"
            });
        }
    }

    /// <summary>
    /// Calculates estimated data reduction percentage based on optimization results.
    /// </summary>
    /// <param name="result">File optimization result</param>
    /// <returns>Estimated data reduction percentage</returns>
    private static string CalculateDataReduction(FileReadOptimizationResult result)
    {
        var reductionFactors = new List<double>();

        // Column projection reduction
        if (result.ProjectedColumns?.Any() == true)
        {
            // Assume average 70% reduction with column projection
            reductionFactors.Add(0.7);
        }

        // Filter pushdown reduction  
        if (result.PushdownFilters?.Any() == true)
        {
            // Assume average 50% reduction with filter pushdown
            reductionFactors.Add(0.5);
        }

        if (!reductionFactors.Any())
            return "0%";

        // Calculate combined reduction (multiplicative effect)
        var totalReduction = reductionFactors.Aggregate(1.0, (acc, factor) => acc * (1 - factor));
        var reductionPercentage = (1 - totalReduction) * 100;

        return $"{reductionPercentage:F0}%";
    }

    /// <summary>
    /// Gets supported optimization types for a specific file format.
    /// </summary>
    /// <param name="fileType">File type (parquet, csv, etc.)</param>
    /// <returns>List of supported optimization types</returns>
    private static List<string> GetSupportedOptimizations(string fileType)
    {
        return fileType.ToLower() switch
        {
            "parquet" => new List<string> { "ColumnProjection", "FilterPushdown", "PredicatePushdown" },
            "csv" => new List<string> { "ColumnProjection", "RowSkipping" },
            _ => new List<string> { "ColumnProjection" }
        };
    }
}

// Request Models

/// <summary>
/// Request model for comprehensive query optimization analysis.
/// </summary>
public class QueryOptimizationRequest
{
    /// <summary>
    /// Unique identifier for tracking the optimization request.
    /// </summary>
    public string QueryId { get; set; } = Guid.NewGuid().ToString();

    /// <summary>
    /// SQL query to analyze for optimization opportunities.
    /// </summary>
    [Required]
    public string SqlQuery { get; set; } = string.Empty;

    /// <summary>
    /// Table name or dataset identifier for context.
    /// </summary>
    public string? TableName { get; set; }

    /// <summary>
    /// File path for file-specific optimizations (optional).
    /// </summary>
    public string? FilePath { get; set; }

    /// <summary>
    /// List of available columns in the dataset for projection optimization.
    /// </summary>
    public List<string>? AvailableColumns { get; set; }

    /// <summary>
    /// Whether to validate the optimized query for semantic equivalence.
    /// </summary>
    public bool ValidateOptimization { get; set; } = true;
}

/// <summary>
/// Request model for quick optimization suggestions.
/// </summary>
public class QuickOptimizationRequest
{
    /// <summary>
    /// SQL query to analyze for quick optimization suggestions.
    /// </summary>
    [Required]
    public string SqlQuery { get; set; } = string.Empty;
}

/// <summary>
/// Request model for file reading optimization.
/// </summary>
public class FileOptimizationRequest
{
    /// <summary>
    /// Path to the file to optimize reading for.
    /// </summary>
    [Required]
    public string FilePath { get; set; } = string.Empty;

    /// <summary>
    /// SQL query that will read from this file.
    /// </summary>
    [Required]
    public string SqlQuery { get; set; } = string.Empty;
}

/// <summary>
/// Request model for validating query optimizations.
/// </summary>
public class QueryValidationRequest
{
    /// <summary>
    /// Original SQL query before optimization.
    /// </summary>
    [Required]
    public string OriginalQuery { get; set; } = string.Empty;

    /// <summary>
    /// Optimized SQL query to validate.
    /// </summary>
    [Required]
    public string OptimizedQuery { get; set; } = string.Empty;
}