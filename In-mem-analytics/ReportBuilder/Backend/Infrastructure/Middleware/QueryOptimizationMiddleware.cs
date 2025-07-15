using ReportBuilder.Service;
using System.Text.Json;
using System.Text;

namespace ReportBuilder.Infrastructure.Middleware;

/// <summary>
/// Middleware for automatic SQL query optimization that analyzes incoming queries and provides optimization suggestions.
/// Integrates with the request pipeline to enhance query performance through column projection and filter pushdown.
/// </summary>
public class QueryOptimizationMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ISqlQueryOptimizerService _optimizerService;
    private readonly ILogger<QueryOptimizationMiddleware> _logger;

    /// <summary>
    /// Initializes a new instance of the QueryOptimizationMiddleware.
    /// </summary>
    /// <param name="next">The next middleware delegate in the pipeline</param>
    /// <param name="optimizerService">Service for analyzing and optimizing SQL queries</param>
    /// <param name="logger">Logger for recording optimization events and metrics</param>
    public QueryOptimizationMiddleware(
        RequestDelegate next,
        ISqlQueryOptimizerService optimizerService,
        ILogger<QueryOptimizationMiddleware> logger)
    {
        _next = next;
        _optimizerService = optimizerService;
        _logger = logger;
    }

    /// <summary>
    /// Processes HTTP requests and analyzes SQL queries for optimization opportunities.
    /// Adds optimization headers and can optionally modify queries based on configuration.
    /// </summary>
    /// <param name="context">The HTTP context for the current request</param>
    /// <returns>Task representing the asynchronous middleware execution</returns>
    public async Task InvokeAsync(HttpContext context)
    {
        // Only process POST requests to query endpoints
        if (!ShouldProcessRequest(context))
        {
            await _next(context);
            return;
        }

        try
        {
            // Capture the request body to analyze SQL queries
            var originalBodyStream = context.Request.Body;
            var requestBody = await ReadRequestBodyAsync(context.Request);
            
            // Reset the request body stream for downstream middleware
            context.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes(requestBody));

            // Extract and analyze SQL query from request
            var queryAnalysis = await AnalyzeRequestQueryAsync(requestBody, context.Request.Path);

            if (queryAnalysis != null)
            {
                // Add optimization headers to response
                AddOptimizationHeaders(context, queryAnalysis);
                
                // Log optimization opportunities
                LogOptimizationResults(queryAnalysis, context.Request.Path);
            }

            // Continue to next middleware
            await _next(context);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in query optimization middleware for {Path}", context.Request.Path);
            // Continue processing even if optimization fails
            await _next(context);
        }
    }

    /// <summary>
    /// Determines if the current request should be processed for query optimization.
    /// Targets analytics and query execution endpoints.
    /// </summary>
    /// <param name="context">HTTP context to evaluate</param>
    /// <returns>True if the request should be processed for optimization</returns>
    private static bool ShouldProcessRequest(HttpContext context)
    {
        if (context.Request.Method != "POST")
            return false;

        var path = context.Request.Path.Value?.ToLower() ?? "";
        
        // Target specific query endpoints
        return path.Contains("/query/") || 
               path.Contains("/analytics/") ||
               path.Contains("/execute") ||
               path.Contains("/run");
    }

    /// <summary>
    /// Reads the request body content for SQL query analysis.
    /// Preserves the original stream for downstream processing.
    /// </summary>
    /// <param name="request">HTTP request to read from</param>
    /// <returns>Request body content as string</returns>
    private static async Task<string> ReadRequestBodyAsync(HttpRequest request)
    {
        request.EnableBuffering();
        request.Body.Position = 0;
        
        using var reader = new StreamReader(request.Body, Encoding.UTF8, leaveOpen: true);
        var body = await reader.ReadToEndAsync();
        request.Body.Position = 0;
        
        return body;
    }

    /// <summary>
    /// Analyzes the SQL query from the request body and provides optimization suggestions.
    /// Handles different request formats and extracts SQL queries appropriately.
    /// </summary>
    /// <param name="requestBody">JSON request body containing SQL query</param>
    /// <param name="requestPath">Request path for context</param>
    /// <returns>Query optimization analysis result</returns>
    private async Task<QueryOptimizationResult?> AnalyzeRequestQueryAsync(string requestBody, PathString requestPath)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(requestBody))
                return null;

            // Parse JSON request to extract SQL query
            using var jsonDoc = JsonDocument.Parse(requestBody);
            var root = jsonDoc.RootElement;

            // Try different property names for SQL queries
            var sqlQuery = ExtractSqlQuery(root);
            if (string.IsNullOrWhiteSpace(sqlQuery))
                return null;

            // Extract additional context
            var filePath = ExtractFilePath(root);
            var tableName = ExtractTableName(root, requestPath);

            _logger.LogDebug("Analyzing SQL query for optimization: {QueryPreview}", 
                sqlQuery.Length > 100 ? sqlQuery[..100] + "..." : sqlQuery);

            // Perform optimization analysis
            var result = _optimizerService.AnalyzeQuery(sqlQuery, tableName);

            // Add file read optimization if file path is available
            if (!string.IsNullOrEmpty(filePath))
            {
                var fileOptimization = _optimizerService.OptimizeFileRead(filePath, sqlQuery);
                result.FileOptimization = fileOptimization;
            }

            return result;
        }
        catch (JsonException ex)
        {
            _logger.LogWarning("Failed to parse request JSON for query optimization: {Error}", ex.Message);
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error analyzing request query for optimization");
            return null;
        }
    }

    /// <summary>
    /// Extracts SQL query from various possible JSON property names.
    /// Handles different request formats across controllers.
    /// </summary>
    /// <param name="json">JSON root element to search</param>
    /// <returns>SQL query string if found</returns>
    private static string? ExtractSqlQuery(JsonElement json)
    {
        var possibleProperties = new[] { "sqlQuery", "query", "sql", "statement" };
        
        foreach (var property in possibleProperties)
        {
            if (json.TryGetProperty(property, out var element) && element.ValueKind == JsonValueKind.String)
            {
                return element.GetString();
            }
        }
        
        return null;
    }

    /// <summary>
    /// Extracts file path from JSON request for file-specific optimizations.
    /// </summary>
    /// <param name="json">JSON root element to search</param>
    /// <returns>File path if found</returns>
    private static string? ExtractFilePath(JsonElement json)
    {
        var possibleProperties = new[] { "filePath", "csvFilePath", "adlsFileUri", "datasetPath", "fileName" };
        
        foreach (var property in possibleProperties)
        {
            if (json.TryGetProperty(property, out var element) && element.ValueKind == JsonValueKind.String)
            {
                return element.GetString();
            }
        }
        
        return null;
    }

    /// <summary>
    /// Determines the table name context based on request path and content.
    /// </summary>
    /// <param name="json">JSON request content</param>
    /// <param name="requestPath">Request path for context</param>
    /// <returns>Table name for query analysis</returns>
    private static string ExtractTableName(JsonElement json, PathString requestPath)
    {
        // Try to get explicit table name
        if (json.TryGetProperty("tableName", out var element) && element.ValueKind == JsonValueKind.String)
        {
            return element.GetString() ?? "input_data";
        }

        // Infer from request path
        var path = requestPath.Value?.ToLower() ?? "";
        if (path.Contains("csv")) return "csv_data";
        if (path.Contains("parquet")) return "parquet_data";
        if (path.Contains("arrow")) return "arrow_data";
        
        return "input_data";
    }

    /// <summary>
    /// Adds optimization analysis results as HTTP response headers.
    /// Provides client applications with optimization suggestions without modifying response body.
    /// </summary>
    /// <param name="context">HTTP context to add headers to</param>
    /// <param name="analysis">Query optimization analysis results</param>
    private void AddOptimizationHeaders(HttpContext context, QueryOptimizationResult analysis)
    {
        try
        {
            // Add optimization suggestion count
            context.Response.Headers["X-Query-Optimizations"] = analysis.Optimizations.Count.ToString();
            
            // Add optimization types found
            if (analysis.Optimizations.Any())
            {
                var optimizationTypes = analysis.Optimizations
                    .Select(o => o.OptimizationType.ToString())
                    .Distinct();
                context.Response.Headers["X-Optimization-Types"] = string.Join(",", optimizationTypes);
            }

            // Add performance impact indicator
            var highImpactOptimizations = analysis.Optimizations
                .Count(o => o.EstimatedImpact.Contains("High") || o.EstimatedImpact.Contains("Very High"));
            
            if (highImpactOptimizations > 0)
            {
                context.Response.Headers["X-High-Impact-Optimizations"] = highImpactOptimizations.ToString();
            }

            // Add optimized query availability
            if (!string.IsNullOrEmpty(analysis.OptimizedQuery))
            {
                context.Response.Headers["X-Optimized-Query-Available"] = "true";
            }

            // Add file optimization info
            if (analysis.FileOptimization?.IsOptimized == true)
            {
                context.Response.Headers["X-File-Optimization-Available"] = "true";
                if (analysis.FileOptimization.PushdownFilters?.Any() == true)
                {
                    context.Response.Headers["X-Pushdown-Filters"] = analysis.FileOptimization.PushdownFilters.Count.ToString();
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to add optimization headers");
        }
    }

    /// <summary>
    /// Logs optimization analysis results for monitoring and debugging.
    /// Provides insights into query patterns and optimization opportunities.
    /// </summary>
    /// <param name="analysis">Query optimization analysis results</param>
    /// <param name="requestPath">Request path for context</param>
    private void LogOptimizationResults(QueryOptimizationResult analysis, PathString requestPath)
    {
        var optimizationCount = analysis.Optimizations.Count;
        var highImpactCount = analysis.Optimizations.Count(o => 
            o.EstimatedImpact.Contains("High") || o.EstimatedImpact.Contains("Very High"));

        if (optimizationCount > 0)
        {
            _logger.LogInformation(
                "Query optimization analysis for {Path}: {OptimizationCount} opportunities found, {HighImpactCount} high-impact",
                requestPath, optimizationCount, highImpactCount);

            // Log specific optimization types for analytics
            var optimizationTypes = analysis.Optimizations
                .GroupBy(o => o.OptimizationType)
                .Select(g => $"{g.Key}({g.Count()})")
                .ToList();

            _logger.LogDebug("Optimization types found: {OptimizationTypes}", string.Join(", ", optimizationTypes));
        }
        else
        {
            _logger.LogDebug("No optimization opportunities found for query in {Path}", requestPath);
        }
    }
}

/// <summary>
/// Extension methods for registering query optimization middleware.
/// </summary>
public static class QueryOptimizationMiddlewareExtensions
{
    /// <summary>
    /// Adds query optimization middleware to the application pipeline.
    /// Should be placed early in the pipeline to capture all query requests.
    /// </summary>
    /// <param name="builder">Application builder to configure</param>
    /// <returns>Application builder for method chaining</returns>
    public static IApplicationBuilder UseQueryOptimization(this IApplicationBuilder builder)
    {
        return builder.UseMiddleware<QueryOptimizationMiddleware>();
    }
}

/// <summary>
/// Extended query optimization result with file optimization details.
/// </summary>
public partial class QueryOptimizationResult
{
    public FileReadOptimizationResult? FileOptimization { get; set; }
}