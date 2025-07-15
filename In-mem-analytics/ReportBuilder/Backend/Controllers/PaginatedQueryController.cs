using Microsoft.AspNetCore.Mvc;
using ReportBuilder.Request;
using ReportBuilder.Response;
using ReportBuilder.Service;
using System.Runtime.CompilerServices;

namespace ReportBuilder.Controllers;

/// <summary>
/// Controller for executing paginated SQL queries with comprehensive navigation and performance monitoring.
/// Provides endpoints for efficient pagination of large datasets with memory usage tracking.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public class PaginatedQueryController : ControllerBase
{
    private readonly IPaginatedQueryService _paginatedQueryService;
    private readonly ILogger<PaginatedQueryController> _logger;

    /// <summary>
    /// Initializes a new instance of the PaginatedQueryController.
    /// </summary>
    /// <param name="paginatedQueryService">Service for handling paginated query execution</param>
    /// <param name="logger">Logger for recording pagination performance and events</param>
    public PaginatedQueryController(
        IPaginatedQueryService paginatedQueryService,
        ILogger<PaginatedQueryController> logger)
    {
        _paginatedQueryService = paginatedQueryService;
        _logger = logger;
    }

    /// <summary>
    /// Executes a paginated SQL query using GET parameters with comprehensive pagination support.
    /// Streams results for the specified page with navigation metadata and performance tracking.
    /// </summary>
    /// <param name="sqlQuery">SQL query to execute</param>
    /// <param name="filePath">Path to the data file</param>
    /// <param name="fileFormat">Format of the data file (default: Auto-detect)</param>
    /// <param name="pageNumber">Page number to retrieve (1-based, default: 1)</param>
    /// <param name="pageSize">Number of rows per page (default: 1000, max: 10000)</param>
    /// <param name="includeTotalCount">Whether to include total row count in response</param>
    /// <param name="enableMemoryLogging">Whether to track memory usage during execution</param>
    /// <param name="timeoutSeconds">Query timeout in seconds (default: 120, max: 1800)</param>
    /// <param name="whereClause">Optional WHERE clause to filter results</param>
    /// <param name="orderBy">Optional ORDER BY clause for result ordering</param>
    /// <returns>Paginated query results with navigation metadata</returns>
    [HttpGet("execute")]
    public async Task<IActionResult> ExecutePaginatedQuery(
        [FromQuery] string sqlQuery,
        [FromQuery] string filePath,
        [FromQuery] PaginatedFileFormat fileFormat = PaginatedFileFormat.Auto,
        [FromQuery] int pageNumber = 1,
        [FromQuery] int pageSize = 1000,
        [FromQuery] bool includeTotalCount = false,
        [FromQuery] bool enableMemoryLogging = true,
        [FromQuery] int timeoutSeconds = 120,
        [FromQuery] string? whereClause = null,
        [FromQuery] string? orderBy = null)
    {
        var cancellationToken = HttpContext.RequestAborted;

        try
        {
            var request = new PaginatedQueryRequest
            {
                SqlQuery = sqlQuery,
                FilePath = filePath,
                FileFormat = fileFormat,
                PageNumber = pageNumber,
                PageSize = pageSize,
                IncludeTotalCount = includeTotalCount,
                EnableMemoryLogging = enableMemoryLogging,
                TimeoutSeconds = timeoutSeconds,
                WhereClause = whereClause,
                OrderBy = orderBy
            };

            var validation = ValidateQueryParameters(request);
            if (!validation.isValid)
            {
                return BadRequest(new PaginatedQueryErrorResponse
                {
                    ErrorMessage = validation.errorMessage,
                    MemoryUsage = GC.GetTotalMemory(false),
                    ValidationErrors = new List<string> { validation.errorMessage }
                });
            }

            _logger.LogInformation("Executing paginated query. Page: {PageNumber}, Size: {PageSize}, File: {FilePath}", 
                pageNumber, pageSize, filePath);

            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));

            var result = await _paginatedQueryService.ExecutePaginatedQueryAsync(request, timeoutCts.Token);

            if (!result.Success)
            {
                return BadRequest(new PaginatedQueryErrorResponse
                {
                    ErrorMessage = result.ErrorMessage ?? "Query execution failed",
                    MemoryUsage = result.Performance.FinalMemoryUsage,
                    Suggestions = GetErrorSuggestions(result.ErrorMessage)
                });
            }

            return await StreamPaginatedResults(result, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Paginated query was cancelled");
            return StatusCode(408, new PaginatedQueryErrorResponse
            {
                ErrorMessage = "Query execution timed out or was cancelled",
                MemoryUsage = GC.GetTotalMemory(false)
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing paginated query");
            return StatusCode(500, new PaginatedQueryErrorResponse
            {
                ErrorMessage = "Internal server error occurred while executing the query",
                MemoryUsage = GC.GetTotalMemory(false),
                Suggestions = new List<string>
                {
                    "Check SQL syntax and file path",
                    "Verify file format is supported",
                    "Try reducing page size",
                    "Check server logs for details"
                }
            });
        }
    }

    /// <summary>
    /// Executes a paginated SQL query using POST request body for complex queries.
    /// Provides the same functionality as the GET endpoint but supports larger query parameters.
    /// </summary>
    /// <param name="request">Paginated query request containing all query parameters</param>
    /// <returns>Paginated query results with navigation metadata</returns>
    [HttpPost("execute")]
    public async Task<IActionResult> ExecutePaginatedQueryPost([FromBody] PaginatedQueryRequest request)
    {
        var cancellationToken = HttpContext.RequestAborted;

        if (!ModelState.IsValid)
        {
            return BadRequest(ModelState);
        }

        try
        {
            var validation = ValidateQueryParameters(request);
            if (!validation.isValid)
            {
                return BadRequest(new PaginatedQueryErrorResponse
                {
                    ErrorMessage = validation.errorMessage,
                    MemoryUsage = GC.GetTotalMemory(false),
                    ValidationErrors = new List<string> { validation.errorMessage }
                });
            }

            _logger.LogInformation("Executing paginated query (POST). Page: {PageNumber}, Size: {PageSize}, File: {FilePath}", 
                request.PageNumber, request.PageSize, request.FilePath);

            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(request.TimeoutSeconds));

            var result = await _paginatedQueryService.ExecutePaginatedQueryAsync(request, timeoutCts.Token);

            if (!result.Success)
            {
                return BadRequest(new PaginatedQueryErrorResponse
                {
                    ErrorMessage = result.ErrorMessage ?? "Query execution failed",
                    MemoryUsage = result.Performance.FinalMemoryUsage,
                    Suggestions = GetErrorSuggestions(result.ErrorMessage)
                });
            }

            return await StreamPaginatedResults(result, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Paginated query was cancelled");
            return StatusCode(408, new PaginatedQueryErrorResponse
            {
                ErrorMessage = "Query execution timed out or was cancelled",
                MemoryUsage = GC.GetTotalMemory(false)
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing paginated query");
            return StatusCode(500, new PaginatedQueryErrorResponse
            {
                ErrorMessage = "Internal server error occurred while executing the query",
                MemoryUsage = GC.GetTotalMemory(false)
            });
        }
    }

    /// <summary>
    /// Gets pagination metadata without executing the full query.
    /// Returns total row count, page information, and navigation recommendations.
    /// </summary>
    /// <param name="sqlQuery">SQL query to analyze</param>
    /// <param name="filePath">Path to the data file</param>
    /// <param name="fileFormat">Format of the data file (default: Auto-detect)</param>
    /// <param name="pageSize">Page size for calculating pagination info (default: 1000)</param>
    /// <param name="whereClause">Optional WHERE clause to filter results</param>
    /// <returns>Pagination metadata, performance estimates, and navigation suggestions</returns>
    [HttpGet("pages/info")]
    public async Task<IActionResult> GetPaginationInfo(
        [FromQuery] string sqlQuery,
        [FromQuery] string filePath,
        [FromQuery] PaginatedFileFormat fileFormat = PaginatedFileFormat.Auto,
        [FromQuery] int pageSize = 1000,
        [FromQuery] string? whereClause = null)
    {
        var cancellationToken = HttpContext.RequestAborted;

        try
        {
            var request = new PaginatedQueryRequest
            {
                SqlQuery = sqlQuery,
                FilePath = filePath,
                FileFormat = fileFormat,
                PageNumber = 1, // We only need info, not actual data
                PageSize = pageSize,
                IncludeTotalCount = true,
                EnableMemoryLogging = false,
                WhereClause = whereClause
            };

            var validation = ValidateQueryParameters(request);
            if (!validation.isValid)
            {
                return BadRequest(new { error = validation.errorMessage });
            }

            // Get just the metadata without streaming data
            var result = await _paginatedQueryService.ExecutePaginatedQueryAsync(request, cancellationToken);

            if (!result.Success)
            {
                return BadRequest(new { error = result.ErrorMessage });
            }

            var info = new
            {
                totalRows = result.Pagination.TotalRows,
                totalPages = result.Pagination.TotalPages,
                pageSize = result.Pagination.PageSize,
                fileInfo = new
                {
                    filePath = result.Query.FilePath,
                    fileFormat = result.Query.FileFormat,
                    fileSizeKB = result.Query.FileSize / 1024,
                    columns = result.Query.Columns
                },
                performance = new
                {
                    queryExecutionTimeMs = result.Performance.QueryExecutionTime.TotalMilliseconds,
                    dataLoadTimeMs = result.Performance.DataLoadTime.TotalMilliseconds,
                    memoryUsageMB = result.Performance.FinalMemoryUsage / (1024 * 1024)
                },
                suggestedPageSizes = GetSuggestedPageSizes(result.Pagination.TotalRows),
                navigationHints = new
                {
                    firstPage = 1,
                    lastPage = result.Pagination.TotalPages,
                    maxRecommendedPage = Math.Min(result.Pagination.TotalPages ?? 1000, 1000), // Limit deep pagination
                    deepPaginationWarning = result.Pagination.TotalPages > 1000 
                        ? "Consider using filters to reduce result set for pages beyond 1000" 
                        : null
                }
            };

            return Ok(info);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting pagination info");
            return StatusCode(500, new { error = "Failed to get pagination information" });
        }
    }

    /// <summary>
    /// Streams paginated query results as JSON with real-time navigation metadata.
    /// Includes pagination info, query metadata, performance metrics, and navigation URLs.
    /// </summary>
    /// <param name="result">Paginated query result to stream</param>
    /// <param name="cancellationToken">Token for cancelling the streaming operation</param>
    /// <returns>Empty result after streaming completion</returns>
    private async Task<IActionResult> StreamPaginatedResults(
        PaginatedResponse<Dictionary<string, object?>> result, 
        CancellationToken cancellationToken)
    {
        Response.ContentType = "application/json";
        
        // Write response header
        await Response.WriteAsync("{", cancellationToken);
        await Response.WriteAsync($"\"success\":true,", cancellationToken);
        await Response.WriteAsync($"\"pagination\":{System.Text.Json.JsonSerializer.Serialize(result.Pagination)},", cancellationToken);
        await Response.WriteAsync($"\"query\":{System.Text.Json.JsonSerializer.Serialize(result.Query)},", cancellationToken);
        await Response.WriteAsync($"\"performance\":{System.Text.Json.JsonSerializer.Serialize(result.Performance)},", cancellationToken);
        
        if (result.Warnings.Any())
        {
            await Response.WriteAsync($"\"warnings\":{System.Text.Json.JsonSerializer.Serialize(result.Warnings)},", cancellationToken);
        }
        
        await Response.WriteAsync("\"data\":[", cancellationToken);

        var isFirst = true;
        var rowCount = 0;

        await foreach (var row in result.Data!.WithCancellation(cancellationToken))
        {
            if (!isFirst)
            {
                await Response.WriteAsync(",", cancellationToken);
            }

            var json = System.Text.Json.JsonSerializer.Serialize(row);
            await Response.WriteAsync(json, cancellationToken);

            isFirst = false;
            rowCount++;

            // Flush every 50 rows for better streaming performance
            if (rowCount % 50 == 0)
            {
                await Response.Body.FlushAsync(cancellationToken);
            }
        }

        await Response.WriteAsync("]", cancellationToken);

        // Add final metadata with actual row count
        var finalMetadata = new
        {
            actualRowsReturned = rowCount,
            isPartialPage = rowCount < result.Pagination.PageSize,
            nextPageUrl = result.Pagination.HasNextPage 
                ? $"{Request.Path}?pageNumber={result.Pagination.CurrentPage + 1}&pageSize={result.Pagination.PageSize}"
                : null,
            previousPageUrl = result.Pagination.HasPreviousPage 
                ? $"{Request.Path}?pageNumber={result.Pagination.CurrentPage - 1}&pageSize={result.Pagination.PageSize}"
                : null
        };

        await Response.WriteAsync($",\"metadata\":{System.Text.Json.JsonSerializer.Serialize(finalMetadata)}", cancellationToken);
        await Response.WriteAsync("}", cancellationToken);

        _logger.LogInformation("Streamed {RowCount} rows for page {PageNumber}", 
            rowCount, result.Pagination.CurrentPage);

        return new EmptyResult();
    }

    /// <summary>
    /// Validates pagination query parameters for safety and performance constraints.
    /// Checks SQL query, file path, page bounds, and prevents inefficient deep pagination.
    /// </summary>
    /// <param name="request">Pagination request to validate</param>
    /// <returns>Validation result with success status and error message if validation fails</returns>
    private (bool isValid, string errorMessage) ValidateQueryParameters(PaginatedQueryRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.SqlQuery))
        {
            return (false, "SQL query parameter is required");
        }

        if (string.IsNullOrWhiteSpace(request.FilePath))
        {
            return (false, "File path parameter is required");
        }

        if (request.PageNumber < 1)
        {
            return (false, "Page number must be greater than 0");
        }

        if (request.PageSize < 1 || request.PageSize > 10000)
        {
            return (false, "Page size must be between 1 and 10000");
        }

        if (request.TimeoutSeconds < 1 || request.TimeoutSeconds > 1800)
        {
            return (false, "Timeout must be between 1 and 1800 seconds");
        }

        // Check for deep pagination that might be inefficient
        if (request.PageNumber > 10000)
        {
            return (false, "Page number too high. Consider using filters to narrow results instead of deep pagination");
        }

        return (true, string.Empty);
    }

    /// <summary>
    /// Generates contextual error suggestions for pagination-specific issues.
    /// Provides recommendations for file access, SQL syntax, memory, and timeout problems.
    /// </summary>
    /// <param name="errorMessage">The error message to analyze</param>
    /// <returns>List of suggested solutions and troubleshooting steps</returns>
    private List<string> GetErrorSuggestions(string? errorMessage)
    {
        if (string.IsNullOrEmpty(errorMessage))
            return new List<string>();

        var suggestions = new List<string>();
        var lowerError = errorMessage.ToLower();

        if (lowerError.Contains("file not found") || lowerError.Contains("not found"))
        {
            suggestions.AddRange(new[]
            {
                "Verify the file path is correct and accessible",
                "Check file permissions",
                "Ensure the file exists on the server"
            });
        }

        if (lowerError.Contains("sql") || lowerError.Contains("syntax"))
        {
            suggestions.AddRange(new[]
            {
                "Check SQL query syntax",
                "Verify column names exist in the file",
                "Ensure proper table references (use 'data_table' or 'table')",
                "Check for proper quotation marks around string values"
            });
        }

        if (lowerError.Contains("memory") || lowerError.Contains("limit"))
        {
            suggestions.AddRange(new[]
            {
                "Reduce page size",
                "Add WHERE clauses to filter data",
                "Use column projection to select only needed columns",
                "Consider processing data in smaller chunks"
            });
        }

        if (lowerError.Contains("timeout") || lowerError.Contains("cancelled"))
        {
            suggestions.AddRange(new[]
            {
                "Increase timeout value",
                "Optimize query with better WHERE clauses",
                "Reduce page size for faster processing",
                "Consider adding indexes or pre-filtering data"
            });
        }

        if (!suggestions.Any())
        {
            suggestions.AddRange(new[]
            {
                "Check the query syntax and parameters",
                "Verify file format matches the actual file type",
                "Try with a smaller page size",
                "Check server logs for more details"
            });
        }

        return suggestions;
    }

    /// <summary>
    /// Suggests optimal page sizes based on the total number of rows in the dataset.
    /// Returns progressively larger page sizes for larger datasets to optimize performance.
    /// </summary>
    /// <param name="totalRows">Total number of rows in the dataset</param>
    /// <returns>List of recommended page sizes optimized for the dataset size</returns>
    private List<int> GetSuggestedPageSizes(long? totalRows)
    {
        if (!totalRows.HasValue)
            return new List<int> { 100, 500, 1000, 2500, 5000 };

        var suggestions = new List<int>();
        
        if (totalRows <= 100)
            suggestions.AddRange(new[] { 10, 25, 50 });
        else if (totalRows <= 1000)
            suggestions.AddRange(new[] { 50, 100, 250, 500 });
        else if (totalRows <= 10000)
            suggestions.AddRange(new[] { 100, 500, 1000, 2500 });
        else if (totalRows <= 100000)
            suggestions.AddRange(new[] { 500, 1000, 2500, 5000 });
        else
            suggestions.AddRange(new[] { 1000, 2500, 5000, 10000 });

        return suggestions;
    }
}