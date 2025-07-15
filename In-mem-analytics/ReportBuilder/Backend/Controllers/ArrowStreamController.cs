using Microsoft.AspNetCore.Mvc;
using ReportBuilder.Request;
using ReportBuilder.Response;
using ReportBuilder.Service;
using System.Text.Json;

namespace ReportBuilder.Controllers;

/// <summary>
/// Controller for handling Apache Arrow streaming operations with high-performance data streaming capabilities.
/// Provides endpoints for executing queries and streaming results in Arrow IPC format.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public class ArrowStreamController : ControllerBase
{
    private readonly IArrowStreamService _arrowStreamService;
    private readonly ILogger<ArrowStreamController> _logger;

    /// <summary>
    /// Initializes a new instance of the ArrowStreamController.
    /// </summary>
    /// <param name="arrowStreamService">Service for handling Arrow streaming operations</param>
    /// <param name="logger">Logger for recording controller operations and performance metrics</param>
    public ArrowStreamController(
        IArrowStreamService arrowStreamService,
        ILogger<ArrowStreamController> logger)
    {
        _arrowStreamService = arrowStreamService;
        _logger = logger;
    }

    /// <summary>
    /// Executes a SQL query and streams the results in Apache Arrow IPC format.
    /// Supports cancellation, timeout handling, and performance tracking.
    /// </summary>
    /// <param name="request">Arrow stream request containing SQL query, file path, and configuration options</param>
    /// <returns>Arrow IPC stream with query results, or error response if execution fails</returns>
    [HttpPost("execute")]
    public async Task<IActionResult> ExecuteArrowQuery([FromBody] ArrowStreamRequest request)
    {
        var cancellationToken = HttpContext.RequestAborted;
        var startTime = DateTime.UtcNow;

        try
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(new ArrowStreamErrorResponse
                {
                    ErrorMessage = "Invalid request model",
                    ValidationErrors = ModelState.Values
                        .SelectMany(v => v.Errors)
                        .Select(e => e.ErrorMessage)
                        .ToList()
                });
            }

            _logger.LogInformation("Starting Arrow stream query for file: {FilePath}", request.FilePath);

            // Validate and prepare query
            var validation = await _arrowStreamService.ValidateAndPrepareQueryAsync(request, cancellationToken);
            if (!validation.Success)
            {
                return BadRequest(new ArrowStreamErrorResponse
                {
                    ErrorMessage = validation.ErrorMessage ?? "Query validation failed",
                    ValidationErrors = new List<string> { validation.ErrorMessage ?? "Unknown validation error" },
                    Suggestions = GetErrorSuggestions(validation.ErrorMessage)
                });
            }

            // Set up response for Arrow streaming
            SetupArrowStreamResponse(request, validation.Metadata);

            // Set up timeout
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(request.TimeoutSeconds));

            // Track performance
            ArrowStreamPerformance? performance = null;
            var performanceCallback = new Action<ArrowStreamPerformance>(perf => performance = perf);

            // Stream Arrow data directly to response
            await _arrowStreamService.StreamArrowDataAsync(
                request, 
                Response.Body, 
                performanceCallback, 
                timeoutCts.Token);

            // Log completion
            var endTime = DateTime.UtcNow;
            var totalDuration = endTime - startTime;

            _logger.LogInformation("Arrow stream completed successfully. " +
                "Duration: {Duration}, Bytes: {BytesMB} MB, Rate: {RateMBps:F2} MB/s",
                totalDuration,
                performance?.BytesStreamed / (1024 * 1024) ?? 0,
                performance?.StreamingRateMBps ?? 0);

            return new EmptyResult();
        }
        catch (TaskCanceledException ex) when (ex.Message.Contains("[CANCELLED]"))
        {
            _logger.LogInformation("[CANCELLED] Arrow stream query was cancelled: {Message}", ex.Message);
            return StatusCode(499, new ArrowStreamErrorResponse
            {
                ErrorMessage = ex.Message,
                ErrorType = "ClientClosedRequest"
            });
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("[CANCELLED] Arrow stream query was cancelled by client");
            return StatusCode(499, new ArrowStreamErrorResponse
            {
                ErrorMessage = "[CANCELLED] Query aborted by client",
                ErrorType = "ClientClosedRequest"
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing Arrow stream query");
            return StatusCode(500, new ArrowStreamErrorResponse
            {
                ErrorMessage = "Internal server error occurred while executing the query",
                ErrorType = "InternalError",
                Suggestions = new List<string>
                {
                    "Check SQL syntax and file path",
                    "Verify file format is supported",
                    "Check server logs for details",
                    "Try reducing query complexity or adding filters"
                }
            });
        }
    }

    /// <summary>
    /// Validates a SQL query and returns metadata without executing the full query.
    /// Provides estimated result size, column information, and query recommendations.
    /// </summary>
    /// <param name="request">Arrow stream request to validate</param>
    /// <returns>Validation result with metadata, size estimates, and recommendations</returns>
    [HttpPost("validate")]
    public async Task<IActionResult> ValidateArrowQuery([FromBody] ArrowStreamRequest request)
    {
        var cancellationToken = HttpContext.RequestAborted;

        try
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(new ArrowStreamErrorResponse
                {
                    ErrorMessage = "Invalid request model",
                    ValidationErrors = ModelState.Values
                        .SelectMany(v => v.Errors)
                        .Select(e => e.ErrorMessage)
                        .ToList()
                });
            }

            _logger.LogInformation("Validating Arrow stream query for file: {FilePath}", request.FilePath);

            // Validate and get metadata without streaming
            var validation = await _arrowStreamService.ValidateAndPrepareQueryAsync(request, cancellationToken);
            
            if (!validation.Success)
            {
                return BadRequest(new ArrowStreamErrorResponse
                {
                    ErrorMessage = validation.ErrorMessage ?? "Query validation failed",
                    ValidationErrors = new List<string> { validation.ErrorMessage ?? "Unknown validation error" },
                    Suggestions = GetErrorSuggestions(validation.ErrorMessage)
                });
            }

            // Return metadata about the query
            var response = new
            {
                success = true,
                metadata = validation.Metadata,
                estimatedSizeInfo = new
                {
                    fileSize = validation.Metadata?.FileSize ?? 0,
                    estimatedRows = validation.Metadata?.EstimatedRows ?? 0,
                    columns = validation.Metadata?.Columns?.Count ?? 0,
                    estimatedArrowSizeKB = EstimateArrowSize(validation.Metadata)
                },
                queryInfo = new
                {
                    originalQuery = validation.Metadata?.OriginalQuery,
                    executedQuery = validation.Metadata?.ExecutedQuery,
                    fileFormat = validation.Metadata?.FileFormat,
                    filePath = validation.Metadata?.FilePath
                },
                recommendations = GetQueryRecommendations(validation.Metadata)
            };

            return Ok(response);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error validating Arrow stream query");
            return StatusCode(500, new ArrowStreamErrorResponse
            {
                ErrorMessage = "Internal server error occurred while validating the query",
                ErrorType = "InternalError"
            });
        }
    }

    /// <summary>
    /// Returns information about supported input and output formats for Arrow streaming.
    /// Includes format descriptions, file extensions, and usage examples.
    /// </summary>
    /// <returns>Supported formats information and example queries</returns>
    [HttpGet("formats")]
    public IActionResult GetSupportedFormats()
    {
        var formats = new
        {
            supportedInputFormats = new[]
            {
                new { name = "CSV", extension = ".csv", description = "Comma-separated values with auto-detection" },
                new { name = "Parquet", extension = ".parquet", description = "Columnar storage format" },
                new { name = "Arrow", extension = ".arrow", description = "Arrow IPC format (treated as Parquet)" }
            },
            outputFormat = new
            {
                name = "Arrow IPC",
                mimeType = "application/vnd.apache.arrow.stream",
                description = "Apache Arrow IPC streaming format"
            },
            examples = new
            {
                csvQuery = "SELECT * FROM data_table WHERE amount > 100",
                parquetQuery = "SELECT customer_id, SUM(amount) FROM data_table GROUP BY customer_id",
                arrowQuery = "SELECT * FROM data_table ORDER BY timestamp DESC LIMIT 10000"
            }
        };

        return Ok(formats);
    }

    /// <summary>
    /// Configures HTTP response headers for Arrow IPC streaming.
    /// Sets content type, Arrow-specific headers, and performance optimization headers.
    /// </summary>
    /// <param name="request">The Arrow stream request being processed</param>
    /// <param name="metadata">Query metadata containing column and size information</param>
    private void SetupArrowStreamResponse(ArrowStreamRequest request, ArrowStreamMetadata? metadata)
    {
        // Set Arrow IPC content type
        Response.ContentType = "application/vnd.apache.arrow.stream";
        
        // Add Arrow-specific headers
        Response.Headers["X-Arrow-Schema-Version"] = "1.0";
        Response.Headers["X-Content-Format"] = "arrow-ipc";
        
        // Add metadata headers if available
        if (metadata != null)
        {
            Response.Headers["X-Query-Columns"] = metadata.Columns.Count.ToString();
            Response.Headers["X-Estimated-Rows"] = metadata.EstimatedRows.ToString();
            Response.Headers["X-File-Format"] = metadata.FileFormat;
            Response.Headers["X-File-Size"] = metadata.FileSize.ToString();
        }

        // Add performance headers
        Response.Headers["X-Stream-Start-Time"] = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
        
        // Disable buffering for streaming
        Response.Headers["Cache-Control"] = "no-cache";
        Response.Headers["X-Accel-Buffering"] = "no";
        
        // Add CORS headers if needed
        Response.Headers["Access-Control-Allow-Origin"] = "*";
        Response.Headers["Access-Control-Expose-Headers"] = "X-Arrow-Schema-Version,X-Content-Format,X-Query-Columns,X-Estimated-Rows,X-File-Format,X-File-Size,X-Stream-Start-Time";
    }

    /// <summary>
    /// Generates contextual error suggestions based on the error message.
    /// Provides specific recommendations for common error scenarios.
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
                "Ensure the file exists on the server",
                "Use absolute file paths"
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
                "Add LIMIT clause to reduce result set size",
                "Add WHERE clauses to filter data",
                "Use column projection to select only needed columns",
                "Consider processing data in smaller chunks"
            });
        }

        if (lowerError.Contains("format") || lowerError.Contains("parse"))
        {
            suggestions.AddRange(new[]
            {
                "Verify file format matches the actual file type",
                "Try using 'Auto' format for automatic detection",
                "Check if file is corrupted or incomplete",
                "Ensure file encoding is compatible"
            });
        }

        if (!suggestions.Any())
        {
            suggestions.AddRange(new[]
            {
                "Check the query syntax and parameters",
                "Verify file format matches the actual file type",
                "Try with a smaller result set using LIMIT",
                "Check server logs for more details"
            });
        }

        return suggestions;
    }

    /// <summary>
    /// Estimates the size of the Arrow stream output in kilobytes.
    /// Uses rough approximation based on row count and column count.
    /// </summary>
    /// <param name="metadata">Query metadata containing row and column estimates</param>
    /// <returns>Estimated Arrow stream size in kilobytes</returns>
    private long EstimateArrowSize(ArrowStreamMetadata? metadata)
    {
        if (metadata == null || metadata.EstimatedRows <= 0)
            return 0;

        // Very rough estimate: average 20 bytes per column per row for Arrow format
        var avgBytesPerColumn = 20;
        var estimatedBytes = metadata.EstimatedRows * metadata.Columns.Count * avgBytesPerColumn;
        return estimatedBytes / 1024; // Return in KB
    }

    /// <summary>
    /// Generates performance recommendations based on query metadata.
    /// Suggests optimizations for large result sets, file sizes, and query patterns.
    /// </summary>
    /// <param name="metadata">Query metadata to analyze for recommendations</param>
    /// <returns>List of performance optimization recommendations</returns>
    private List<string> GetQueryRecommendations(ArrowStreamMetadata? metadata)
    {
        var recommendations = new List<string>();

        if (metadata == null)
            return recommendations;

        if (metadata.EstimatedRows > 1000000)
        {
            recommendations.Add("Consider adding LIMIT clause for large result sets");
            recommendations.Add("Use WHERE clauses to filter data");
        }

        if (metadata.Columns.Count > 50)
        {
            recommendations.Add("Consider using column projection to select only needed columns");
        }

        if (metadata.FileSize > 100 * 1024 * 1024) // 100MB
        {
            recommendations.Add("Large file detected - consider using filters for better performance");
        }

        if (!metadata.OriginalQuery.ToLower().Contains("where"))
        {
            recommendations.Add("Adding WHERE clauses can improve performance");
        }

        if (!metadata.OriginalQuery.ToLower().Contains("order by"))
        {
            recommendations.Add("Adding ORDER BY can make results more predictable");
        }

        return recommendations;
    }
}