using Microsoft.AspNetCore.Mvc;
using ReportBuilder.Request;
using ReportBuilder.Response;
using ReportBuilder.Service;
using System.Runtime.CompilerServices;

namespace ReportBuilder.Controllers;

[ApiController]
[Route("query")]
public class AnalyticsController : ControllerBase
{
    private readonly IAnalyticsDuckDbService _analyticsService;
    private readonly IAdlsArrowStreamService _arrowStreamService;
    private readonly IConcurrencyLimiterService _concurrencyLimiter;
    private readonly ILogger<AnalyticsController> _logger;

    /// <summary>
    /// Initializes a new instance of the AnalyticsController with required dependencies
    /// </summary>
    /// <param name="analyticsService">Service for executing analytics queries against ADLS files</param>
    /// <param name="arrowStreamService">Service for streaming Arrow format files</param>
    /// <param name="concurrencyLimiter">Service for managing concurrent query execution</param>
    /// <param name="logger">Logger for recording controller activities</param>
    public AnalyticsController(
        IAnalyticsDuckDbService analyticsService,
        IAdlsArrowStreamService arrowStreamService,
        IConcurrencyLimiterService concurrencyLimiter,
        ILogger<AnalyticsController> logger)
    {
        _analyticsService = analyticsService;
        _arrowStreamService = arrowStreamService;
        _concurrencyLimiter = concurrencyLimiter;
        _logger = logger;
    }

    /// <summary>
    /// Executes analytics queries against ADLS files (Parquet/CSV) with streaming response
    /// </summary>
    /// <param name="request">Analytics query request with SQL and ADLS file details</param>
    /// <returns>Streamed query results in JSON or CSV format with performance metadata</returns>
    [HttpPost("run")]
    [RequestSizeLimit(100_000_000)] // 100MB limit for request
    public async Task<IActionResult> RunQuery([FromBody] AnalyticsQueryRequest request)
    {
        var cancellationToken = HttpContext.RequestAborted;
        
        if (!ModelState.IsValid)
        {
            return BadRequest(ModelState);
        }

        try
        {
            var validation = ValidateQueryRequest(request);
            if (!validation.isValid)
            {
                return BadRequest(new AnalyticsQueryErrorResponse
                {
                    ErrorMessage = validation.errorMessage,
                    MemoryUsage = GC.GetTotalMemory(false),
                    ValidationErrors = new List<string> { validation.errorMessage }
                });
            }

            _logger.LogInformation("Processing analytics query. SQL: {Sql}, File: {File}", 
                request.SqlQuery, request.AdlsFileUri);

            var result = await _analyticsService.ExecuteQueryAsync(request, cancellationToken);
            
            if (!result.Success)
            {
                var statusCode = DetermineErrorStatusCode(result.ErrorMessage);
                
                return StatusCode(statusCode, new AnalyticsQueryErrorResponse
                {
                    ErrorMessage = result.ErrorMessage ?? "Query execution failed",
                    QueryId = result.QueryId,
                    MemoryUsage = result.FinalMemoryUsage,
                    ShouldRetry = IsRetryableError(result.ErrorMessage),
                    Suggestions = GetErrorSuggestions(result.ErrorMessage)
                });
            }

            // Stream results based on response format
            return await StreamQueryResults(result, request.ResponseFormat, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Query was cancelled by client");
            return StatusCode(499, new AnalyticsQueryErrorResponse
            {
                ErrorMessage = "Request was cancelled",
                MemoryUsage = GC.GetTotalMemory(false)
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing analytics query");
            return StatusCode(500, new AnalyticsQueryErrorResponse
            {
                ErrorMessage = "Internal server error occurred while processing the query",
                MemoryUsage = GC.GetTotalMemory(false),
                Suggestions = new List<string> 
                { 
                    "Check query syntax and ADLS file path",
                    "Verify file format and accessibility",
                    "Consider adding filters to reduce memory usage"
                }
            });
        }
    }

    /// <summary>
    /// Executes join queries between two ADLS datasets with optimized processing
    /// </summary>
    /// <param name="request">Join request containing SQL and references to left/right datasets</param>
    /// <returns>Streamed join results with execution statistics</returns>
    [HttpPost("join")]
    [RequestSizeLimit(200_000_000)] // 200MB limit for join requests
    public async Task<IActionResult> ExecuteJoin([FromBody] AnalyticsJoinRequest request)
    {
        var cancellationToken = HttpContext.RequestAborted;
        
        if (!ModelState.IsValid)
        {
            return BadRequest(ModelState);
        }

        try
        {
            var validation = ValidateJoinRequest(request);
            if (!validation.isValid)
            {
                return BadRequest(new AnalyticsQueryErrorResponse
                {
                    ErrorMessage = validation.errorMessage,
                    MemoryUsage = GC.GetTotalMemory(false)
                });
            }

            _logger.LogInformation("Processing join query. Left: {LeftFile}, Right: {RightFile}", 
                request.LeftDataset.AdlsFileUri, request.RightDataset.AdlsFileUri);

            var result = await _analyticsService.ExecuteJoinQueryAsync(request, cancellationToken);
            
            if (!result.Success)
            {
                var statusCode = DetermineErrorStatusCode(result.ErrorMessage);
                
                return StatusCode(statusCode, new AnalyticsQueryErrorResponse
                {
                    ErrorMessage = result.ErrorMessage ?? "Join query execution failed",
                    QueryId = result.QueryId,
                    MemoryUsage = result.FinalMemoryUsage,
                    ShouldRetry = IsRetryableError(result.ErrorMessage)
                });
            }

            return await StreamQueryResults(result, ResponseFormat.Json, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing join query");
            return StatusCode(500, new AnalyticsQueryErrorResponse
            {
                ErrorMessage = "Internal server error occurred while processing the join query",
                MemoryUsage = GC.GetTotalMemory(false)
            });
        }
    }

    /// <summary>
    /// Streams Arrow format files from ADLS with efficient chunk-based processing
    /// </summary>
    /// <param name="request">Arrow stream request with file details and query parameters</param>
    /// <returns>Streamed Arrow data with schema information and processing statistics</returns>
    [HttpPost("arrow-stream")]
    [RequestSizeLimit(50_000_000)] // 50MB limit for Arrow stream requests
    public async Task<IActionResult> ArrowStream([FromBody] AdlsArrowStreamRequest request)
    {
        var cancellationToken = HttpContext.RequestAborted;
        
        if (!ModelState.IsValid)
        {
            return BadRequest(ModelState);
        }

        try
        {
            var validation = ValidateArrowStreamRequest(request);
            if (!validation.isValid)
            {
                return BadRequest(new AnalyticsQueryErrorResponse
                {
                    ErrorMessage = validation.errorMessage,
                    MemoryUsage = GC.GetTotalMemory(false)
                });
            }

            _logger.LogInformation("Processing Arrow stream query. File: {File}, SQL: {Sql}", 
                request.AdlsFileUri, request.SqlQuery);

            var result = await _arrowStreamService.StreamArrowFileAsync(request, cancellationToken);
            
            if (!result.Success)
            {
                return BadRequest(new AnalyticsQueryErrorResponse
                {
                    ErrorMessage = result.ErrorMessage ?? "Arrow stream processing failed",
                    QueryId = result.QueryId,
                    MemoryUsage = result.FinalMemoryUsage
                });
            }

            return await StreamArrowResults(result, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing Arrow stream");
            return StatusCode(500, new AnalyticsQueryErrorResponse
            {
                ErrorMessage = "Internal server error occurred while processing the Arrow stream",
                MemoryUsage = GC.GetTotalMemory(false)
            });
        }
    }

    /// <summary>
    /// Retrieves current system status including active queries and resource utilization
    /// </summary>
    /// <param name="request">Status request parameters</param>
    /// <returns>System status information including query statistics and health metrics</returns>
    [HttpGet("status")]
    public async Task<IActionResult> GetQueryStatus([FromQuery] QueryStatusRequest request)
    {
        try
        {
            var activeQueries = _concurrencyLimiter.GetActiveQueries();
            var systemStats = _concurrencyLimiter.GetSystemStats();
            var isHealthy = await _concurrencyLimiter.IsSystemHealthyAsync();

            var response = new QueryStatusResponse
            {
                Success = true,
                ActiveQueries = activeQueries,
                SystemStats = systemStats
            };

            if (!isHealthy)
            {
                Response.StatusCode = 503; // Service Unavailable
            }

            return Ok(response);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting query status");
            return StatusCode(500, new QueryStatusResponse
            {
                Success = false,
                ErrorMessage = "Failed to get system status"
            });
        }
    }

    /// <summary>
    /// Retrieves schema information for ADLS files (Parquet/CSV/Arrow)
    /// </summary>
    /// <param name="request">Schema request with file details and format information</param>
    /// <returns>Schema details including column names, types, and metadata</returns>
    [HttpPost("schema")]
    public async Task<IActionResult> GetSchema([FromBody] SchemaRequest request)
    {
        var cancellationToken = HttpContext.RequestAborted;
        
        if (!ModelState.IsValid)
        {
            return BadRequest(ModelState);
        }

        try
        {
            if (string.IsNullOrEmpty(request.AdlsFileUri))
            {
                return BadRequest(new { error = "AdlsFileUri is required" });
            }

            _logger.LogInformation("Getting schema for file: {File}", request.AdlsFileUri);

            SchemaResponse result;
            
            if (request.FileFormat == FileFormat.Arrow)
            {
                result = await _arrowStreamService.GetArrowSchemaAsync(request.AdlsFileUri, request.SasToken, cancellationToken);
            }
            else
            {
                result = await _analyticsService.GetSchemaAsync(request, cancellationToken);
            }
            
            if (!result.Success)
            {
                return BadRequest(new { error = result.ErrorMessage });
            }

            return Ok(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting schema");
            return StatusCode(500, new { error = "Failed to get schema" });
        }
    }

    /// <summary>
    /// Routes query results to appropriate streaming method based on response format
    /// </summary>
    /// <param name="result">Analytics query response containing data to stream</param>
    /// <param name="format">Desired response format (JSON or CSV)</param>
    /// <param name="cancellationToken">Token to cancel the streaming operation</param>
    /// <returns>Streamed response in the requested format</returns>
    private async Task<IActionResult> StreamQueryResults(AnalyticsQueryResponse result, ResponseFormat format, CancellationToken cancellationToken)
    {
        switch (format)
        {
            case ResponseFormat.Json:
                return await StreamJsonResults(result, cancellationToken);
            
            case ResponseFormat.Csv:
                return await StreamCsvResults(result, cancellationToken);
            
            default:
                return await StreamJsonResults(result, cancellationToken);
        }
    }

    /// <summary>
    /// Streams query results in JSON format with metadata
    /// </summary>
    /// <param name="result">Query response containing data to stream</param>
    /// <param name="cancellationToken">Token to cancel the streaming operation</param>
    /// <returns>JSON streaming response with performance and memory statistics</returns>
    private async Task<IActionResult> StreamJsonResults(AnalyticsQueryResponse result, CancellationToken cancellationToken)
    {
        Response.ContentType = "application/json";
        await Response.WriteAsync("{\"success\":true,\"data\":[", cancellationToken);
        
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
        
        var metadata = new
        {
            queryId = result.QueryId,
            totalRows = rowCount,
            columns = result.Columns,
            queryExecuted = result.QueryExecuted,
            performance = result.Performance,
            memoryStats = new
            {
                initialMemoryMB = result.InitialMemoryUsage / (1024 * 1024),
                finalMemoryMB = result.FinalMemoryUsage / (1024 * 1024),
                peakMemoryMB = result.PeakMemoryUsage / (1024 * 1024)
            },
            fileStats = new
            {
                filePath = result.FilePath,
                fileSizeKB = result.FileSize / 1024
            },
            warnings = result.Warnings,
            duration = result.Duration?.TotalMilliseconds
        };
        
        await Response.WriteAsync($"],\"metadata\":{System.Text.Json.JsonSerializer.Serialize(metadata)}}}", cancellationToken);
        
        _logger.LogInformation("Query {QueryId} streaming completed. Rows: {RowCount}", result.QueryId, rowCount);
        
        return new EmptyResult();
    }

    /// <summary>
    /// Streams query results in CSV format for download
    /// </summary>
    /// <param name="result">Query response containing data to stream</param>
    /// <param name="cancellationToken">Token to cancel the streaming operation</param>
    /// <returns>CSV file download response</returns>
    private async Task<IActionResult> StreamCsvResults(AnalyticsQueryResponse result, CancellationToken cancellationToken)
    {
        Response.ContentType = "text/csv";
        Response.Headers["Content-Disposition"] = $"attachment; filename=\"query_result_{result.QueryId}.csv\"";
        
        var headerWritten = false;
        var rowCount = 0;
        
        await foreach (var row in result.Data!.WithCancellation(cancellationToken))
        {
            if (!headerWritten)
            {
                var headers = string.Join(",", row.Keys.Select(k => $"\"{k}\""));
                await Response.WriteAsync(headers + "\n", cancellationToken);
                headerWritten = true;
            }
            
            var values = string.Join(",", row.Values.Select(v => $"\"{v?.ToString()?.Replace("\"", "\"\"")}\""));
            await Response.WriteAsync(values + "\n", cancellationToken);
            
            rowCount++;
            
            if (rowCount % 100 == 0)
            {
                await Response.Body.FlushAsync(cancellationToken);
            }
        }
        
        return new EmptyResult();
    }

    /// <summary>
    /// Streams Arrow format results with chunk processing statistics
    /// </summary>
    /// <param name="result">Arrow stream response containing data and metadata</param>
    /// <param name="cancellationToken">Token to cancel the streaming operation</param>
    /// <returns>JSON response with Arrow data and processing statistics</returns>
    private async Task<IActionResult> StreamArrowResults(ArrowStreamResponse result, CancellationToken cancellationToken)
    {
        Response.ContentType = "application/json";
        await Response.WriteAsync("{\"success\":true,\"data\":[", cancellationToken);
        
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
            
            if (rowCount % 50 == 0)
            {
                await Response.Body.FlushAsync(cancellationToken);
            }
        }
        
        var metadata = new
        {
            queryId = result.QueryId,
            totalRows = rowCount,
            chunksProcessed = result.ChunksProcessed,
            totalBytesRead = result.TotalBytesRead,
            schemaInfo = result.SchemaInfo,
            memoryStats = new
            {
                initialMemoryMB = result.InitialMemoryUsage / (1024 * 1024),
                finalMemoryMB = result.FinalMemoryUsage / (1024 * 1024),
                peakMemoryMB = result.PeakMemoryUsage / (1024 * 1024)
            },
            warnings = result.Warnings,
            duration = result.Duration?.TotalMilliseconds
        };
        
        await Response.WriteAsync($"],\"metadata\":{System.Text.Json.JsonSerializer.Serialize(metadata)}}}", cancellationToken);
        
        _logger.LogInformation("Arrow stream {QueryId} completed. Rows: {RowCount}, Chunks: {Chunks}", 
            result.QueryId, rowCount, result.ChunksProcessed);
        
        return new EmptyResult();
    }

    /// <summary>
    /// Validates analytics query request parameters and applies defaults
    /// </summary>
    /// <param name="request">Query request to validate</param>
    /// <returns>Tuple indicating validation result and error message if invalid</returns>
    private (bool isValid, string errorMessage) ValidateQueryRequest(AnalyticsQueryRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.SqlQuery))
        {
            return (false, "SQL query is required");
        }

        if (string.IsNullOrWhiteSpace(request.AdlsFileUri))
        {
            return (false, "ADLS file URI is required");
        }

        if (!Uri.TryCreate(request.AdlsFileUri, UriKind.Absolute, out _))
        {
            return (false, "Invalid ADLS file URI format");
        }

        if (request.PageSize <= 0 || request.PageSize > 10000)
        {
            request.PageSize = 1000;
        }

        if (request.TimeoutSeconds <= 0 || request.TimeoutSeconds > 1800) // Max 30 minutes
        {
            request.TimeoutSeconds = 300;
        }

        return (true, string.Empty);
    }

    /// <summary>
    /// Validates join query request parameters for both left and right datasets
    /// </summary>
    /// <param name="request">Join request to validate</param>
    /// <returns>Tuple indicating validation result and error message if invalid</returns>
    private (bool isValid, string errorMessage) ValidateJoinRequest(AnalyticsJoinRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.SqlQuery))
        {
            return (false, "SQL query is required");
        }

        if (string.IsNullOrWhiteSpace(request.LeftDataset.AdlsFileUri))
        {
            return (false, "Left dataset ADLS URI is required");
        }

        if (string.IsNullOrWhiteSpace(request.RightDataset.AdlsFileUri))
        {
            return (false, "Right dataset ADLS URI is required");
        }

        return (true, string.Empty);
    }

    /// <summary>
    /// Validates Arrow stream request parameters and applies chunk size defaults
    /// </summary>
    /// <param name="request">Arrow stream request to validate</param>
    /// <returns>Tuple indicating validation result and error message if invalid</returns>
    private (bool isValid, string errorMessage) ValidateArrowStreamRequest(AdlsArrowStreamRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.AdlsFileUri))
        {
            return (false, "ADLS file URI is required");
        }

        if (string.IsNullOrWhiteSpace(request.SqlQuery))
        {
            return (false, "SQL query is required");
        }

        if (request.ChunkSize < 1024 || request.ChunkSize > 1048576) // 1KB to 1MB
        {
            request.ChunkSize = 65536; // Default 64KB
        }

        return (true, string.Empty);
    }

    /// <summary>
    /// Determines appropriate HTTP status code based on error message content
    /// </summary>
    /// <param name="errorMessage">Error message to analyze</param>
    /// <returns>HTTP status code that best represents the error type</returns>
    private int DetermineErrorStatusCode(string? errorMessage)
    {
        if (string.IsNullOrEmpty(errorMessage))
            return 500;

        var lowerError = errorMessage.ToLower();
        
        if (lowerError.Contains("memory") || lowerError.Contains("limit exceeded"))
            return 413; // Payload Too Large
        
        if (lowerError.Contains("concurrent") || lowerError.Contains("slot"))
            return 429; // Too Many Requests
        
        if (lowerError.Contains("not found") || lowerError.Contains("inaccessible"))
            return 404; // Not Found
        
        if (lowerError.Contains("unauthorized") || lowerError.Contains("access"))
            return 401; // Unauthorized
        
        if (lowerError.Contains("timeout") || lowerError.Contains("cancelled"))
            return 408; // Request Timeout
        
        return 400; // Bad Request
    }

    /// <summary>
    /// Determines if an error is transient and the request should be retried
    /// </summary>
    /// <param name="errorMessage">Error message to analyze</param>
    /// <returns>True if the error is likely transient and retryable</returns>
    private bool IsRetryableError(string? errorMessage)
    {
        if (string.IsNullOrEmpty(errorMessage))
            return false;

        var lowerError = errorMessage.ToLower();
        return lowerError.Contains("timeout") || 
               lowerError.Contains("concurrent") || 
               lowerError.Contains("503") || 
               lowerError.Contains("500");
    }

    /// <summary>
    /// Provides actionable suggestions based on error message content
    /// </summary>
    /// <param name="errorMessage">Error message to analyze</param>
    /// <returns>List of suggestions to help resolve the error</returns>
    private List<string> GetErrorSuggestions(string? errorMessage)
    {
        if (string.IsNullOrEmpty(errorMessage))
            return new List<string>();

        var suggestions = new List<string>();
        var lowerError = errorMessage.ToLower();

        if (lowerError.Contains("memory"))
        {
            suggestions.AddRange(new[]
            {
                "Add filters to reduce data size",
                "Use column projection to select only needed columns",
                "Consider breaking query into smaller chunks",
                "Enable memory optimization in request"
            });
        }

        if (lowerError.Contains("concurrent"))
        {
            suggestions.AddRange(new[]
            {
                "Wait a moment and retry",
                "Check /query/status for current load",
                "Consider scheduling queries during off-peak hours"
            });
        }

        if (lowerError.Contains("not found") || lowerError.Contains("inaccessible"))
        {
            suggestions.AddRange(new[]
            {
                "Verify the ADLS file path is correct",
                "Check SAS token validity and permissions",
                "Ensure the file exists and is accessible"
            });
        }

        return suggestions;
    }
}