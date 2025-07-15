using System.Net;
using System.Text.Json;
using ReportBuilder.Response;

namespace ReportBuilder.Infrastructure.Middleware;

/// <summary>
/// Global exception handling middleware that provides comprehensive error handling with contextual responses.
/// Converts various exception types into structured API responses with actionable suggestions.
/// </summary>
public class GlobalExceptionMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<GlobalExceptionMiddleware> _logger;

    /// <summary>
    /// Initializes a new instance of the GlobalExceptionMiddleware.
    /// </summary>
    /// <param name="next">The next middleware delegate in the pipeline</param>
    /// <param name="logger">Logger for recording exception details and system metrics</param>
    public GlobalExceptionMiddleware(RequestDelegate next, ILogger<GlobalExceptionMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }

    /// <summary>
    /// Processes HTTP requests with comprehensive exception handling and structured error responses.
    /// Catches all unhandled exceptions and converts them to appropriate HTTP responses with context.
    /// </summary>
    /// <param name="context">The HTTP context for the current request</param>
    /// <returns>Task representing the asynchronous middleware execution</returns>
    public async Task InvokeAsync(HttpContext context)
    {
        try
        {
            await _next(context);
        }
        catch (Exception ex)
        {
            await HandleExceptionAsync(context, ex);
        }
    }

    /// <summary>
    /// Handles caught exceptions by creating structured error responses with logging and metrics.
    /// Records exception details, memory usage, and request context for monitoring.
    /// </summary>
    /// <param name="context">HTTP context for the failed request</param>
    /// <param name="exception">The exception that was caught</param>
    /// <returns>Task representing the asynchronous exception handling operation</returns>
    private async Task HandleExceptionAsync(HttpContext context, Exception exception)
    {
        var requestId = context.TraceIdentifier;
        var endpoint = $"{context.Request.Method} {context.Request.Path}";
        var currentMemory = GC.GetTotalMemory(false);

        _logger.LogError(exception, 
            "Unhandled exception in request {RequestId} to {Endpoint}. Memory: {MemoryMB} MB",
            requestId, endpoint, currentMemory / (1024 * 1024));

        var response = context.Response;
        response.ContentType = "application/json";

        var errorResponse = CreateErrorResponse(exception, requestId, currentMemory);
        response.StatusCode = (int)errorResponse.StatusCode;

        var jsonResponse = JsonSerializer.Serialize(errorResponse.Response, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });

        await response.WriteAsync(jsonResponse);
    }

    /// <summary>
    /// Creates contextual error responses based on exception type with appropriate HTTP status codes.
    /// Provides specific error messages and actionable suggestions for different exception scenarios.
    /// </summary>
    /// <param name="exception">The exception to convert to an error response</param>
    /// <param name="requestId">Unique identifier for the failed request</param>
    /// <param name="currentMemory">Current system memory usage in bytes</param>
    /// <returns>Tuple containing HTTP status code and structured error response object</returns>
    private (HttpStatusCode StatusCode, object Response) CreateErrorResponse(Exception exception, string requestId, long currentMemory)
    {
        return exception switch
        {
            OperationCanceledException => (
                HttpStatusCode.RequestTimeout,
                new AnalyticsQueryErrorResponse
                {
                    ErrorMessage = "Request was cancelled or timed out",
                    QueryId = requestId,
                    MemoryUsage = currentMemory,
                    ErrorType = "OperationCancelled",
                    ShouldRetry = true,
                    Suggestions = new List<string> { "Try again with a shorter timeout", "Consider breaking the query into smaller parts" }
                }
            ),

            OutOfMemoryException => (
                HttpStatusCode.InsufficientStorage,
                new AnalyticsQueryErrorResponse
                {
                    ErrorMessage = "System ran out of memory processing the request",
                    QueryId = requestId,
                    MemoryUsage = currentMemory,
                    ErrorType = "OutOfMemory",
                    ShouldRetry = false,
                    Suggestions = new List<string> 
                    { 
                        "Add filters to reduce data size",
                        "Use column projection to select only needed columns",
                        "Consider processing data in smaller chunks",
                        "Enable memory optimization in request"
                    }
                }
            ),

            InvalidOperationException when exception.Message.Contains("Memory limit exceeded") => (
                HttpStatusCode.RequestEntityTooLarge,
                new AnalyticsQueryErrorResponse
                {
                    ErrorMessage = exception.Message,
                    QueryId = requestId,
                    MemoryUsage = currentMemory,
                    ErrorType = "MemoryLimitExceeded",
                    ShouldRetry = false,
                    Suggestions = new List<string>
                    {
                        "Reduce query complexity",
                        "Add WHERE clauses to filter data",
                        "Use column projection",
                        "Consider using a smaller LIMIT"
                    }
                }
            ),

            TimeoutException => (
                HttpStatusCode.RequestTimeout,
                new AnalyticsQueryErrorResponse
                {
                    ErrorMessage = "Request processing timed out",
                    QueryId = requestId,
                    MemoryUsage = currentMemory,
                    ErrorType = "Timeout",
                    ShouldRetry = true,
                    Suggestions = new List<string> 
                    { 
                        "Increase timeout value",
                        "Optimize query with filters and projections",
                        "Break query into smaller parts"
                    }
                }
            ),

            UnauthorizedAccessException => (
                HttpStatusCode.Unauthorized,
                new AnalyticsQueryErrorResponse
                {
                    ErrorMessage = "Access denied to the requested resource",
                    QueryId = requestId,
                    MemoryUsage = currentMemory,
                    ErrorType = "Unauthorized",
                    ShouldRetry = false,
                    Suggestions = new List<string>
                    {
                        "Check SAS token validity and permissions",
                        "Verify ADLS access credentials",
                        "Ensure file path is correct"
                    }
                }
            ),

            FileNotFoundException => (
                HttpStatusCode.NotFound,
                new AnalyticsQueryErrorResponse
                {
                    ErrorMessage = "The requested file was not found",
                    QueryId = requestId,
                    MemoryUsage = currentMemory,
                    ErrorType = "FileNotFound",
                    ShouldRetry = false,
                    Suggestions = new List<string>
                    {
                        "Verify the ADLS file path is correct",
                        "Check if the file exists in the storage account",
                        "Ensure proper file permissions"
                    }
                }
            ),

            ArgumentException when exception.Message.Contains("SQL") => (
                HttpStatusCode.BadRequest,
                new AnalyticsQueryErrorResponse
                {
                    ErrorMessage = $"Invalid SQL query: {exception.Message}",
                    QueryId = requestId,
                    MemoryUsage = currentMemory,
                    ErrorType = "InvalidSQL",
                    ShouldRetry = false,
                    Suggestions = new List<string>
                    {
                        "Check SQL syntax",
                        "Verify column names exist",
                        "Ensure proper JOIN conditions",
                        "Validate WHERE clause syntax"
                    }
                }
            ),

            ArgumentException => (
                HttpStatusCode.BadRequest,
                new AnalyticsQueryErrorResponse
                {
                    ErrorMessage = $"Invalid request: {exception.Message}",
                    QueryId = requestId,
                    MemoryUsage = currentMemory,
                    ErrorType = "InvalidArgument",
                    ShouldRetry = false,
                    Suggestions = new List<string>
                    {
                        "Check request parameters",
                        "Verify file paths and URIs",
                        "Validate input data types"
                    }
                }
            ),

            NotSupportedException => (
                HttpStatusCode.NotImplemented,
                new AnalyticsQueryErrorResponse
                {
                    ErrorMessage = $"Operation not supported: {exception.Message}",
                    QueryId = requestId,
                    MemoryUsage = currentMemory,
                    ErrorType = "NotSupported",
                    ShouldRetry = false,
                    Suggestions = new List<string>
                    {
                        "Check supported file formats (CSV, Parquet, Arrow)",
                        "Verify SQL operations are supported",
                        "Review API documentation for limitations"
                    }
                }
            ),

            _ => (
                HttpStatusCode.InternalServerError,
                new AnalyticsQueryErrorResponse
                {
                    ErrorMessage = "An unexpected error occurred while processing the request",
                    QueryId = requestId,
                    MemoryUsage = currentMemory,
                    ErrorType = "InternalServerError",
                    ShouldRetry = true,
                    Suggestions = new List<string>
                    {
                        "Try the request again",
                        "Check system status at /query/status",
                        "Contact support if the issue persists"
                    }
                }
            )
        };
    }
}