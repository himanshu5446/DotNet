using ReportBuilder.Response;
using ReportBuilder.Service;
using System.Text.Json;

namespace ReportBuilder.Infrastructure.Middleware;

/// <summary>
/// Middleware for managing concurrent request throttling and system health monitoring.
/// Protects analytics endpoints from overload by enforcing concurrency limits and monitoring system resources.
/// </summary>
public class ConcurrencyThrottleMiddleware
{
    private readonly RequestDelegate _next;
    private readonly IConcurrencyLimiterService _concurrencyLimiter;
    private readonly ILogger<ConcurrencyThrottleMiddleware> _logger;

    /// <summary>
    /// Initializes a new instance of the ConcurrencyThrottleMiddleware.
    /// </summary>
    /// <param name="next">The next middleware delegate in the pipeline</param>
    /// <param name="concurrencyLimiter">Service for managing concurrency limits and system health</param>
    /// <param name="logger">Logger for recording throttling events and system metrics</param>
    public ConcurrencyThrottleMiddleware(
        RequestDelegate next, 
        IConcurrencyLimiterService concurrencyLimiter,
        ILogger<ConcurrencyThrottleMiddleware> logger)
    {
        _next = next;
        _concurrencyLimiter = concurrencyLimiter;
        _logger = logger;
    }

    /// <summary>
    /// Processes HTTP requests with concurrency throttling and health checks for analytics endpoints.
    /// Monitors system health, enforces concurrency limits, and provides intelligent retry suggestions.
    /// </summary>
    /// <param name="context">The HTTP context for the current request</param>
    /// <returns>Task representing the asynchronous middleware execution</returns>
    public async Task InvokeAsync(HttpContext context)
    {
        // Only apply throttling to analytics endpoints
        if (!IsAnalyticsEndpoint(context.Request.Path))
        {
            await _next(context);
            return;
        }

        // Check system health before processing
        var isHealthy = await _concurrencyLimiter.IsSystemHealthyAsync();
        if (!isHealthy)
        {
            await WriteThrottleResponse(context, "System is currently unhealthy", 503, 60);
            return;
        }

        // Get current system stats
        var systemStats = _concurrencyLimiter.GetSystemStats();
        
        // Check if we're at capacity
        if (systemStats.ActiveConnections >= systemStats.MaxConcurrentQueries)
        {
            var retryAfter = CalculateRetryAfter(systemStats);
            
            _logger.LogWarning("Request throttled: {Path}. Active: {Active}/{Max}, Queue: {Queue}", 
                context.Request.Path, 
                systemStats.ActiveConnections, 
                systemStats.MaxConcurrentQueries,
                systemStats.QueuedQueries);

            await WriteThrottleResponse(context, 
                $"Maximum concurrent queries ({systemStats.MaxConcurrentQueries}) reached. " +
                $"Currently active: {systemStats.ActiveConnections}",
                429, retryAfter);
            return;
        }

        // Check memory pressure
        if (systemStats.AvailableMemory < 50 * 1024 * 1024) // Less than 50MB available
        {
            _logger.LogWarning("Request throttled due to memory pressure: {Path}. Available: {AvailableMB} MB", 
                context.Request.Path, systemStats.AvailableMemory / (1024 * 1024));

            await WriteThrottleResponse(context, 
                "System is under memory pressure. Please try again later.",
                503, 30);
            return;
        }

        await _next(context);
    }

    /// <summary>
    /// Determines if the request path corresponds to an analytics endpoint that requires throttling.
    /// Checks for query execution, join, and arrow-stream endpoints.
    /// </summary>
    /// <param name="path">The request path to evaluate</param>
    /// <returns>True if the path represents an analytics endpoint, false otherwise</returns>
    private static bool IsAnalyticsEndpoint(PathString path)
    {
        return path.StartsWithSegments("/query") && 
               (path.Value?.Contains("run") == true || 
                path.Value?.Contains("join") == true || 
                path.Value?.Contains("arrow-stream") == true);
    }

    /// <summary>
    /// Calculates intelligent retry-after delay based on current system load and queue statistics.
    /// Considers average query time and queue length to provide optimal retry timing.
    /// </summary>
    /// <param name="stats">Current system statistics including query times and queue length</param>
    /// <returns>Recommended retry delay in seconds (maximum 300 seconds)</returns>
    private static int CalculateRetryAfter(QuerySystemStats stats)
    {
        // Base retry time on average query time and current load
        var baseRetry = Math.Max(15, (int)stats.AverageQueryTime.TotalSeconds / 2);
        
        // Increase retry time based on queue length
        var queuePenalty = stats.QueuedQueries * 5;
        
        return Math.Min(baseRetry + queuePenalty, 300); // Max 5 minutes
    }

    /// <summary>
    /// Writes a structured throttling response with system statistics and retry recommendations.
    /// Provides detailed error information and actionable suggestions for the client.
    /// </summary>
    /// <param name="context">HTTP context for writing the response</param>
    /// <param name="message">Error message describing the throttling reason</param>
    /// <param name="statusCode">HTTP status code (429 for rate limiting, 503 for service unavailable)</param>
    /// <param name="retryAfterSeconds">Recommended retry delay in seconds</param>
    /// <returns>Task representing the asynchronous response writing operation</returns>
    private async Task WriteThrottleResponse(HttpContext context, string message, int statusCode, int retryAfterSeconds)
    {
        context.Response.StatusCode = statusCode;
        context.Response.ContentType = "application/json";
        context.Response.Headers["Retry-After"] = retryAfterSeconds.ToString();

        var systemStats = _concurrencyLimiter.GetSystemStats();
        var response = new AnalyticsQueryErrorResponse
        {
            ErrorMessage = message,
            ErrorType = statusCode == 429 ? "TooManyRequests" : "ServiceUnavailable",
            MemoryUsage = systemStats.TotalMemoryUsage,
            ConcurrentQueries = systemStats.ActiveConnections,
            ShouldRetry = true,
            Suggestions = new List<string>
            {
                $"Wait {retryAfterSeconds} seconds before retrying",
                "Check /query/status for current system load",
                "Consider scheduling queries during off-peak hours",
                "Use filters and projections to reduce query complexity"
            }
        };

        var jsonResponse = JsonSerializer.Serialize(response, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });

        await context.Response.WriteAsync(jsonResponse);
    }
}