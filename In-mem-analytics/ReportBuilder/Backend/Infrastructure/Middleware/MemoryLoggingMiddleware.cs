using System.Diagnostics;

namespace ReportBuilder.Infrastructure.Middleware;

/// <summary>
/// Middleware for comprehensive memory usage monitoring and logging during request processing.
/// Provides real-time memory tracking, peak usage detection, and automatic cleanup for high-memory operations.
/// </summary>
public class MemoryLoggingMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<MemoryLoggingMiddleware> _logger;
    private const long MemoryWarnThresholdBytes = 400 * 1024 * 1024; // 400MB
    private const long MemoryCriticalThresholdBytes = 480 * 1024 * 1024; // 480MB

    /// <summary>
    /// Initializes a new instance of the MemoryLoggingMiddleware.
    /// </summary>
    /// <param name="next">The next middleware delegate in the pipeline</param>
    /// <param name="logger">Logger for recording memory usage metrics and performance data</param>
    public MemoryLoggingMiddleware(RequestDelegate next, ILogger<MemoryLoggingMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }

    /// <summary>
    /// Processes HTTP requests with comprehensive memory monitoring and performance tracking.
    /// Monitors memory usage throughout request lifecycle, provides cleanup for high-memory operations.
    /// </summary>
    /// <param name="context">The HTTP context for the current request</param>
    /// <returns>Task representing the asynchronous middleware execution</returns>
    public async Task InvokeAsync(HttpContext context)
    {
        var startTime = DateTime.UtcNow;
        var initialMemory = GC.GetTotalMemory(false);
        var requestId = context.TraceIdentifier;
        var endpoint = $"{context.Request.Method} {context.Request.Path}";

        // Log initial memory state for analytics endpoints
        if (IsAnalyticsEndpoint(context.Request.Path))
        {
            _logger.LogInformation("Request {RequestId} started: {Endpoint}. Initial memory: {MemoryMB} MB", 
                requestId, endpoint, initialMemory / (1024 * 1024));
        }

        var stopwatch = Stopwatch.StartNew();
        var peakMemory = initialMemory;
        var memoryCheckTask = Task.CompletedTask;

        try
        {
            // Start memory monitoring for long-running requests
            if (IsAnalyticsEndpoint(context.Request.Path))
            {
                var cts = new CancellationTokenSource();
                context.RequestAborted.Register(() => cts.Cancel());
                memoryCheckTask = MonitorMemoryAsync(requestId, endpoint, cts.Token);
            }

            // Execute the request
            await _next(context);

            // Check peak memory
            var currentMemory = GC.GetTotalMemory(false);
            peakMemory = Math.Max(peakMemory, currentMemory);
        }
        catch (Exception ex)
        {
            var errorMemory = GC.GetTotalMemory(false);
            peakMemory = Math.Max(peakMemory, errorMemory);
            
            _logger.LogError(ex, "Request {RequestId} failed: {Endpoint}. Error memory: {MemoryMB} MB", 
                requestId, endpoint, errorMemory / (1024 * 1024));
            
            throw;
        }
        finally
        {
            stopwatch.Stop();
            var finalMemory = GC.GetTotalMemory(false);
            var duration = stopwatch.Elapsed;
            var memoryDelta = finalMemory - initialMemory;

            // Log completion for analytics endpoints
            if (IsAnalyticsEndpoint(context.Request.Path))
            {
                var logLevel = DetermineLogLevel(peakMemory, memoryDelta);
                
                _logger.Log(logLevel, 
                    "Request {RequestId} completed: {Endpoint}. " +
                    "Duration: {Duration:mm\\:ss\\.fff}, " +
                    "Initial: {InitialMB} MB, " +
                    "Final: {FinalMB} MB, " +
                    "Peak: {PeakMB} MB, " +
                    "Delta: {DeltaMB} MB, " +
                    "Status: {StatusCode}",
                    requestId, endpoint, duration,
                    initialMemory / (1024 * 1024),
                    finalMemory / (1024 * 1024),
                    peakMemory / (1024 * 1024),
                    memoryDelta / (1024 * 1024),
                    context.Response.StatusCode);

                // Force cleanup for high memory usage
                if (peakMemory > MemoryWarnThresholdBytes)
                {
                    _logger.LogWarning("High memory usage detected for request {RequestId}. Forcing GC cleanup.", requestId);
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    GC.Collect();
                    
                    var afterCleanupMemory = GC.GetTotalMemory(false);
                    _logger.LogInformation("Post-cleanup memory: {MemoryMB} MB (freed: {FreedMB} MB)", 
                        afterCleanupMemory / (1024 * 1024), 
                        (finalMemory - afterCleanupMemory) / (1024 * 1024));
                }
            }

            // Cancel memory monitoring
            if (memoryCheckTask != Task.CompletedTask)
            {
                try
                {
                    await memoryCheckTask;
                }
                catch (OperationCanceledException)
                {
                    // Expected when request completes
                }
            }
        }
    }

    /// <summary>
    /// Continuously monitors memory usage during long-running analytics requests.
    /// Provides real-time logging of memory changes and alerts for high usage scenarios.
    /// </summary>
    /// <param name="requestId">Unique identifier for the request being monitored</param>
    /// <param name="endpoint">The endpoint being processed for context in logging</param>
    /// <param name="cancellationToken">Token for cancelling the monitoring when request completes</param>
    /// <returns>Task representing the asynchronous memory monitoring operation</returns>
    private async Task MonitorMemoryAsync(string requestId, string endpoint, CancellationToken cancellationToken)
    {
        var checkInterval = TimeSpan.FromSeconds(5);
        var lastMemoryCheck = GC.GetTotalMemory(false);
        
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(checkInterval, cancellationToken);
                
                var currentMemory = GC.GetTotalMemory(false);
                var memoryDelta = currentMemory - lastMemoryCheck;
                
                // Log significant memory changes
                if (Math.Abs(memoryDelta) > 50 * 1024 * 1024) // 50MB change
                {
                    _logger.LogDebug("Request {RequestId} memory change: {DeltaMB} MB (current: {CurrentMB} MB)", 
                        requestId, memoryDelta / (1024 * 1024), currentMemory / (1024 * 1024));
                }
                
                // Warn on high memory usage
                if (currentMemory > MemoryWarnThresholdBytes)
                {
                    _logger.LogWarning("Request {RequestId} ({Endpoint}) high memory usage: {MemoryMB} MB", 
                        requestId, endpoint, currentMemory / (1024 * 1024));
                }
                
                // Critical memory usage
                if (currentMemory > MemoryCriticalThresholdBytes)
                {
                    _logger.LogCritical("Request {RequestId} ({Endpoint}) critical memory usage: {MemoryMB} MB", 
                        requestId, endpoint, currentMemory / (1024 * 1024));
                }
                
                lastMemoryCheck = currentMemory;
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when request completes
        }
    }

    /// <summary>
    /// Determines if the request path corresponds to an analytics endpoint requiring memory monitoring.
    /// Analytics endpoints typically involve data processing and require detailed memory tracking.
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
    /// Determines appropriate log level based on peak memory usage and memory delta.
    /// Escalates log level for high memory usage to ensure visibility in monitoring systems.
    /// </summary>
    /// <param name="peakMemory">Peak memory usage during request processing in bytes</param>
    /// <param name="memoryDelta">Change in memory usage from start to end in bytes</param>
    /// <returns>Appropriate log level (Information, Warning, or Critical)</returns>
    private static LogLevel DetermineLogLevel(long peakMemory, long memoryDelta)
    {
        if (peakMemory > MemoryCriticalThresholdBytes)
            return LogLevel.Critical;
        
        if (peakMemory > MemoryWarnThresholdBytes || Math.Abs(memoryDelta) > 100 * 1024 * 1024)
            return LogLevel.Warning;
        
        return LogLevel.Information;
    }
}