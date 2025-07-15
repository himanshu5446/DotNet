using Microsoft.AspNetCore.Mvc;
using ReportBuilder.Service;
using System.Text.Json;

namespace ReportBuilder.Controllers;

/// <summary>
/// Controller for monitoring DuckDB memory usage and system performance metrics.
/// Provides real-time memory statistics, query concurrency information, and system health data.
/// </summary>
[ApiController]
[Route("api/duckdb")]
public class DuckDbMonitoringController : ControllerBase
{
    private readonly IConcurrencyLimiterService _concurrencyLimiter;
    private readonly ILogger<DuckDbMonitoringController> _logger;
    private static readonly object _memoryLock = new();
    private static long _lastPeakMemoryBytes = 0;
    private static DateTime _lastPeakTime = DateTime.UtcNow;

    /// <summary>
    /// Initializes a new instance of the DuckDbMonitoringController.
    /// </summary>
    /// <param name="concurrencyLimiter">Service for tracking active queries and system concurrency</param>
    /// <param name="logger">Logger for recording monitoring events and system metrics</param>
    public DuckDbMonitoringController(
        IConcurrencyLimiterService concurrencyLimiter,
        ILogger<DuckDbMonitoringController> logger)
    {
        _concurrencyLimiter = concurrencyLimiter;
        _logger = logger;
    }

    /// <summary>
    /// Returns current DuckDB memory usage and system performance metrics.
    /// Provides .NET memory statistics, active query count, and peak memory tracking.
    /// </summary>
    /// <returns>JSON response with current memory usage, query statistics, and system health data</returns>
    [HttpGet("memory")]
    public async Task<IActionResult> GetMemoryUsage()
    {
        try
        {
            var currentMemoryBytes = GC.GetTotalMemory(false);
            var currentMemoryMB = Math.Round(currentMemoryBytes / (1024.0 * 1024.0), 2);
            
            // Track peak memory usage
            lock (_memoryLock)
            {
                if (currentMemoryBytes > _lastPeakMemoryBytes)
                {
                    _lastPeakMemoryBytes = currentMemoryBytes;
                    _lastPeakTime = DateTime.UtcNow;
                }
            }

            var lastPeakMemoryMB = Math.Round(_lastPeakMemoryBytes / (1024.0 * 1024.0), 2);
            
            // Get current system stats from concurrency limiter
            var systemStats = _concurrencyLimiter.GetSystemStats();
            var activeQueries = _concurrencyLimiter.GetActiveQueries();

            var response = new
            {
                dotNetMemoryBytes = currentMemoryBytes,
                dotNetMemoryMB = currentMemoryMB,
                queriesInProgress = systemStats.ActiveConnections,
                lastPeakMemoryMB = lastPeakMemoryMB,
                lastPeakTime = _lastPeakTime.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
                timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
                systemHealth = new
                {
                    maxConcurrentQueries = systemStats.MaxConcurrentQueries,
                    queuedQueries = systemStats.QueuedQueries,
                    averageQueryTimeSeconds = Math.Round(systemStats.AverageQueryTime.TotalSeconds, 2),
                    availableMemoryMB = Math.Round(systemStats.AvailableMemory / (1024.0 * 1024.0), 2),
                    isSystemHealthy = await _concurrencyLimiter.IsSystemHealthyAsync()
                },
                activeQueries = activeQueries.Select(q => new
                {
                    queryId = q.QueryId,
                    startTime = q.StartTime.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
                    durationSeconds = Math.Round((DateTime.UtcNow - q.StartTime).TotalSeconds, 1),
                    status = q.Status,
                    sqlPreview = q.SqlQuery.Length > 50 ? q.SqlQuery.Substring(0, 50) + "..." : q.SqlQuery
                }).ToList()
            };

            _logger.LogDebug("Memory monitoring requested - Current: {MemoryMB} MB, Active queries: {ActiveQueries}", 
                currentMemoryMB, systemStats.ActiveConnections);

            return Ok(response);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving DuckDB memory usage");
            return StatusCode(500, new
            {
                success = false,
                errorMessage = "Failed to retrieve memory usage statistics",
                timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
            });
        }
    }

    /// <summary>
    /// Forces garbage collection and returns updated memory statistics.
    /// Useful for testing memory cleanup and getting accurate memory measurements.
    /// </summary>
    /// <returns>JSON response with memory usage before and after garbage collection</returns>
    [HttpPost("memory/gc")]
    public IActionResult ForceGarbageCollection()
    {
        try
        {
            var memoryBefore = GC.GetTotalMemory(false);
            var memoryBeforeMB = Math.Round(memoryBefore / (1024.0 * 1024.0), 2);
            
            // Force garbage collection
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            
            var memoryAfter = GC.GetTotalMemory(true);
            var memoryAfterMB = Math.Round(memoryAfter / (1024.0 * 1024.0), 2);
            var freedMemoryMB = Math.Round((memoryBefore - memoryAfter) / (1024.0 * 1024.0), 2);

            var response = new
            {
                success = true,
                memoryBeforeGC = new
                {
                    bytes = memoryBefore,
                    megabytes = memoryBeforeMB
                },
                memoryAfterGC = new
                {
                    bytes = memoryAfter,
                    megabytes = memoryAfterMB
                },
                freedMemoryMB = freedMemoryMB,
                timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
            };

            _logger.LogInformation("Forced garbage collection - Freed {FreedMB} MB memory", freedMemoryMB);
            
            return Ok(response);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during forced garbage collection");
            return StatusCode(500, new
            {
                success = false,
                errorMessage = "Failed to perform garbage collection",
                timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
            });
        }
    }

    /// <summary>
    /// Resets the peak memory tracking to current memory usage.
    /// Useful for starting fresh memory spike monitoring after system optimization.
    /// </summary>
    /// <returns>JSON response confirming peak memory reset</returns>
    [HttpPost("memory/reset-peak")]
    public IActionResult ResetPeakMemory()
    {
        try
        {
            var currentMemoryBytes = GC.GetTotalMemory(false);
            var currentMemoryMB = Math.Round(currentMemoryBytes / (1024.0 * 1024.0), 2);
            
            lock (_memoryLock)
            {
                _lastPeakMemoryBytes = currentMemoryBytes;
                _lastPeakTime = DateTime.UtcNow;
            }

            var response = new
            {
                success = true,
                message = "Peak memory tracking reset",
                newPeakMemoryMB = currentMemoryMB,
                resetTime = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
            };

            _logger.LogInformation("Peak memory tracking reset to {MemoryMB} MB", currentMemoryMB);
            
            return Ok(response);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error resetting peak memory tracking");
            return StatusCode(500, new
            {
                success = false,
                errorMessage = "Failed to reset peak memory tracking",
                timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
            });
        }
    }
}