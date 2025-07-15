using ReportBuilder.Response;
using System.Collections.Concurrent;

namespace ReportBuilder.Service;

public interface IConcurrencyLimiterService
{
    Task<ConcurrencySlot> TryAcquireSlotAsync(string queryId, string operation, CancellationToken cancellationToken = default);
    Task ReleaseSlotAsync(string queryId);
    QuerySystemStats GetSystemStats();
    List<ActiveQuery> GetActiveQueries();
    Task<bool> IsSystemHealthyAsync();
}

public class ConcurrencyLimiterService : IConcurrencyLimiterService, IDisposable
{
    private readonly SemaphoreSlim _querySemaphore;
    private readonly ConcurrentDictionary<string, ActiveQuery> _activeQueries;
    private readonly ILogger<ConcurrencyLimiterService> _logger;
    private readonly Timer _healthCheckTimer;
    private readonly object _statsLock = new();
    
    private const int MaxConcurrentQueries = 3;
    private const long MemoryThresholdBytes = 500 * 1024 * 1024; // 500MB
    private const int HealthCheckIntervalMs = 5000; // 5 seconds
    
    private volatile bool _systemHealthy = true;
    private DateTime _lastHealthCheck = DateTime.UtcNow;
    private readonly List<TimeSpan> _recentQueryTimes = new();

    public ConcurrencyLimiterService(ILogger<ConcurrencyLimiterService> logger)
    {
        _logger = logger;
        _querySemaphore = new SemaphoreSlim(MaxConcurrentQueries, MaxConcurrentQueries);
        _activeQueries = new ConcurrentDictionary<string, ActiveQuery>();
        
        // Start health monitoring
        _healthCheckTimer = new Timer(PerformHealthCheck, null, HealthCheckIntervalMs, HealthCheckIntervalMs);
        
        _logger.LogInformation("Concurrency limiter initialized with max {MaxQueries} concurrent queries", MaxConcurrentQueries);
    }

    public async Task<ConcurrencySlot> TryAcquireSlotAsync(string queryId, string operation, CancellationToken cancellationToken = default)
    {
        _logger.LogDebug("Attempting to acquire slot for query {QueryId}, operation: {Operation}", queryId, operation);
        
        // Check system health first
        if (!_systemHealthy)
        {
            _logger.LogWarning("System unhealthy, rejecting new query {QueryId}", queryId);
            return new ConcurrencySlot
            {
                Success = false,
                ErrorMessage = "System is currently unhealthy. High memory usage or overloaded.",
                SuggestedRetryAfterSeconds = 30
            };
        }

        // Check memory before allowing new queries
        var currentMemory = GC.GetTotalMemory(false);
        if (currentMemory > MemoryThresholdBytes)
        {
            _logger.LogWarning("Memory threshold exceeded: {MemoryMB} MB, rejecting query {QueryId}", 
                currentMemory / (1024 * 1024), queryId);
            
            return new ConcurrencySlot
            {
                Success = false,
                ErrorMessage = $"Memory usage too high: {currentMemory / (1024 * 1024)} MB. Please try again later.",
                SuggestedRetryAfterSeconds = 60
            };
        }

        // Try to acquire semaphore with timeout
        var acquired = await _querySemaphore.WaitAsync(1000, cancellationToken);
        
        if (!acquired)
        {
            var activeCount = _activeQueries.Count;
            _logger.LogWarning("Failed to acquire slot for query {QueryId}. Active queries: {ActiveCount}", queryId, activeCount);
            
            return new ConcurrencySlot
            {
                Success = false,
                ErrorMessage = $"Maximum concurrent queries ({MaxConcurrentQueries}) reached. Currently active: {activeCount}",
                ActiveQueries = activeCount,
                SuggestedRetryAfterSeconds = 15
            };
        }

        // Register the active query
        var activeQuery = new ActiveQuery
        {
            QueryId = queryId,
            SqlQuery = operation,
            StartTime = DateTime.UtcNow,
            CurrentMemoryUsage = currentMemory,
            Status = "Running"
        };

        _activeQueries.TryAdd(queryId, activeQuery);
        
        _logger.LogInformation("Acquired concurrency slot for query {QueryId}. Active queries: {ActiveCount}/{MaxCount}", 
            queryId, _activeQueries.Count, MaxConcurrentQueries);

        return new ConcurrencySlot
        {
            Success = true,
            QueryId = queryId,
            SlotNumber = _activeQueries.Count,
            AcquiredAt = DateTime.UtcNow
        };
    }

    public async Task ReleaseSlotAsync(string queryId)
    {
        _logger.LogDebug("Releasing slot for query {QueryId}", queryId);
        
        try
        {
            if (_activeQueries.TryRemove(queryId, out var activeQuery))
            {
                var duration = DateTime.UtcNow - activeQuery.StartTime;
                
                // Track query completion time for health metrics
                lock (_statsLock)
                {
                    _recentQueryTimes.Add(duration);
                    
                    // Keep only last 50 query times
                    if (_recentQueryTimes.Count > 50)
                    {
                        _recentQueryTimes.RemoveAt(0);
                    }
                }
                
                _logger.LogInformation("Released slot for query {QueryId}. Duration: {Duration:mm\\:ss\\.fff}, Remaining active: {ActiveCount}", 
                    queryId, duration, _activeQueries.Count);
            }
            else
            {
                _logger.LogWarning("Attempted to release slot for unknown query {QueryId}", queryId);
            }
        }
        finally
        {
            // Always release the semaphore
            _querySemaphore.Release();
        }
    }

    public QuerySystemStats GetSystemStats()
    {
        var currentMemory = GC.GetTotalMemory(false);
        var availableMemory = Math.Max(0, MemoryThresholdBytes - currentMemory);
        
        TimeSpan averageQueryTime;
        lock (_statsLock)
        {
            averageQueryTime = _recentQueryTimes.Any() 
                ? TimeSpan.FromTicks((long)_recentQueryTimes.Select(t => t.Ticks).Average())
                : TimeSpan.Zero;
        }

        return new QuerySystemStats
        {
            ActiveConnections = _activeQueries.Count,
            QueuedQueries = MaxConcurrentQueries - _querySemaphore.CurrentCount - _activeQueries.Count,
            TotalMemoryUsage = currentMemory,
            AvailableMemory = availableMemory,
            MaxConcurrentQueries = MaxConcurrentQueries,
            AverageQueryTime = averageQueryTime
        };
    }

    public List<ActiveQuery> GetActiveQueries()
    {
        var currentMemory = GC.GetTotalMemory(false);
        
        return _activeQueries.Values.Select(q => new ActiveQuery
        {
            QueryId = q.QueryId,
            SqlQuery = q.SqlQuery.Length > 100 ? q.SqlQuery.Substring(0, 100) + "..." : q.SqlQuery,
            StartTime = q.StartTime,
            CurrentMemoryUsage = currentMemory, // Update with current memory
            RowsProcessed = q.RowsProcessed,
            Status = q.Status,
            FilePath = q.FilePath
        }).OrderBy(q => q.StartTime).ToList();
    }

    public Task<bool> IsSystemHealthyAsync()
    {
        return Task.FromResult(_systemHealthy);
    }

    private void PerformHealthCheck(object? state)
    {
        try
        {
            var currentMemory = GC.GetTotalMemory(false);
            var memoryHealthy = currentMemory < MemoryThresholdBytes;
            var concurrencyHealthy = _activeQueries.Count <= MaxConcurrentQueries;
            
            // Check for stuck queries (running longer than 10 minutes)
            var stuckQueries = _activeQueries.Values
                .Where(q => DateTime.UtcNow - q.StartTime > TimeSpan.FromMinutes(10))
                .ToList();

            var noStuckQueries = !stuckQueries.Any();
            
            if (stuckQueries.Any())
            {
                _logger.LogWarning("Found {Count} stuck queries running longer than 10 minutes", stuckQueries.Count);
                foreach (var query in stuckQueries)
                {
                    _logger.LogWarning("Stuck query {QueryId} running for {Duration}", 
                        query.QueryId, DateTime.UtcNow - query.StartTime);
                }
            }

            var previousHealth = _systemHealthy;
            _systemHealthy = memoryHealthy && concurrencyHealthy && noStuckQueries;
            
            if (previousHealth != _systemHealthy)
            {
                _logger.LogWarning("System health changed: {Health}. Memory: {MemoryMB} MB, Active queries: {ActiveCount}, Stuck queries: {StuckCount}", 
                    _systemHealthy ? "HEALTHY" : "UNHEALTHY", 
                    currentMemory / (1024 * 1024), 
                    _activeQueries.Count, 
                    stuckQueries.Count);
            }
            
            _lastHealthCheck = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during health check");
        }
    }

    public void Dispose()
    {
        _healthCheckTimer?.Dispose();
        _querySemaphore?.Dispose();
        _logger.LogInformation("Concurrency limiter disposed");
    }
}

public class ConcurrencySlot
{
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
    public string QueryId { get; set; } = string.Empty;
    public int SlotNumber { get; set; }
    public DateTime AcquiredAt { get; set; }
    public int ActiveQueries { get; set; }
    public int SuggestedRetryAfterSeconds { get; set; }
}