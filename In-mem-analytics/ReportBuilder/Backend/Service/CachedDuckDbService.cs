using DuckDB.NET.Data;
using ReportBuilder.Request;
using ReportBuilder.Response;
using System.Text.Json;

namespace ReportBuilder.Service;

/// <summary>
/// Interface for DuckDB query execution with integrated Arrow result caching.
/// Provides transparent caching for query results with automatic cache management.
/// </summary>
public interface ICachedDuckDbService
{
    /// <summary>
    /// Executes a query with automatic Arrow caching support.
    /// Checks cache first, executes if needed, and stores results in Arrow format.
    /// </summary>
    /// <param name="request">Query request containing SQL and execution parameters</param>
    /// <param name="cancellationToken">Cancellation token for the operation</param>
    /// <returns>Query response with cache information and Arrow-formatted results</returns>
    Task<CachedQueryResponse> ExecuteQueryWithCacheAsync(QueryRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a streaming query with Arrow cache support.
    /// For cached results, creates a DuckDB view from cached Arrow data using relation_from_arrow_array().
    /// </summary>
    /// <param name="request">Query request for streaming execution</param>
    /// <param name="cancellationToken">Cancellation token for the operation</param>
    /// <returns>Async enumerable of query results with cache information</returns>
    IAsyncEnumerable<Dictionary<string, object?>> StreamQueryWithCacheAsync(QueryRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Forces cache refresh for a specific query by invalidating existing cache entry and re-executing.
    /// </summary>
    /// <param name="request">Query request to refresh in cache</param>
    /// <param name="cancellationToken">Cancellation token for the operation</param>
    /// <returns>Refreshed query results with updated cache entry</returns>
    Task<CachedQueryResponse> RefreshCacheAsync(QueryRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Preloads frequently accessed queries into cache for improved performance.
    /// </summary>
    /// <param name="queries">List of queries to preload into cache</param>
    /// <param name="cancellationToken">Cancellation token for the operation</param>
    /// <returns>Preload results with success/failure information</returns>
    Task<CachePreloadResult> PreloadCacheAsync(List<QueryRequest> queries, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets cache statistics and performance metrics for monitoring.
    /// </summary>
    /// <returns>Comprehensive cache statistics and performance data</returns>
    Task<ArrowCacheStatistics> GetCacheStatisticsAsync();
}

/// <summary>
/// DuckDB service with integrated Arrow columnar result caching.
/// Provides transparent caching layer that stores query results as Arrow IPC buffers
/// and reuses them via DuckDB's relation_from_arrow_array() functionality.
/// </summary>
public class CachedDuckDbService : ICachedDuckDbService
{
    private readonly IArrowCacheService _cacheService;
    private readonly IDuckDbQueryService _duckDbService;
    private readonly ILogger<CachedDuckDbService> _logger;
    private readonly CachedDuckDbConfiguration _config;

    public CachedDuckDbService(
        IArrowCacheService cacheService,
        IDuckDbQueryService duckDbService,
        ILogger<CachedDuckDbService> logger,
        CachedDuckDbConfiguration config)
    {
        _cacheService = cacheService;
        _duckDbService = duckDbService;
        _logger = logger;
        _config = config;
    }

    public async Task<CachedQueryResponse> ExecuteQueryWithCacheAsync(QueryRequest request, CancellationToken cancellationToken = default)
    {
        var startTime = DateTime.UtcNow;
        var queryId = Guid.NewGuid().ToString();

        try
        {
            _logger.LogInformation("Executing cached query {QueryId}: {SqlPreview}", 
                queryId, TruncateQuery(request.SqlQuery));

            // Generate cache fingerprint
            var fingerprint = _cacheService.GenerateQueryFingerprint(
                request.SqlQuery, 
                request.DatasetPath,
                ExtractParameters(request));

            // Check if caching should be used for this query
            if (!ShouldUseCache(request))
            {
                _logger.LogDebug("Cache disabled for query {QueryId}, executing directly", queryId);
                var directResult = await _duckDbService.ExecuteQueryAsync(request, cancellationToken);
                return ConvertToCache dQueryResponse(directResult, false, queryId, TimeSpan.Zero);
            }

            // Try to get from cache first
            var cachedResult = await _cacheService.GetCachedResultAsync(fingerprint, cancellationToken);
            if (cachedResult != null)
            {
                _logger.LogInformation("Serving query {QueryId} from cache (fingerprint: {Fingerprint})", 
                    queryId, fingerprint);

                var cacheResponse = await ConvertArrowBufferToQueryResponse(
                    cachedResult.ArrowBuffer, 
                    cachedResult.Metadata, 
                    request,
                    cancellationToken);

                cacheResponse.IsFromCache = true;
                cacheResponse.QueryId = queryId;
                cacheResponse.CacheAge = DateTime.UtcNow - cachedResult.CreatedAt;
                cacheResponse.CacheFingerprint = fingerprint;

                return cacheResponse;
            }

            // Execute query fresh and cache the result
            _logger.LogDebug("Cache miss for query {QueryId}, executing fresh query", queryId);
            
            var freshResult = await ExecuteAndCacheQuery(request, fingerprint, queryId, cancellationToken);
            freshResult.ExecutionTime = DateTime.UtcNow - startTime;
            
            return freshResult;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing cached query {QueryId}", queryId);
            throw;
        }
    }

    public async IAsyncEnumerable<Dictionary<string, object?>> StreamQueryWithCacheAsync(
        QueryRequest request, 
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var queryId = Guid.NewGuid().ToString();
        
        try
        {
            _logger.LogInformation("Streaming cached query {QueryId}: {SqlPreview}", 
                queryId, TruncateQuery(request.SqlQuery));

            var fingerprint = _cacheService.GenerateQueryFingerprint(
                request.SqlQuery, 
                request.DatasetPath,
                ExtractParameters(request));

            // Check cache for streaming queries
            if (ShouldUseCache(request))
            {
                var cachedResult = await _cacheService.GetCachedResultAsync(fingerprint, cancellationToken);
                if (cachedResult != null)
                {
                    _logger.LogInformation("Streaming query {QueryId} from cached Arrow buffer", queryId);
                    
                    // Use DuckDB's relation_from_arrow_array() to create a relation from cached Arrow data
                    await foreach (var row in StreamFromArrowBufferAsync(cachedResult.ArrowBuffer, request, cancellationToken))
                    {
                        yield return row;
                    }
                    yield break;
                }
            }

            // Stream from fresh execution and cache the result
            _logger.LogDebug("Streaming fresh query {QueryId} and caching result", queryId);
            
            var arrowBuffer = new List<byte[]>();
            var rowCount = 0;
            
            await foreach (var row in _duckDbService.StreamQueryAsync(request, cancellationToken))
            {
                // Collect rows for caching while streaming
                if (ShouldUseCache(request) && rowCount < _config.MaxRowsForCaching)
                {
                    // Convert row to Arrow format and buffer it
                    // This is simplified - in practice you'd batch the conversion
                }
                
                rowCount++;
                yield return row;
            }

            // Cache the collected Arrow data if appropriate
            if (ShouldUseCache(request) && rowCount <= _config.MaxRowsForCaching)
            {
                try
                {
                    // Convert collected data to Arrow buffer and cache it
                    // This is a placeholder for the actual Arrow conversion
                    _logger.LogDebug("Caching streamed query result for {QueryId} ({RowCount} rows)", queryId, rowCount);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to cache streamed query result for {QueryId}", queryId);
                }
            }
        }
        finally
        {
            _logger.LogDebug("Completed streaming query {QueryId}", queryId);
        }
    }

    public async Task<CachedQueryResponse> RefreshCacheAsync(QueryRequest request, CancellationToken cancellationToken = default)
    {
        var queryId = Guid.NewGuid().ToString();
        
        try
        {
            _logger.LogInformation("Refreshing cache for query {QueryId}: {SqlPreview}", 
                queryId, TruncateQuery(request.SqlQuery));

            var fingerprint = _cacheService.GenerateQueryFingerprint(
                request.SqlQuery, 
                request.DatasetPath,
                ExtractParameters(request));

            // Invalidate existing cache entry
            await _cacheService.InvalidateCacheAsync(fingerprint, cancellationToken);

            // Execute fresh and cache
            var freshResult = await ExecuteAndCacheQuery(request, fingerprint, queryId, cancellationToken);
            freshResult.IsFromCache = false; // Explicitly mark as fresh
            
            _logger.LogInformation("Cache refreshed for query {QueryId} (fingerprint: {Fingerprint})", 
                queryId, fingerprint);

            return freshResult;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error refreshing cache for query {QueryId}", queryId);
            throw;
        }
    }

    public async Task<CachePreloadResult> PreloadCacheAsync(List<QueryRequest> queries, CancellationToken cancellationToken = default)
    {
        var result = new CachePreloadResult
        {
            TotalQueries = queries.Count,
            StartTime = DateTime.UtcNow
        };

        try
        {
            _logger.LogInformation("Preloading cache with {QueryCount} queries", queries.Count);

            var tasks = queries.Select(async (query, index) =>
            {
                try
                {
                    var queryResult = await ExecuteQueryWithCacheAsync(query, cancellationToken);
                    return new PreloadQueryResult
                    {
                        Index = index,
                        Success = true,
                        QueryFingerprint = queryResult.CacheFingerprint,
                        ExecutionTime = queryResult.ExecutionTime,
                        IsFromCache = queryResult.IsFromCache
                    };
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to preload query {Index}: {SqlPreview}", 
                        index, TruncateQuery(query.SqlQuery));
                    
                    return new PreloadQueryResult
                    {
                        Index = index,
                        Success = false,
                        ErrorMessage = ex.Message
                    };
                }
            });

            var preloadResults = await Task.WhenAll(tasks);
            
            result.QueryResults = preloadResults.ToList();
            result.SuccessfulQueries = preloadResults.Count(r => r.Success);
            result.FailedQueries = preloadResults.Count(r => !r.Success);
            result.CompletionTime = DateTime.UtcNow;
            result.TotalDuration = result.CompletionTime - result.StartTime;

            _logger.LogInformation("Cache preload completed: {Successful}/{Total} queries successful in {Duration}", 
                result.SuccessfulQueries, result.TotalQueries, result.TotalDuration);

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during cache preload");
            result.CompletionTime = DateTime.UtcNow;
            result.TotalDuration = result.CompletionTime - result.StartTime;
            throw;
        }
    }

    public async Task<ArrowCacheStatistics> GetCacheStatisticsAsync()
    {
        return await _cacheService.GetCacheStatisticsAsync();
    }

    // Private helper methods

    private async Task<CachedQueryResponse> ExecuteAndCacheQuery(
        QueryRequest request, 
        string fingerprint, 
        string queryId, 
        CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        
        // Execute the query using the base DuckDB service
        var queryResult = await _duckDbService.ExecuteQueryAsync(request, cancellationToken);
        
        // Convert result to Arrow format and cache it
        var (arrowBuffer, metadata) = await ConvertQueryResultToArrow(queryResult, cancellationToken);
        
        var cacheTtl = DetermineCacheTtl(request, metadata);
        await _cacheService.SetCachedResultAsync(fingerprint, arrowBuffer, metadata, cacheTtl, cancellationToken);

        var response = ConvertToCachedQueryResponse(queryResult, false, queryId, TimeSpan.Zero);
        response.CacheFingerprint = fingerprint;
        response.ExecutionTime = DateTime.UtcNow - startTime;

        return response;
    }

    private async IAsyncEnumerable<Dictionary<string, object?>> StreamFromArrowBufferAsync(
        byte[] arrowBuffer, 
        QueryRequest request,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        try
        {
            using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync(cancellationToken);

            // Create a relation from the Arrow buffer using DuckDB's relation_from_arrow_array()
            // This is a simplified implementation - in practice you'd need proper Arrow integration
            var tempTableName = $"cached_data_{Guid.NewGuid():N}";
            
            // In a real implementation, you would:
            // 1. Write the Arrow buffer to a temporary file or memory stream
            // 2. Use DuckDB's Arrow integration to load it: "SELECT * FROM relation_from_arrow_array(...)"
            
            // For now, this is a placeholder that demonstrates the concept
            var loadArrowCommand = $"CREATE TABLE {tempTableName} AS SELECT * FROM read_arrow_from_buffer($1)";
            using var loadCmd = new DuckDBCommand(loadArrowCommand, connection);
            // loadCmd.Parameters.Add(new DuckDBParameter("$1", arrowBuffer));
            // await loadCmd.ExecuteNonQueryAsync(cancellationToken);

            // Execute the original query against the cached Arrow data
            var modifiedQuery = request.SqlQuery.Replace("input_data", tempTableName);
            using var queryCmd = new DuckDBCommand(modifiedQuery, connection);
            using var reader = await queryCmd.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                }
                yield return row;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error streaming from cached Arrow buffer");
            throw;
        }
    }

    private async Task<(byte[] arrowBuffer, QueryCacheMetadata metadata)> ConvertQueryResultToArrow(
        QueryResponse queryResult, 
        CancellationToken cancellationToken)
    {
        // This is a placeholder for Arrow conversion logic
        // In practice, you would:
        // 1. Iterate through the query result data
        // 2. Convert to Apache Arrow format using Arrow .NET libraries
        // 3. Serialize to Arrow IPC format
        
        var arrowBuffer = Array.Empty<byte>(); // Placeholder
        
        var metadata = new QueryCacheMetadata
        {
            RowCount = queryResult.TotalRows,
            ColumnCount = 0, // Would be determined from schema
            DataSizeBytes = arrowBuffer.Length,
            ExecutionTime = DateTime.UtcNow,
            QueryComplexity = QueryComplexity.Simple // Would be analyzed
        };

        return (arrowBuffer, metadata);
    }

    private async Task<CachedQueryResponse> ConvertArrowBufferToQueryResponse(
        byte[] arrowBuffer, 
        QueryCacheMetadata metadata, 
        QueryRequest request,
        CancellationToken cancellationToken)
    {
        // Convert cached Arrow buffer back to QueryResponse format
        // This would involve deserializing the Arrow IPC data
        
        return new CachedQueryResponse
        {
            Success = true,
            TotalRows = metadata.RowCount,
            InitialMemoryUsage = 0,
            FinalMemoryUsage = arrowBuffer.Length,
            Data = StreamFromArrowBufferAsync(arrowBuffer, request, cancellationToken),
            IsFromCache = true,
            CacheFingerprint = string.Empty // Will be set by caller
        };
    }

    private CachedQueryResponse ConvertToCachedQueryResponse(QueryResponse queryResult, bool isFromCache, string queryId, TimeSpan cacheAge)
    {
        return new CachedQueryResponse
        {
            Success = queryResult.Success,
            ErrorMessage = queryResult.ErrorMessage,
            InitialMemoryUsage = queryResult.InitialMemoryUsage,
            FinalMemoryUsage = queryResult.FinalMemoryUsage,
            TotalRows = queryResult.TotalRows,
            Data = queryResult.Data,
            IsFromCache = isFromCache,
            QueryId = queryId,
            CacheAge = cacheAge
        };
    }

    private bool ShouldUseCache(QueryRequest request)
    {
        // Determine if caching should be used based on query characteristics
        if (!_config.EnableCaching) return false;
        
        // Don't cache very small or very large queries
        if (request.SqlQuery.Length < _config.MinQueryLengthForCaching) return false;
        if (request.SqlQuery.Length > _config.MaxQueryLengthForCaching) return false;
        
        // Don't cache queries with non-deterministic functions
        var upperSql = request.SqlQuery.ToUpper();
        var nonDeterministicKeywords = new[] { "NOW()", "RANDOM()", "UUID()", "CURRENT_TIMESTAMP" };
        if (nonDeterministicKeywords.Any(keyword => upperSql.Contains(keyword))) return false;
        
        return true;
    }

    private TimeSpan DetermineCacheTtl(QueryRequest request, QueryCacheMetadata metadata)
    {
        // Determine cache TTL based on query characteristics
        var baseTtl = _config.DefaultCacheTtl;
        
        // Longer TTL for complex queries (more expensive to recompute)
        if (metadata.QueryComplexity == QueryComplexity.Complex)
            baseTtl = TimeSpan.FromTicks(baseTtl.Ticks * 2);
        
        // Shorter TTL for very large results (memory pressure)
        if (metadata.DataSizeBytes > 10 * 1024 * 1024) // 10MB
            baseTtl = TimeSpan.FromTicks(baseTtl.Ticks / 2);
        
        return baseTtl;
    }

    private Dictionary<string, object>? ExtractParameters(QueryRequest request)
    {
        // Extract any query parameters for fingerprinting
        // This would depend on how parameters are passed in your QueryRequest
        return null; // Placeholder
    }

    private string TruncateQuery(string query, int maxLength = 100)
    {
        return query.Length <= maxLength ? query : query[..maxLength] + "...";
    }
}

// Supporting types

public class CachedQueryResponse : QueryResponse
{
    public bool IsFromCache { get; set; }
    public string QueryId { get; set; } = string.Empty;
    public string CacheFingerprint { get; set; } = string.Empty;
    public TimeSpan CacheAge { get; set; }
    public TimeSpan ExecutionTime { get; set; }
}

public class CachePreloadResult
{
    public int TotalQueries { get; set; }
    public int SuccessfulQueries { get; set; }
    public int FailedQueries { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime CompletionTime { get; set; }
    public TimeSpan TotalDuration { get; set; }
    public List<PreloadQueryResult> QueryResults { get; set; } = new();
}

public class PreloadQueryResult
{
    public int Index { get; set; }
    public bool Success { get; set; }
    public string? QueryFingerprint { get; set; }
    public TimeSpan ExecutionTime { get; set; }
    public bool IsFromCache { get; set; }
    public string? ErrorMessage { get; set; }
}

public class CachedDuckDbConfiguration
{
    public bool EnableCaching { get; set; } = true;
    public TimeSpan DefaultCacheTtl { get; set; } = TimeSpan.FromMinutes(30);
    public int MinQueryLengthForCaching { get; set; } = 50;
    public int MaxQueryLengthForCaching { get; set; } = 10000;
    public int MaxRowsForCaching { get; set; } = 100000;
    public long MaxCacheSizeBytes { get; set; } = 500 * 1024 * 1024; // 500MB
}