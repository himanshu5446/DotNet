using StackExchange.Redis;
using Microsoft.Extensions.Caching.Memory;
using System.Text.Json;

namespace ReportBuilder.Service;

/// <summary>
/// Redis-optimized Arrow cache service for distributed columnar result caching.
/// Provides high-performance distributed caching with Redis-specific optimizations for Arrow buffers.
/// </summary>
public class RedisArrowCacheService : IArrowCacheService
{
    private readonly IDatabase _redisDatabase;
    private readonly IMemoryCache _memoryCache;
    private readonly ILogger<RedisArrowCacheService> _logger;
    private readonly RedisArrowCacheConfiguration _config;
    
    // Cache statistics tracking
    private long _cacheHits = 0;
    private long _cacheMisses = 0;
    private long _cacheWrites = 0;
    private long _redisCacheHits = 0;
    private long _memoryCacheHits = 0;
    private readonly object _statsLock = new();

    // Redis key prefixes
    private const string ARROW_BUFFER_PREFIX = "arrow:buffer:";
    private const string ARROW_METADATA_PREFIX = "arrow:meta:";
    private const string CACHE_STATS_KEY = "arrow:stats";
    private const string QUERY_PATTERN_PREFIX = "arrow:pattern:";

    public RedisArrowCacheService(
        IConnectionMultiplexer redis,
        IMemoryCache memoryCache,
        ILogger<RedisArrowCacheService> logger,
        RedisArrowCacheConfiguration config)
    {
        _redisDatabase = redis.GetDatabase();
        _memoryCache = memoryCache;
        _logger = logger;
        _config = config;
    }

    public async Task<ArrowCacheEntry?> GetCachedResultAsync(string queryFingerprint, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogDebug("Checking Redis cache for query fingerprint: {Fingerprint}", queryFingerprint);

            // Try L1 cache (memory) first
            if (_memoryCache.TryGetValue(queryFingerprint, out ArrowCacheEntry? memoryEntry))
            {
                RecordCacheHit(isMemeory: true);
                _logger.LogDebug("Cache HIT (L1-Memory) for fingerprint: {Fingerprint}", queryFingerprint);
                await UpdateAccessStatsAsync(queryFingerprint);
                return memoryEntry;
            }

            // Try L2 cache (Redis)
            var bufferKey = ARROW_BUFFER_PREFIX + queryFingerprint;
            var metadataKey = ARROW_METADATA_PREFIX + queryFingerprint;

            // Use Redis pipeline for efficient batch operations
            var batch = _redisDatabase.CreateBatch();
            var bufferTask = batch.StringGetAsync(bufferKey);
            var metadataTask = batch.StringGetAsync(metadataKey);
            var ttlTask = batch.KeyTimeToLiveAsync(bufferKey);
            
            batch.Execute();
            
            var bufferData = await bufferTask;
            var metadataData = await metadataTask;
            var ttl = await ttlTask;

            if (bufferData.HasValue && metadataData.HasValue)
            {
                var metadata = JsonSerializer.Deserialize<QueryCacheMetadata>(metadataData!);
                var entry = new ArrowCacheEntry
                {
                    QueryFingerprint = queryFingerprint,
                    ArrowBuffer = bufferData!,
                    Metadata = metadata!,
                    CreatedAt = DateTime.UtcNow.Subtract(ttl ?? TimeSpan.Zero),
                    TimeToLive = ttl ?? _config.DefaultTtl,
                    AccessCount = 0
                };

                // Promote to L1 cache (memory) for faster future access
                var memoryCacheOptions = new MemoryCacheEntryOptions
                {
                    AbsoluteExpirationRelativeToNow = ttl,
                    Size = bufferData.Length(),
                    Priority = DetermineCachePriority(metadata!)
                };
                _memoryCache.Set(queryFingerprint, entry, memoryCacheOptions);

                RecordCacheHit(isMemeory: false);
                _logger.LogDebug("Cache HIT (L2-Redis) for fingerprint: {Fingerprint}, promoted to L1", queryFingerprint);
                await UpdateAccessStatsAsync(queryFingerprint);
                return entry;
            }

            RecordCacheMiss();
            _logger.LogDebug("Cache MISS for fingerprint: {Fingerprint}", queryFingerprint);
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving cached result from Redis for fingerprint: {Fingerprint}", queryFingerprint);
            RecordCacheMiss();
            return null;
        }
    }

    public async Task SetCachedResultAsync(string queryFingerprint, byte[] arrowBuffer, QueryCacheMetadata metadata, 
        TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        try
        {
            if (arrowBuffer.Length > _config.MaxEntrySizeBytes)
            {
                _logger.LogWarning("Arrow buffer too large for caching: {Size} bytes (max: {MaxSize})", 
                    arrowBuffer.Length, _config.MaxEntrySizeBytes);
                return;
            }

            var entry = new ArrowCacheEntry
            {
                QueryFingerprint = queryFingerprint,
                ArrowBuffer = arrowBuffer,
                Metadata = metadata,
                CreatedAt = DateTime.UtcNow,
                TimeToLive = ttl,
                AccessCount = 0
            };

            // Store in L1 cache (memory) for fastest access
            var memoryCacheOptions = new MemoryCacheEntryOptions
            {
                AbsoluteExpirationRelativeToNow = ttl,
                Size = arrowBuffer.Length,
                Priority = DetermineCachePriority(metadata)
            };
            _memoryCache.Set(queryFingerprint, entry, memoryCacheOptions);

            // Store in L2 cache (Redis) for persistence and sharing
            var bufferKey = ARROW_BUFFER_PREFIX + queryFingerprint;
            var metadataKey = ARROW_METADATA_PREFIX + queryFingerprint;
            var serializedMetadata = JsonSerializer.Serialize(metadata);

            // Use Redis pipeline for efficient batch operations
            var batch = _redisDatabase.CreateBatch();
            
            // Store Arrow buffer with compression if configured
            if (_config.UseCompression)
            {
                var compressedBuffer = await CompressArrowBufferAsync(arrowBuffer);
                batch.StringSetAsync(bufferKey, compressedBuffer, ttl);
            }
            else
            {
                batch.StringSetAsync(bufferKey, arrowBuffer, ttl);
            }
            
            batch.StringSetAsync(metadataKey, serializedMetadata, ttl);
            
            // Update cache statistics
            batch.HashIncrementAsync(CACHE_STATS_KEY, "writes", 1);
            batch.HashSetAsync(CACHE_STATS_KEY, "last_write", DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            
            await batch.ExecuteAsync();

            // Track query patterns for analytics
            if (_config.TrackQueryPatterns)
            {
                await TrackQueryPatternAsync(queryFingerprint, metadata);
            }

            RecordCacheWrite();
            
            _logger.LogInformation("Cached Arrow result in Redis for fingerprint: {Fingerprint}, Size: {SizeKB} KB, TTL: {TTL}", 
                queryFingerprint, arrowBuffer.Length / 1024, ttl);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error caching Arrow result in Redis for fingerprint: {Fingerprint}", queryFingerprint);
        }
    }

    public string GenerateQueryFingerprint(string sqlQuery, string? filePath = null, Dictionary<string, object>? parameters = null)
    {
        var fingerprintData = new
        {
            sql = NormalizeSql(sqlQuery),
            file = filePath,
            parameters = parameters?.OrderBy(kvp => kvp.Key).ToDictionary(kvp => kvp.Key, kvp => kvp.Value?.ToString()),
            version = _config.CacheVersion,
            node = _config.NodeIdentifier // Include node ID for multi-node consistency
        };

        var json = JsonSerializer.Serialize(fingerprintData, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });

        return ComputeSHA256Hash(json);
    }

    public async Task<CachedQueryResult> ExecuteWithCacheAsync(string sqlQuery, string? filePath = null, 
        Dictionary<string, object>? parameters = null, TimeSpan? cacheTtl = null, 
        CancellationToken cancellationToken = default)
    {
        var startTime = DateTime.UtcNow;
        var fingerprint = GenerateQueryFingerprint(sqlQuery, filePath, parameters);
        
        try
        {
            // Check cache first
            var cachedEntry = await GetCachedResultAsync(fingerprint, cancellationToken);
            if (cachedEntry != null)
            {
                _logger.LogInformation("Serving cached result from Redis for query fingerprint: {Fingerprint}", fingerprint);
                
                return new CachedQueryResult
                {
                    IsFromCache = true,
                    ArrowBuffer = cachedEntry.ArrowBuffer,
                    Metadata = cachedEntry.Metadata,
                    QueryFingerprint = fingerprint,
                    ExecutionTime = DateTime.UtcNow - startTime,
                    CacheAge = DateTime.UtcNow - cachedEntry.CreatedAt
                };
            }

            // Execute query using distributed lock to prevent cache stampede
            var lockKey = $"lock:{fingerprint}";
            var lockValue = Guid.NewGuid().ToString();
            var lockTtl = TimeSpan.FromMinutes(5);

            if (await _redisDatabase.StringSetAsync(lockKey, lockValue, lockTtl, When.NotExists))
            {
                try
                {
                    _logger.LogDebug("Acquired execution lock for fingerprint: {Fingerprint}", fingerprint);
                    
                    var (arrowBuffer, queryMetadata) = await ExecuteQueryToArrowAsync(sqlQuery, filePath, parameters, cancellationToken);
                    
                    // Cache the result
                    var ttl = cacheTtl ?? _config.DefaultTtl;
                    await SetCachedResultAsync(fingerprint, arrowBuffer, queryMetadata, ttl, cancellationToken);

                    return new CachedQueryResult
                    {
                        IsFromCache = false,
                        ArrowBuffer = arrowBuffer,
                        Metadata = queryMetadata,
                        QueryFingerprint = fingerprint,
                        ExecutionTime = DateTime.UtcNow - startTime,
                        CacheAge = TimeSpan.Zero
                    };
                }
                finally
                {
                    // Release the lock
                    await _redisDatabase.ScriptEvaluateAsync(
                        "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
                        new RedisKey[] { lockKey },
                        new RedisValue[] { lockValue });
                }
            }
            else
            {
                // Another node is executing this query, wait and check cache again
                _logger.LogDebug("Query execution locked by another node for fingerprint: {Fingerprint}", fingerprint);
                await Task.Delay(100, cancellationToken);
                
                var retryEntry = await GetCachedResultAsync(fingerprint, cancellationToken);
                if (retryEntry != null)
                {
                    return new CachedQueryResult
                    {
                        IsFromCache = true,
                        ArrowBuffer = retryEntry.ArrowBuffer,
                        Metadata = retryEntry.Metadata,
                        QueryFingerprint = fingerprint,
                        ExecutionTime = DateTime.UtcNow - startTime,
                        CacheAge = DateTime.UtcNow - retryEntry.CreatedAt
                    };
                }
                
                throw new InvalidOperationException("Failed to execute query: locked by another node and no cached result available");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing query with Redis cache for fingerprint: {Fingerprint}", fingerprint);
            throw;
        }
    }

    public async Task<ArrowCacheStatistics> GetCacheStatisticsAsync()
    {
        try
        {
            // Get statistics from Redis
            var redisStats = await _redisDatabase.HashGetAllAsync(CACHE_STATS_KEY);
            var redisStatsDict = redisStats.ToDictionary(kv => kv.Name.ToString(), kv => kv.Value.ToString());

            // Get memory cache info
            var memoryCacheInfo = GetMemoryCacheInfo();

            lock (_statsLock)
            {
                var totalAccess = _cacheHits + _cacheMisses;
                var hitRate = totalAccess > 0 ? (double)_cacheHits / totalAccess : 0.0;

                return new ArrowCacheStatistics
                {
                    CacheHits = _cacheHits,
                    CacheMisses = _cacheMisses,
                    CacheWrites = _cacheWrites,
                    HitRate = hitRate,
                    MemoryEntries = memoryCacheInfo.EntryCount,
                    MemoryUsageBytes = memoryCacheInfo.SizeEstimate,
                    MemoryUsageMB = memoryCacheInfo.SizeEstimate / (1024.0 * 1024.0),
                    UniqueQueries = GetRedisStatInt(redisStatsDict, "unique_queries"),
                    AverageEntryAge = TimeSpan.FromSeconds(GetRedisStatInt(redisStatsDict, "avg_age_seconds")),
                    LastAccessed = DateTimeOffset.FromUnixTimeSeconds(GetRedisStatLong(redisStatsDict, "last_access")).DateTime
                };
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving cache statistics from Redis");
            return new ArrowCacheStatistics();
        }
    }

    public async Task<int> InvalidateCacheAsync(string? pattern = null, CancellationToken cancellationToken = default)
    {
        try
        {
            var invalidatedCount = 0;

            if (pattern == null)
            {
                // Clear all Arrow cache entries
                var server = _redisDatabase.Multiplexer.GetServer(_redisDatabase.Multiplexer.GetEndPoints().First());
                
                var bufferKeys = server.Keys(pattern: ARROW_BUFFER_PREFIX + "*").ToArray();
                var metadataKeys = server.Keys(pattern: ARROW_METADATA_PREFIX + "*").ToArray();
                var allKeys = bufferKeys.Concat(metadataKeys).ToArray();

                if (allKeys.Any())
                {
                    await _redisDatabase.KeyDeleteAsync(allKeys);
                    invalidatedCount = allKeys.Length / 2; // Each entry has buffer + metadata
                }

                // Clear memory cache
                // Note: MemoryCache doesn't support pattern-based clearing
                _logger.LogInformation("Invalidated all Redis cache entries: {Count}", invalidatedCount);
            }
            else
            {
                // Pattern-based invalidation
                var server = _redisDatabase.Multiplexer.GetServer(_redisDatabase.Multiplexer.GetEndPoints().First());
                
                var bufferPattern = ARROW_BUFFER_PREFIX + pattern;
                var metadataPattern = ARROW_METADATA_PREFIX + pattern;
                
                var bufferKeys = server.Keys(pattern: bufferPattern).ToArray();
                var metadataKeys = server.Keys(pattern: metadataPattern).ToArray();
                var matchingKeys = bufferKeys.Concat(metadataKeys).ToArray();

                if (matchingKeys.Any())
                {
                    await _redisDatabase.KeyDeleteAsync(matchingKeys);
                    invalidatedCount = matchingKeys.Length / 2;
                }

                _logger.LogInformation("Invalidated Redis cache entries matching pattern '{Pattern}': {Count}", pattern, invalidatedCount);
            }

            // Update statistics
            await _redisDatabase.HashIncrementAsync(CACHE_STATS_KEY, "invalidations", invalidatedCount);
            
            return invalidatedCount;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error invalidating Redis cache entries");
            return 0;
        }
    }

    // Private helper methods

    private async Task<byte[]> CompressArrowBufferAsync(byte[] arrowBuffer)
    {
        // Implement compression (e.g., using System.IO.Compression)
        // This is a placeholder - would use actual compression
        return arrowBuffer;
    }

    private async Task TrackQueryPatternAsync(string fingerprint, QueryCacheMetadata metadata)
    {
        try
        {
            var patternKey = QUERY_PATTERN_PREFIX + DateTime.UtcNow.ToString("yyyy-MM-dd");
            var patternData = new
            {
                fingerprint,
                complexity = metadata.QueryComplexity.ToString(),
                rowCount = metadata.RowCount,
                dataSize = metadata.DataSizeBytes,
                timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
            };

            await _redisDatabase.ListLeftPushAsync(patternKey, JsonSerializer.Serialize(patternData));
            await _redisDatabase.KeyExpireAsync(patternKey, TimeSpan.FromDays(7)); // Keep patterns for 7 days
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to track query pattern for fingerprint: {Fingerprint}", fingerprint);
        }
    }

    private async Task UpdateAccessStatsAsync(string fingerprint)
    {
        try
        {
            var batch = _redisDatabase.CreateBatch();
            batch.HashIncrementAsync(CACHE_STATS_KEY, "hits", 1);
            batch.HashSetAsync(CACHE_STATS_KEY, "last_access", DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            await batch.ExecuteAsync();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to update access stats for fingerprint: {Fingerprint}", fingerprint);
        }
    }

    private string ComputeSHA256Hash(string input)
    {
        using var sha256 = System.Security.Cryptography.SHA256.Create();
        var hash = sha256.ComputeHash(System.Text.Encoding.UTF8.GetBytes(input));
        return Convert.ToHexString(hash).ToLower();
    }

    private string NormalizeSql(string sql)
    {
        return sql.Trim()
                  .ToUpperInvariant()
                  .Replace("\r\n", "\n")
                  .Replace("\t", " ")
                  .Replace("  ", " ");
    }

    private CacheItemPriority DetermineCachePriority(QueryCacheMetadata metadata)
    {
        if (metadata.QueryComplexity == QueryComplexity.Complex || metadata.DataSizeBytes > 1024 * 1024)
            return CacheItemPriority.High;
        if (metadata.QueryComplexity == QueryComplexity.Medium)
            return CacheItemPriority.Normal;
        return CacheItemPriority.Low;
    }

    private void RecordCacheHit(bool isMemeory)
    {
        Interlocked.Increment(ref _cacheHits);
        if (isMemeory)
            Interlocked.Increment(ref _memoryCacheHits);
        else
            Interlocked.Increment(ref _redisCacheHits);
    }

    private void RecordCacheMiss() => Interlocked.Increment(ref _cacheMisses);
    private void RecordCacheWrite() => Interlocked.Increment(ref _cacheWrites);

    private (int EntryCount, long SizeEstimate) GetMemoryCacheInfo()
    {
        // MemoryCache doesn't expose internal statistics
        return (0, 0);
    }

    private int GetRedisStatInt(Dictionary<string, string> stats, string key)
    {
        return stats.TryGetValue(key, out var value) && int.TryParse(value, out var result) ? result : 0;
    }

    private long GetRedisStatLong(Dictionary<string, string> stats, string key)
    {
        return stats.TryGetValue(key, out var value) && long.TryParse(value, out var result) ? result : 0;
    }

    private async Task<(byte[] arrowBuffer, QueryCacheMetadata metadata)> ExecuteQueryToArrowAsync(
        string sqlQuery, string? filePath, Dictionary<string, object>? parameters, CancellationToken cancellationToken)
    {
        // This would delegate to the existing DuckDB services or implement the query execution
        // For now, this is a placeholder that would integrate with your existing services
        throw new NotImplementedException("Query execution would be delegated to existing DuckDB services");
    }
}

/// <summary>
/// Configuration class for Redis-specific Arrow cache settings.
/// </summary>
public class RedisArrowCacheConfiguration : ArrowCacheConfiguration
{
    public bool UseCompression { get; set; } = true;
    public bool TrackQueryPatterns { get; set; } = true;
    public string NodeIdentifier { get; set; } = Environment.MachineName;
    public TimeSpan LockTimeout { get; set; } = TimeSpan.FromMinutes(5);
    public int MaxRetryAttempts { get; set; } = 3;
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromMilliseconds(100);
}