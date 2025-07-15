namespace ReportBuilder.Configuration;

/// <summary>
/// Configuration settings for backpressure management and flow control in streaming operations.
/// Controls buffering, throttling, and queue management to prevent system overload.
/// </summary>
public class BackpressureSettings
{
    /// <summary>
    /// Configuration section name for binding from appsettings.json.
    /// </summary>
    public const string SectionName = "BackpressureSettings";

    /// <summary>
    /// Gets or sets the maximum size of the channel buffer for bounded channel operations.
    /// Higher values allow more buffering but consume more memory. Default: 10000.
    /// </summary>
    public int MaxChannelBufferSize { get; set; } = 10000;
    
    /// <summary>
    /// Gets or sets the delay in milliseconds for throttling streaming operations.
    /// Used to control the rate of data delivery to prevent overwhelming consumers. Default: 10ms.
    /// </summary>
    public int ThrottleDelayMs { get; set; } = 10;
    
    /// <summary>
    /// Gets or sets whether to flush the output stream after each row.
    /// Improves real-time delivery but may impact performance for large datasets. Default: true.
    /// </summary>
    public bool FlushAfterEachRow { get; set; } = true;
    
    /// <summary>
    /// Gets or sets whether throttling is enabled for streaming operations.
    /// When disabled, data is streamed as fast as possible. Default: true.
    /// </summary>
    public bool EnableThrottling { get; set; } = true;
    
    /// <summary>
    /// Gets or sets the maximum queue threshold before triggering backpressure protection.
    /// When exceeded, the operation may be cancelled to prevent memory issues. Default: 10000.
    /// </summary>
    public int MaxQueueThreshold { get; set; } = 10000;
}

/// <summary>
/// Configuration settings for JSON streaming operations and serialization behavior.
/// Controls JSON serialization performance, formatting, and buffer management.
/// </summary>
public class JsonStreamingSettings
{
    /// <summary>
    /// Configuration section name for binding from appsettings.json.
    /// </summary>
    public const string SectionName = "JsonStreamingSettings";

    /// <summary>
    /// Gets or sets whether to measure and log JSON serialization latency.
    /// Useful for performance monitoring but may add overhead. Default: true.
    /// </summary>
    public bool MeasureSerializationLatency { get; set; } = true;
    
    /// <summary>
    /// Gets or sets whether JSON property name matching is case insensitive.
    /// Improves compatibility but may impact performance. Default: true.
    /// </summary>
    public bool EnablePropertyNameCaseInsensitive { get; set; } = true;
    
    /// <summary>
    /// Gets or sets whether JSON output should be indented for readability.
    /// Improves readability but increases payload size. Default: false for streaming efficiency.
    /// </summary>
    public bool WriteIndented { get; set; } = false;
    
    /// <summary>
    /// Gets or sets the maximum depth for JSON object nesting.
    /// Prevents stack overflow from deeply nested objects. Default: 64.
    /// </summary>
    public int MaxDepth { get; set; } = 64;
    
    /// <summary>
    /// Gets or sets whether trailing commas are allowed in JSON parsing.
    /// Improves compatibility with JavaScript-style JSON. Default: true.
    /// </summary>
    public bool AllowTrailingCommas { get; set; } = true;
    
    /// <summary>
    /// Gets or sets the default buffer size for JSON streaming operations in bytes.
    /// Larger buffers improve performance but consume more memory. Default: 16KB.
    /// </summary>
    public int DefaultBufferSize { get; set; } = 16384;
}

/// <summary>
/// Configuration settings for Arrow columnar result caching functionality.
/// Controls cache behavior, TTL management, and performance optimization settings.
/// </summary>
public class ArrowCacheSettings
{
    /// <summary>
    /// Configuration section name for binding from appsettings.json.
    /// </summary>
    public const string SectionName = "ArrowCacheSettings";

    /// <summary>
    /// Gets or sets whether Arrow result caching is enabled.
    /// When disabled, all queries execute fresh without caching. Default: true.
    /// </summary>
    public bool EnableCaching { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to use distributed cache (Redis) in addition to memory cache.
    /// Enables cache sharing across multiple application instances. Default: false.
    /// </summary>
    public bool UseDistributedCache { get; set; } = false;

    /// <summary>
    /// Gets or sets the default time-to-live for cached query results.
    /// How long results remain in cache before expiring. Default: 30 minutes.
    /// </summary>
    public TimeSpan DefaultCacheTtl { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// Gets or sets the maximum size in bytes for a single cache entry.
    /// Large results exceeding this limit will not be cached. Default: 100MB.
    /// </summary>
    public long MaxEntrySizeBytes { get; set; } = 100 * 1024 * 1024;

    /// <summary>
    /// Gets or sets the maximum number of rows to cache from streaming queries.
    /// Prevents memory issues with very large result sets. Default: 100,000.
    /// </summary>
    public int MaxRowsForCaching { get; set; } = 100000;

    /// <summary>
    /// Gets or sets the minimum query length in characters to consider for caching.
    /// Very short queries may not benefit from caching. Default: 50.
    /// </summary>
    public int MinQueryLengthForCaching { get; set; } = 50;

    /// <summary>
    /// Gets or sets the maximum query length in characters to consider for caching.
    /// Very complex queries may be too dynamic to cache effectively. Default: 10,000.
    /// </summary>
    public int MaxQueryLengthForCaching { get; set; } = 10000;

    /// <summary>
    /// Gets or sets the cache version string for cache invalidation.
    /// Increment to invalidate all existing cache entries. Default: "v1.0".
    /// </summary>
    public string CacheVersion { get; set; } = "v1.0";

    /// <summary>
    /// Gets or sets whether to use compression for cached Arrow buffers in Redis.
    /// Reduces storage space but adds CPU overhead. Default: true.
    /// </summary>
    public bool UseCompression { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to track query patterns for analytics.
    /// Stores anonymized query statistics for optimization insights. Default: true.
    /// </summary>
    public bool TrackQueryPatterns { get; set; } = true;

    /// <summary>
    /// Gets or sets the maximum total cache size in bytes for memory cache.
    /// Prevents excessive memory usage by the cache. Default: 500MB.
    /// </summary>
    public long MaxTotalCacheSizeBytes { get; set; } = 500 * 1024 * 1024;

    /// <summary>
    /// Gets or sets the cache cleanup interval for expired entries.
    /// How often to check for and remove expired cache entries. Default: 5 minutes.
    /// </summary>
    public TimeSpan CacheCleanupInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Gets or sets SQL keywords that indicate non-deterministic queries.
    /// Queries containing these keywords will not be cached. Default: NOW, RANDOM, UUID, etc.
    /// </summary>
    public List<string> NonDeterministicKeywords { get; set; } = new()
    {
        "NOW()", "RANDOM()", "UUID()", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME"
    };

    /// <summary>
    /// Gets or sets cache priority rules based on query characteristics.
    /// Determines cache eviction priority and storage allocation.
    /// </summary>
    public CachePriorityRules PriorityRules { get; set; } = new();
}

/// <summary>
/// Cache priority rules for determining storage allocation and eviction order.
/// </summary>
public class CachePriorityRules
{
    /// <summary>
    /// Gets or sets the minimum data size in bytes for high-priority caching.
    /// Large result sets get higher priority due to expensive recomputation. Default: 1MB.
    /// </summary>
    public long HighPriorityMinSizeBytes { get; set; } = 1024 * 1024;

    /// <summary>
    /// Gets or sets the minimum execution time for high-priority caching.
    /// Slow queries get higher priority due to expensive recomputation. Default: 5 seconds.
    /// </summary>
    public TimeSpan HighPriorityMinExecutionTime { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets complex query keywords that increase cache priority.
    /// Queries with these operations benefit most from caching. Default: JOIN, GROUP BY, etc.
    /// </summary>
    public List<string> HighPriorityKeywords { get; set; } = new()
    {
        "JOIN", "GROUP BY", "ORDER BY", "WINDOW", "WITH", "SUBQUERY", "UNION"
    };

    /// <summary>
    /// Gets or sets the TTL multiplier for high-priority cache entries.
    /// High-priority entries are cached longer. Default: 2.0 (double TTL).
    /// </summary>
    public double HighPriorityTtlMultiplier { get; set; } = 2.0;

    /// <summary>
    /// Gets or sets the TTL multiplier for low-priority cache entries.
    /// Low-priority entries expire sooner. Default: 0.5 (half TTL).
    /// </summary>
    public double LowPriorityTtlMultiplier { get; set; } = 0.5;
}