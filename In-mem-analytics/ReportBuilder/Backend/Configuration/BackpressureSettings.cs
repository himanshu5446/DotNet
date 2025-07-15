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