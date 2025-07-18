/// <summary>
/// Entry point for the ReportBuilder application - a high-performance analytics platform.
/// Configures services, middleware, and dependencies for data processing with DuckDB, Azure Storage, and streaming capabilities.
/// </summary>

using DuckDB.NET.Data;
using ReportBuilder.Service;
using ReportBuilder.Infrastructure.Middleware;
using ReportBuilder.Configuration;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Options;

// Create web application builder with command line arguments
var builder = WebApplication.CreateBuilder(args);

// Configure core ASP.NET Core services
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Configure application-specific settings from configuration
// Backpressure settings control streaming flow and prevent system overload
builder.Services.Configure<BackpressureSettings>(
    builder.Configuration.GetSection(BackpressureSettings.SectionName));

// JSON streaming settings control serialization behavior and performance
builder.Services.Configure<JsonStreamingSettings>(
    builder.Configuration.GetSection(JsonStreamingSettings.SectionName));

// Query optimization settings for automatic SQL analysis and enhancement
builder.Services.Configure<QueryOptimizationSettings>(
    builder.Configuration.GetSection(QueryOptimizationSettings.SectionName));

// Arrow cache settings for columnar result caching with TTL management
builder.Services.Configure<ArrowCacheSettings>(
    builder.Configuration.GetSection(ArrowCacheSettings.SectionName));

// Configure Azure Blob Storage client for ADLS integration
// Registered as singleton for connection pooling and optimal performance
builder.Services.AddSingleton<BlobServiceClient>(provider =>
{
    var connectionString = builder.Configuration.GetConnectionString("AzureStorage");
    return new BlobServiceClient(connectionString);
});

// Configure DuckDB in-memory database and core query services
// Each request gets a fresh in-memory database instance for isolation
builder.Services.AddScoped<DuckDBConnection>(_ => new DuckDBConnection("Data Source=:memory:"));
builder.Services.AddScoped<IDuckDbQueryService, DuckDbQueryService>();
builder.Services.AddScoped<ICsvDuckDbService, CsvDuckDbService>();
builder.Services.AddScoped<IJoinQueryService, JoinQueryService>();
builder.Services.AddScoped<IParquetQueryService, ParquetQueryService>();

// Configure advanced analytics services with concurrency management
// Concurrency limiter is singleton to maintain global state across requests
builder.Services.AddSingleton<IConcurrencyLimiterService, ConcurrencyLimiterService>();
builder.Services.AddScoped<IAnalyticsDuckDbService, AnalyticsDuckDbService>();
builder.Services.AddScoped<IAdlsArrowStreamService, AdlsArrowStreamService>();
builder.Services.AddScoped<IPaginatedQueryService, PaginatedQueryService>();
builder.Services.AddScoped<IArrowStreamService, ArrowStreamService>();
builder.Services.AddScoped<ICancellationSafeDuckDbService, CancellationSafeDuckDbService>();
builder.Services.AddScoped<IDuckDbUdfService, DuckDbUdfService>();

// Configure specialized streaming services for high-performance data delivery
// Backpressure service prevents system overload during streaming
builder.Services.AddScoped<IBackpressureStreamingService, BackpressureStreamingService>();

// Memory-safe JSON streaming for large dataset processing
builder.Services.AddScoped<IMemorySafeJsonStreamingService, MemorySafeJsonStreamingService>();

// SQL query optimization service for automatic performance enhancement
builder.Services.AddScoped<ISqlQueryOptimizerService, SqlQueryOptimizerService>();

// Arrow columnar result caching services for high-performance query result storage
// Memory cache is singleton, Arrow cache service is scoped for request-level operations
builder.Services.AddMemoryCache(options =>
{
    options.SizeLimit = 500 * 1024 * 1024; // 500MB memory cache limit
});
builder.Services.AddScoped<IArrowCacheService, ArrowCacheService>(provider =>
{
    var memoryCache = provider.GetRequiredService<IMemoryCache>();
    var logger = provider.GetRequiredService<ILogger<ArrowCacheService>>();
    var settings = provider.GetRequiredService<IOptions<ArrowCacheSettings>>().Value;
    
    var config = new ArrowCacheConfiguration
    {
        UseDistributedCache = settings.UseDistributedCache,
        DefaultTtl = settings.DefaultCacheTtl,
        MaxEntrySizeBytes = settings.MaxEntrySizeBytes,
        CacheVersion = settings.CacheVersion
    };
    
    return new ArrowCacheService(memoryCache, logger, config);
});

// Enhanced DuckDB service with integrated Arrow caching
builder.Services.AddScoped<ICachedDuckDbService, CachedDuckDbService>(provider =>
{
    var cacheService = provider.GetRequiredService<IArrowCacheService>();
    var duckDbService = provider.GetRequiredService<IDuckDbQueryService>();
    var logger = provider.GetRequiredService<ILogger<CachedDuckDbService>>();
    var settings = provider.GetRequiredService<IOptions<ArrowCacheSettings>>().Value;
    
    var config = new CachedDuckDbConfiguration
    {
        EnableCaching = settings.EnableCaching,
        DefaultCacheTtl = settings.DefaultCacheTtl,
        MinQueryLengthForCaching = settings.MinQueryLengthForCaching,
        MaxQueryLengthForCaching = settings.MaxQueryLengthForCaching,
        MaxRowsForCaching = settings.MaxRowsForCaching,
        MaxCacheSizeBytes = settings.MaxTotalCacheSizeBytes
    };
    
    return new CachedDuckDbService(cacheService, duckDbService, logger, config);
});

// Build the web application with configured services
var app = builder.Build();

// Configure the HTTP request pipeline with development tools
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

// Configure custom middleware pipeline in execution order
// Global exception handling for structured error responses
app.UseMiddleware<GlobalExceptionMiddleware>();
// Memory logging for performance monitoring and resource tracking
app.UseMiddleware<MemoryLoggingMiddleware>();
// Query optimization analysis for automatic performance enhancement
app.UseQueryOptimization();
// Concurrency throttling to prevent system overload
app.UseMiddleware<ConcurrencyThrottleMiddleware>();

app.UseHttpsRedirection();

app.MapControllers();

// Configure graceful shutdown with resource cleanup
// Ensures proper disposal of resources and completion of in-flight requests
var lifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();
lifetime.ApplicationStopping.Register(() =>
{
    var logger = app.Services.GetRequiredService<ILogger<Program>>();
    logger.LogInformation("Application is shutting down gracefully...");
    
    // Release concurrency limiter resources and complete pending operations
    var concurrencyLimiter = app.Services.GetService<IConcurrencyLimiterService>();
    if (concurrencyLimiter is IDisposable disposable)
    {
        disposable.Dispose();
    }
});

// Start the application and begin listening for requests
app.Run();
