# C# Interview Questions Based on ReportBuilder Service Classes

## Question 1: IAsyncEnumerable and Streaming Data

**Question**: Explain the benefits of using `IAsyncEnumerable<T>` over `Task<List<T>>` for data streaming. Show how you would implement memory-safe streaming of database results.

**Answer**: 
`IAsyncEnumerable<T>` provides several advantages over `Task<List<T>>`:

1. **Memory Efficiency**: Processes data one item at a time instead of loading everything into memory
2. **Better Performance**: Starts yielding results immediately without waiting for all data
3. **Backpressure Support**: Allows consumer to control the rate of data consumption
4. **Cancellation Support**: Can be cancelled at any point during enumeration

**Example Implementation**:
```csharp
private async IAsyncEnumerable<Dictionary<string, object?>> StreamQueryResultsAsync(
    DuckDBConnection connection,
    string query,
    [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
    using var command = new DuckDBCommand(query, connection);
    using var reader = await command.ExecuteReaderAsync(cancellationToken);
    
    var columnNames = new string[reader.FieldCount];
    for (int i = 0; i < reader.FieldCount; i++)
    {
        columnNames[i] = reader.GetName(i);
    }

    while (await reader.ReadAsync(cancellationToken))
    {
        // Memory check every 1000 rows
        if (rowCount % 1000 == 0)
        {
            var currentMemory = GC.GetTotalMemory(false);
            if (currentMemory > MaxMemoryBytes)
                throw new InvalidOperationException($"Memory limit exceeded");
        }

        var row = new Dictionary<string, object?>();
        for (int i = 0; i < reader.FieldCount; i++)
        {
            row[columnNames[i]] = reader.IsDBNull(i) ? null : reader.GetValue(i);
        }
        yield return row;
    }
}
```

---

## Question 2: CancellationToken and Cooperative Cancellation

**Question**: How do you implement proper cancellation support in async methods? Explain the difference between cancellation and timeout, and show examples of both.

**Answer**:
Proper cancellation requires:

1. **Accept CancellationToken**: All async methods should accept a CancellationToken parameter
2. **Check for Cancellation**: Regularly check `cancellationToken.IsCancellationRequested`
3. **Pass Token Down**: Forward the token to all async calls
4. **Handle OperationCanceledException**: Catch and handle cancellation gracefully

**Cancellation vs Timeout**:
- **Cancellation**: User-initiated cancellation (client disconnect, user action)
- **Timeout**: System-initiated cancellation after time limit

**Example Implementation**:
```csharp
public async Task<T> ExecuteQueryAsync<T>(
    string query,
    Func<DuckDBDataReader, CancellationToken, Task<T>> resultProcessor,
    CancellationToken cancellationToken = default)
{
    var queryId = Guid.NewGuid().ToString("N")[..8];
    var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    
    try
    {
        using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync(linkedCts.Token);
        
        using var command = new DuckDBCommand(query, connection);
        command.CommandTimeout = 300; // Timeout
        
        using var reader = await command.ExecuteReaderAsync(linkedCts.Token);
        return await resultProcessor(reader, linkedCts.Token);
    }
    catch (OperationCanceledException) when (linkedCts.Token.IsCancellationRequested)
    {
        _logger.LogWarning("[CANCELLED] Query {QueryId} aborted by client", queryId);
        throw new TaskCanceledException("[CANCELLED] Query aborted by client");
    }
    finally
    {
        linkedCts.Dispose();
    }
}
```

---

## Question 3: Thread-Safe Collections and Concurrency Control

**Question**: Explain the difference between `Dictionary<TKey, TValue>` and `ConcurrentDictionary<TKey, TValue>`. When would you use each, and what are the performance implications?

**Answer**:

**Dictionary<TKey, TValue>**:
- **Thread Safety**: Not thread-safe
- **Performance**: Faster for single-threaded scenarios
- **Use Case**: When access is controlled by a single thread or when external synchronization is used

**ConcurrentDictionary<TKey, TValue>**:
- **Thread Safety**: Thread-safe for all operations
- **Performance**: Slower due to synchronization overhead
- **Use Case**: When multiple threads need concurrent access

**Performance Implications**:
- ConcurrentDictionary uses lock-free algorithms for reads
- Writes may require locks on specific buckets
- Memory overhead is higher due to synchronization structures

**Example Usage**:
```csharp
public class ConcurrencyLimiterService
{
    // Thread-safe for tracking active queries across multiple threads
    private readonly ConcurrentDictionary<string, ActiveQuery> _activeQueries;
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _activeCancellations = new();
    
    public async Task<ConcurrencySlot> TryAcquireSlotAsync(string queryId, string operation, CancellationToken cancellationToken)
    {
        // Safe to access from multiple threads
        var activeQuery = new ActiveQuery { QueryId = queryId, StartTime = DateTime.UtcNow };
        _activeQueries.TryAdd(queryId, activeQuery);
        
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _activeCancellations[queryId] = linkedCts;
        
        return new ConcurrencySlot { Success = true, QueryId = queryId };
    }
}
```

---

## Question 4: Generic Methods and Delegates

**Question**: Explain how generic methods with delegate parameters provide flexibility. Show how you would design a reusable database service that can return different types based on the result processor.

**Answer**:
Generic methods with delegates provide:

1. **Type Safety**: Compile-time type checking
2. **Reusability**: Same method can process different return types
3. **Flexibility**: Custom processing logic via delegates
4. **Performance**: Avoids boxing/unboxing

**Example Implementation**:
```csharp
public interface ICancellationSafeDuckDbService
{
    Task<T> ExecuteQueryAsync<T>(
        string query,
        Func<DuckDBDataReader, CancellationToken, Task<T>> resultProcessor,
        CancellationToken cancellationToken = default);
}

// Usage examples:
// Return a list of dictionaries
var results = await service.ExecuteQueryAsync(
    "SELECT * FROM users",
    async (reader, ct) =>
    {
        var list = new List<Dictionary<string, object?>>();
        while (await reader.ReadAsync(ct))
        {
            var row = new Dictionary<string, object?>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }
            list.Add(row);
        }
        return list;
    });

// Return a scalar value
var count = await service.ExecuteQueryAsync(
    "SELECT COUNT(*) FROM users",
    async (reader, ct) =>
    {
        if (await reader.ReadAsync(ct))
            return reader.GetInt64(0);
        return 0L;
    });

// Return custom objects
var users = await service.ExecuteQueryAsync(
    "SELECT id, name, email FROM users",
    async (reader, ct) =>
    {
        var users = new List<User>();
        while (await reader.ReadAsync(ct))
        {
            users.Add(new User
            {
                Id = reader.GetInt32("id"),
                Name = reader.GetString("name"),
                Email = reader.GetString("email")
            });
        }
        return users;
    });
```

---

## Question 5: Exception Handling and Resource Management

**Question**: Demonstrate proper exception handling in async methods with resource cleanup. How do you ensure resources are properly disposed even when exceptions occur?

**Answer**:
Proper exception handling requires:

1. **Using Statements**: Automatic resource disposal
2. **Try-Finally**: Manual cleanup when using statements aren't sufficient
3. **Specific Exception Handling**: Different handling for different exception types
4. **Logging**: Comprehensive error logging for debugging

**Example Implementation**:
```csharp
public async Task StreamArrowDataAsync(
    ArrowStreamRequest request,
    Stream outputStream,
    Action<ArrowStreamPerformance>? onPerformanceUpdate = null,
    CancellationToken cancellationToken = default)
{
    var queryId = Guid.NewGuid().ToString("N")[..8];
    var tempFile = string.Empty;
    var bytesStreamed = 0L;

    try
    {
        using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync(cancellationToken);

        // Load and process data
        var loadResult = await LoadFileIntoTableAsync(connection, request, cancellationToken);
        if (!loadResult.success)
        {
            throw new InvalidOperationException($"Failed to load file: {loadResult.errorMessage}");
        }

        tempFile = Path.GetTempFileName() + ".arrow";
        
        try
        {
            var exportQuery = $"COPY ({BuildSelectQuery(request)}) TO '{tempFile}' (FORMAT 'arrow')";
            using var exportCommand = new DuckDBCommand(exportQuery, connection);
            
            await exportCommand.ExecuteNonQueryAsync(cancellationToken);

            // Stream the file
            using var fileStream = new FileStream(tempFile, FileMode.Open, FileAccess.Read);
            var buffer = new byte[BufferSize];
            int bytesRead;

            while ((bytesRead = await fileStream.ReadAsync(buffer, 0, buffer.Length, cancellationToken)) > 0)
            {
                await outputStream.WriteAsync(buffer, 0, bytesRead, cancellationToken);
                bytesStreamed += bytesRead;
            }
        }
        finally
        {
            // Always clean up temp file
            await CleanupTempFileAsync(tempFile, queryId);
        }
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        _logger.LogWarning("[CANCELLED] Arrow stream for query {QueryId} was cancelled", queryId);
        await CleanupTempFileAsync(tempFile, queryId);
        throw new TaskCanceledException("[CANCELLED] Query aborted by client");
    }
    catch (Exception ex)
    {
        _logger.LogError(ex, "[ARROW-{QueryId}] Error streaming Arrow data", queryId);
        await CleanupTempFileAsync(tempFile, queryId);
        throw;
    }
}
```

---

## Question 6: Channel-Based Producer-Consumer Pattern

**Question**: Explain how to implement backpressure using System.Threading.Channels. What are the benefits over traditional queue-based approaches?

**Answer**:
Channels provide several advantages:

1. **Built-in Backpressure**: Bounded channels automatically apply backpressure when full
2. **Async Support**: Native async/await integration
3. **Cancellation Support**: Built-in cancellation token support
4. **Memory Efficiency**: Controlled memory usage through bounds

**Example Implementation**:
```csharp
public async IAsyncEnumerable<Dictionary<string, object?>> StreamQueryResultsAsync(
    QueryRequest request,
    [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
    // Create bounded channel with specific capacity
    var channel = Channel.CreateBounded<Dictionary<string, object?>>(
        new BoundedChannelOptions(_settings.MaxChannelBufferSize)
        {
            FullMode = BoundedChannelFullMode.Wait, // Blocks producer when full
            SingleReader = true,
            SingleWriter = true
        });

    var reader = channel.Reader;
    var writer = channel.Writer;

    // Start producer task
    var producerTask = ProduceDataAsync(request, writer, cancellationToken);

    // Consume data with throttling
    await foreach (var item in reader.ReadAllAsync(cancellationToken))
    {
        // Apply throttling if enabled
        if (_settings.EnableThrottling && _settings.ThrottleDelayMs > 0)
        {
            await Task.Delay(_settings.ThrottleDelayMs, cancellationToken);
        }

        // Check queue threshold for safety
        if (reader.Count > _settings.MaxQueueThreshold)
        {
            writer.TryComplete();
            throw new OperationCanceledException("Queue threshold exceeded");
        }

        yield return item;
    }

    // Ensure producer completes
    await producerTask;
}

private async Task ProduceDataAsync(
    QueryRequest request,
    ChannelWriter<Dictionary<string, object?>> writer,
    CancellationToken cancellationToken)
{
    try
    {
        await foreach (var row in ExecuteStreamingQueryAsync(connection, request.Query, cancellationToken))
        {
            // This will block if channel is full (backpressure)
            await writer.WriteAsync(row, cancellationToken);
        }
        writer.TryComplete(); // Signal completion
    }
    catch (Exception ex)
    {
        writer.TryComplete(ex); // Signal error
    }
}
```

---

## Question 7: Memory Management and Garbage Collection

**Question**: How do you monitor and control memory usage in .NET applications? Explain when and how to use explicit garbage collection.

**Answer**:
Memory management strategies:

1. **Monitor Memory Usage**: `GC.GetTotalMemory()` for current usage
2. **Explicit GC**: Use sparingly, only when beneficial
3. **Disposal Patterns**: Proper `IDisposable` implementation
4. **Large Object Heap**: Understanding LOH behavior for large objects

**Example Implementation**:
```csharp
public async Task<AnalyticsQueryResponse> ExecuteQueryAsync(
    AnalyticsQueryRequest request, 
    CancellationToken cancellationToken = default)
{
    var initialMemory = GC.GetTotalMemory(true); // Force GC and get baseline
    var maxMemory = initialMemory;
    var peakMemory = initialMemory;
    
    try
    {
        // Process data and monitor memory
        var memoryAfterLoad = GC.GetTotalMemory(false); // Don't force GC
        maxMemory = Math.Max(maxMemory, memoryAfterLoad);
        
        // Check memory limits
        if (memoryAfterLoad > MaxMemoryBytes)
        {
            return new AnalyticsQueryResponse
            {
                Success = false,
                ErrorMessage = $"Memory limit exceeded: {memoryAfterLoad / (1024 * 1024)} MB"
            };
        }

        // Stream results with periodic memory checks
        await foreach (var row in StreamResults(cancellationToken))
        {
            // Check memory every 100 rows
            if (rowCount % 100 == 0)
            {
                var currentMemory = GC.GetTotalMemory(false);
                peakMemory = Math.Max(peakMemory, currentMemory);
                
                if (currentMemory > MaxMemoryBytes)
                    throw new InvalidOperationException("Memory limit exceeded");
            }

            // Periodic GC for long-running operations
            if (rowCount % (pageSize * 5) == 0)
            {
                GC.Collect(); // Explicit collection when safe
                GC.WaitForPendingFinalizers();
            }
            
            yield return row;
        }
    }
    finally
    {
        var finalMemory = GC.GetTotalMemory(false);
        _logger.LogInformation("Memory usage - Initial: {Initial}MB, Peak: {Peak}MB, Final: {Final}MB",
            initialMemory / (1024 * 1024), 
            peakMemory / (1024 * 1024), 
            finalMemory / (1024 * 1024));
    }
}
```

---

## Question 8: Options Pattern and Configuration

**Question**: Explain the Options pattern in .NET. How do you implement strongly-typed configuration that can be injected into services?

**Answer**:
The Options pattern provides:

1. **Strongly-Typed Configuration**: Type-safe access to configuration
2. **Dependency Injection**: Configuration injected as dependencies
3. **Validation**: Built-in validation support
4. **Hot Reload**: Configuration changes without restart (with IOptionsMonitor)

**Example Implementation**:
```csharp
// Configuration class
public class BackpressureSettings
{
    public const string SectionName = "BackpressureSettings";
    
    public int MaxChannelBufferSize { get; set; } = 1000;
    public bool EnableThrottling { get; set; } = false;
    public int ThrottleDelayMs { get; set; } = 10;
    public int MaxQueueThreshold { get; set; } = 5000;
}

// Service registration in Program.cs or Startup.cs
builder.Services.Configure<BackpressureSettings>(
    builder.Configuration.GetSection(BackpressureSettings.SectionName));

// Service that uses the configuration
public class BackpressureStreamingService : IBackpressureStreamingService
{
    private readonly ILogger<BackpressureStreamingService> _logger;
    private readonly BackpressureSettings _settings;

    public BackpressureStreamingService(
        ILogger<BackpressureStreamingService> logger,
        IOptions<BackpressureSettings> settings)
    {
        _logger = logger;
        _settings = settings.Value; // Get the actual configuration values
    }

    public async IAsyncEnumerable<Dictionary<string, object?>> StreamQueryResultsAsync(
        QueryRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Use configuration values
        var channel = Channel.CreateBounded<Dictionary<string, object?>>(
            new BoundedChannelOptions(_settings.MaxChannelBufferSize)
            {
                FullMode = BoundedChannelFullMode.Wait
            });

        if (_settings.EnableThrottling && _settings.ThrottleDelayMs > 0)
        {
            await Task.Delay(_settings.ThrottleDelayMs, cancellationToken);
        }
    }
}

// appsettings.json
{
  "BackpressureSettings": {
    "MaxChannelBufferSize": 2000,
    "EnableThrottling": true,
    "ThrottleDelayMs": 5,
    "MaxQueueThreshold": 10000
  }
}
```

---

## Question 9: Switch Expressions and Pattern Matching

**Question**: Compare traditional switch statements with C# 8.0 switch expressions. Show examples of advanced pattern matching techniques.

**Answer**:
Switch expressions provide:

1. **Conciseness**: More compact syntax
2. **Expression-Based**: Return values directly
3. **Exhaustiveness**: Compiler ensures all cases are handled
4. **Pattern Matching**: Support for complex patterns

**Traditional Switch Statement**:
```csharp
private string CreateLoadCommand(string filePath, string fileFormat)
{
    string command;
    var extension = Path.GetExtension(filePath).ToLower();
    
    switch (fileFormat.ToLower())
    {
        case "csv":
            command = $"CREATE TABLE data AS SELECT * FROM read_csv_auto('{filePath}')";
            break;
        case "parquet":
            command = $"CREATE TABLE data AS SELECT * FROM parquet_scan('{filePath}')";
            break;
        default:
            switch (extension)
            {
                case ".csv":
                    command = $"CREATE TABLE data AS SELECT * FROM read_csv_auto('{filePath}')";
                    break;
                case ".parquet":
                    command = $"CREATE TABLE data AS SELECT * FROM parquet_scan('{filePath}')";
                    break;
                default:
                    command = $"CREATE TABLE data AS SELECT * FROM read_csv_auto('{filePath}')";
                    break;
            }
            break;
    }
    
    return command;
}
```

**Modern Switch Expression**:
```csharp
private string CreateLoadCommand(string filePath, string fileFormat)
{
    var extension = Path.GetExtension(filePath).ToLower();
    var tableName = "data_table";
    
    return (fileFormat.ToLower(), extension) switch
    {
        ("csv", _) or (_, ".csv") => $"CREATE TABLE {tableName} AS SELECT * FROM read_csv_auto('{filePath}')",
        ("parquet", _) or (_, ".parquet") => $"CREATE TABLE {tableName} AS SELECT * FROM parquet_scan('{filePath}')",
        ("arrow", _) or (_, ".arrow") => $"CREATE TABLE {tableName} AS SELECT * FROM read_parquet('{filePath}')",
        _ => $"CREATE TABLE {tableName} AS SELECT * FROM read_csv_auto('{filePath}')" // Default
    };
}

// Advanced pattern matching with when guards
private string DetermineAction(FileInfo file) => file switch
{
    { Length: > 1_000_000, Extension: ".csv" } => "Large CSV file - use streaming",
    { Length: < 10_000, Extension: ".json" } => "Small JSON file - load directly",
    { Extension: ".parquet" } when file.Name.Contains("temp") => "Temporary parquet file",
    var f when f.CreationTime < DateTime.Now.AddDays(-30) => "Old file - archive",
    _ => "Unknown file type"
};
```

---

## Question 10: Extension Methods and Fluent APIs

**Question**: How do you create extension methods that enhance existing types? Design a fluent API for query building using extension methods.

**Answer**:
Extension methods provide:

1. **Enhanced APIs**: Add functionality to existing types
2. **Fluent Interfaces**: Chain method calls for readability
3. **Namespace Organization**: Group related functionality
4. **Backward Compatibility**: Enhance without modifying original types

**Example Implementation**:
```csharp
// Extension methods for the cancellation-safe service
public static class CancellationSafeDuckDbExtensions
{
    public static async Task<List<Dictionary<string, object?>>> ExecuteToListAsync(
        this ICancellationSafeDuckDbService service,
        string query,
        CancellationToken cancellationToken = default)
    {
        return await service.ExecuteQueryAsync(query, async (reader, ct) =>
        {
            var results = new List<Dictionary<string, object?>>();
            
            while (await reader.ReadAsync(ct))
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                }
                results.Add(row);
            }
            
            return results;
        }, cancellationToken);
    }

    public static async Task<object?> ExecuteScalarAsync(
        this ICancellationSafeDuckDbService service,
        string query,
        CancellationToken cancellationToken = default)
    {
        return await service.ExecuteQueryAsync(query, async (reader, ct) =>
        {
            if (await reader.ReadAsync(ct))
            {
                return reader.IsDBNull(0) ? null : reader.GetValue(0);
            }
            return null;
        }, cancellationToken);
    }

    public static async IAsyncEnumerable<Dictionary<string, object?>> ExecuteStreamAsync(
        this ICancellationSafeDuckDbService service,
        string query,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var row in service.ExecuteQueryAsync(query, async (reader, ct) =>
        {
            return StreamResults(reader, ct);
        }, cancellationToken))
        {
            yield return row;
        }
    }
}

// Fluent Query Builder Extension
public static class QueryBuilderExtensions
{
    public static QueryBuilder Select(this ICancellationSafeDuckDbService service, params string[] columns)
    {
        return new QueryBuilder(service).Select(columns);
    }
}

public class QueryBuilder
{
    private readonly ICancellationSafeDuckDbService _service;
    private readonly List<string> _selectColumns = new();
    private string _fromTable = "";
    private readonly List<string> _whereConditions = new();
    private readonly List<string> _orderByColumns = new();
    private int? _limitValue;

    public QueryBuilder(ICancellationSafeDuckDbService service)
    {
        _service = service;
    }

    public QueryBuilder Select(params string[] columns)
    {
        _selectColumns.AddRange(columns);
        return this;
    }

    public QueryBuilder From(string table)
    {
        _fromTable = table;
        return this;
    }

    public QueryBuilder Where(string condition)
    {
        _whereConditions.Add(condition);
        return this;
    }

    public QueryBuilder OrderBy(string column)
    {
        _orderByColumns.Add(column);
        return this;
    }

    public QueryBuilder Limit(int count)
    {
        _limitValue = count;
        return this;
    }

    public async Task<List<Dictionary<string, object?>>> ToListAsync(CancellationToken cancellationToken = default)
    {
        var query = BuildQuery();
        return await _service.ExecuteToListAsync(query, cancellationToken);
    }

    public async IAsyncEnumerable<Dictionary<string, object?>> ToStreamAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var query = BuildQuery();
        await foreach (var row in _service.ExecuteStreamAsync(query, cancellationToken))
        {
            yield return row;
        }
    }

    private string BuildQuery()
    {
        var selectClause = _selectColumns.Any() ? string.Join(", ", _selectColumns) : "*";
        var query = $"SELECT {selectClause} FROM {_fromTable}";
        
        if (_whereConditions.Any())
        {
            query += $" WHERE {string.Join(" AND ", _whereConditions)}";
        }
        
        if (_orderByColumns.Any())
        {
            query += $" ORDER BY {string.Join(", ", _orderByColumns)}";
        }
        
        if (_limitValue.HasValue)
        {
            query += $" LIMIT {_limitValue}";
        }
        
        return query;
    }
}

// Usage example:
var results = await duckDbService
    .Select("id", "name", "email")
    .From("users")
    .Where("age > 18")
    .Where("status = 'active'")
    .OrderBy("name")
    .Limit(100)
    .ToListAsync();

await foreach (var user in duckDbService
    .Select("*")
    .From("large_user_table")
    .Where("created_date > '2023-01-01'")
    .ToStreamAsync())
{
    Console.WriteLine($"Processing user: {user["name"]}");
}
```

These extension methods provide a fluent, readable API while maintaining the underlying performance and safety features of the cancellation-safe service.

---

## Question 11: Disposable Pattern and Resource Management

**Question**: Explain the difference between `IDisposable`, `IAsyncDisposable`, and finalizers. When should you implement each pattern?

**Answer**:

**IDisposable Pattern**:
- For synchronous resource cleanup
- Called explicitly via `using` statements or `Dispose()`
- Deterministic cleanup timing

**IAsyncDisposable Pattern**:
- For asynchronous resource cleanup
- Used with `await using` statements
- Allows async cleanup operations

**Finalizers**:
- Non-deterministic cleanup
- Called by GC when object is collected
- Should be avoided unless implementing SafeHandle

**Example Implementation**:
```csharp
public class ConcurrencyLimiterService : IConcurrencyLimiterService, IDisposable
{
    private readonly SemaphoreSlim _querySemaphore;
    private readonly Timer _healthCheckTimer;
    private bool _disposed = false;

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                // Dispose managed resources
                _healthCheckTimer?.Dispose();
                _querySemaphore?.Dispose();
                _logger.LogInformation("Concurrency limiter disposed");
            }
            
            _disposed = true;
        }
    }
}

// Async disposable example
public class AsyncResourceManager : IAsyncDisposable
{
    private readonly DuckDBConnection _connection;
    private bool _disposed = false;

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            await DisposeAsyncCore();
            Dispose(false);
            GC.SuppressFinalize(this);
        }
    }

    protected virtual async ValueTask DisposeAsyncCore()
    {
        if (_connection != null)
        {
            await _connection.CloseAsync();
            _connection.Dispose();
        }
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            _connection?.Dispose();
        }
        _disposed = true;
    }
}
```

---

## Question 12: SemaphoreSlim and Synchronization Primitives

**Question**: Compare `SemaphoreSlim`, `Mutex`, and `lock` statements. When would you use each for concurrency control?

**Answer**:

**SemaphoreSlim**:
- Allows multiple threads (up to a specified count)
- Async-compatible with `WaitAsync()`
- Useful for resource pooling and rate limiting

**Mutex**:
- Cross-process synchronization
- Only one thread allowed
- Named mutexes work across applications

**Lock Statement**:
- Single-threaded access within the same process
- Fastest for simple scenarios
- Not async-compatible

**Example Implementation**:
```csharp
public class ConcurrencyLimiterService
{
    private readonly SemaphoreSlim _querySemaphore;
    private const int MaxConcurrentQueries = 3;

    public ConcurrencyLimiterService()
    {
        // Allow up to 3 concurrent queries
        _querySemaphore = new SemaphoreSlim(MaxConcurrentQueries, MaxConcurrentQueries);
    }

    public async Task<ConcurrencySlot> TryAcquireSlotAsync(
        string queryId, 
        string operation, 
        CancellationToken cancellationToken = default)
    {
        // Try to acquire with timeout - async compatible
        var acquired = await _querySemaphore.WaitAsync(1000, cancellationToken);
        
        if (!acquired)
        {
            return new ConcurrencySlot
            {
                Success = false,
                ErrorMessage = $"Maximum concurrent queries ({MaxConcurrentQueries}) reached"
            };
        }

        return new ConcurrencySlot { Success = true, QueryId = queryId };
    }

    public async Task ReleaseSlotAsync(string queryId)
    {
        try
        {
            // Perform cleanup logic
            await CleanupQueryResources(queryId);
        }
        finally
        {
            // Always release the semaphore
            _querySemaphore.Release();
        }
    }
}

// Comparison with lock statement
public class ThreadSafeCounter
{
    private readonly object _lock = new object();
    private int _count = 0;

    public void Increment()
    {
        lock (_lock) // Simple, fast, but not async-compatible
        {
            _count++;
        }
    }

    public int GetCount()
    {
        lock (_lock)
        {
            return _count;
        }
    }
}
```

---

## Question 13: Volatile Keyword and Memory Barriers

**Question**: Explain the `volatile` keyword in C#. When is it necessary and what are its limitations?

**Answer**:

**Volatile Keyword**:
- Prevents compiler and CPU optimizations that could reorder operations
- Ensures reads and writes are not cached in CPU registers
- Provides acquire/release semantics for memory operations

**When to Use**:
- Flag variables read by multiple threads
- Simple state variables
- When you need basic memory ordering guarantees

**Limitations**:
- Only works with simple types and reference types
- Doesn't provide atomicity for complex operations
- Modern alternatives like `Interlocked` are often better

**Example Implementation**:
```csharp
public class ConcurrencyLimiterService
{
    // Volatile ensures all threads see the latest value
    private volatile bool _systemHealthy = true;
    private readonly object _statsLock = new();

    private void PerformHealthCheck(object? state)
    {
        try
        {
            var currentMemory = GC.GetTotalMemory(false);
            var memoryHealthy = currentMemory < MemoryThresholdBytes;
            var concurrencyHealthy = _activeQueries.Count <= MaxConcurrentQueries;
            
            var previousHealth = _systemHealthy;
            
            // Volatile write - visible to all threads immediately
            _systemHealthy = memoryHealthy && concurrencyHealthy;
            
            if (previousHealth != _systemHealthy)
            {
                _logger.LogWarning("System health changed: {Health}", 
                    _systemHealthy ? "HEALTHY" : "UNHEALTHY");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during health check");
            _systemHealthy = false; // Volatile write
        }
    }

    public async Task<ConcurrencySlot> TryAcquireSlotAsync(
        string queryId, 
        string operation, 
        CancellationToken cancellationToken = default)
    {
        // Volatile read - gets the latest value
        if (!_systemHealthy)
        {
            return new ConcurrencySlot
            {
                Success = false,
                ErrorMessage = "System is currently unhealthy"
            };
        }

        // Rest of the method...
    }
}

// Alternative with Interlocked for more complex scenarios
public class AtomicOperations
{
    private int _systemState = 0; // 0 = healthy, 1 = unhealthy

    public bool IsSystemHealthy => Interlocked.Read(ref _systemState) == 0;

    public void SetSystemHealth(bool healthy)
    {
        Interlocked.Exchange(ref _systemState, healthy ? 0 : 1);
    }

    public bool TrySetUnhealthy()
    {
        // Atomic compare-and-swap: only set to unhealthy if currently healthy
        return Interlocked.CompareExchange(ref _systemState, 1, 0) == 0;
    }
}
```

---

## Question 14: System.Text.Json vs Newtonsoft.Json

**Question**: Compare System.Text.Json with Newtonsoft.Json. When would you choose each, and how do you handle serialization edge cases?

**Answer**:

**System.Text.Json**:
- Higher performance and lower memory usage
- Built into .NET Core 3.0+
- More strict by default
- Limited customization compared to Newtonsoft

**Newtonsoft.Json**:
- More flexible and feature-rich
- Better backward compatibility
- Extensive customization options
- Larger ecosystem

**Example Implementation**:
```csharp
public class MemorySafeJsonStreamingService
{
    private readonly JsonSerializerOptions _serializerOptions;

    public MemorySafeJsonStreamingService(IOptions<JsonStreamingSettings> jsonSettings)
    {
        _serializerOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = jsonSettings.Value.EnablePropertyNameCaseInsensitive,
            WriteIndented = jsonSettings.Value.WriteIndented,
            MaxDepth = jsonSettings.Value.MaxDepth,
            AllowTrailingCommas = jsonSettings.Value.AllowTrailingCommas,
            ReferenceHandler = ReferenceHandler.IgnoreCycles, // Handle circular references
            DefaultIgnoreCondition = JsonIgnoreCondition.Never,
            NumberHandling = JsonNumberHandling.AllowReadingFromString
        };
    }

    private async Task<string> SerializeRowSafely(Dictionary<string, object?> row)
    {
        try
        {
            var sanitizedRow = SanitizeRowForSerialization(row);
            
            // Use streaming serialization to avoid large memory allocations
            using var memoryStream = new MemoryStream(_jsonSettings.DefaultBufferSize);
            await JsonSerializer.SerializeAsync(memoryStream, sanitizedRow, _serializerOptions);
            
            memoryStream.Position = 0;
            using var reader = new StreamReader(memoryStream);
            return await reader.ReadToEndAsync();
        }
        catch (JsonException ex) when (ex.Message.Contains("cycle"))
        {
            _logger.LogWarning("Cyclic reference detected in row");
            return JsonSerializer.Serialize(new { 
                __error = "cyclic_reference", 
                __original_keys = row.Keys 
            }, _serializerOptions);
        }
        catch (JsonException ex) when (ex.Message.Contains("too large") || ex.Message.Contains("depth"))
        {
            _logger.LogWarning("Value too large or deep for serialization");
            return JsonSerializer.Serialize(new { 
                __error = "value_too_large", 
                __row_summary = GetRowSummary(row) 
            }, _serializerOptions);
        }
        catch (OutOfMemoryException)
        {
            _logger.LogError("Out of memory during serialization");
            return JsonSerializer.Serialize(new { 
                __error = "out_of_memory", 
                __field_count = row.Count 
            }, _serializerOptions);
        }
    }

    private Dictionary<string, object?> SanitizeRowForSerialization(Dictionary<string, object?> row)
    {
        var sanitized = new Dictionary<string, object?>();
        
        foreach (var kvp in row)
        {
            try
            {
                var value = kvp.Value;
                
                // Handle large binary data
                if (value is byte[] byteArray)
                {
                    if (byteArray.Length > 1024 * 1024) // 1MB limit
                    {
                        sanitized[kvp.Key] = new { 
                            __type = "large_binary", 
                            __size = byteArray.Length 
                        };
                    }
                    else
                    {
                        sanitized[kvp.Key] = Convert.ToBase64String(byteArray);
                    }
                }
                // Handle large strings
                else if (value is string str && str.Length > 100000) // 100KB string limit
                {
                    sanitized[kvp.Key] = new { 
                        __type = "large_string", 
                        __size = str.Length, 
                        __preview = str[..Math.Min(1000, str.Length)] 
                    };
                }
                else
                {
                    sanitized[kvp.Key] = value;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Error sanitizing field {FieldName}: {Error}", kvp.Key, ex.Message);
                sanitized[kvp.Key] = new { 
                    __error = "sanitization_failed", 
                    __type = kvp.Value?.GetType().Name 
                };
            }
        }
        
        return sanitized;
    }
}
```

---

## Question 15: Regular Expressions for SQL Validation

**Question**: How do you use regular expressions to validate and sanitize SQL queries for security? What are the best practices?

**Answer**:

**SQL Validation Strategy**:
- Use regex for basic dangerous pattern detection
- Combine with parameterized queries
- Implement whitelist approach for allowed operations
- Consider using SQL parsers for complex validation

**Example Implementation**:
```csharp
public class AnalyticsDuckDbService
{
    private async Task<(bool isValid, string errorMessage)> ValidateSqlSafetyAsync(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            return (false, "SQL query cannot be empty");
        }

        // Check for dangerous operations using regex
        var dangerousPatterns = new[]
        {
            @"\bDELETE\b",           // DELETE statements
            @"\bUPDATE\b",           // UPDATE statements  
            @"\bINSERT\b",           // INSERT statements
            @"\bDROP\b",             // DROP statements
            @"\bCREATE\b",           // CREATE statements
            @"\bALTER\b",            // ALTER statements
            @"\bTRUNCATE\b",         // TRUNCATE statements
            @"\bEXEC\b",             // EXEC statements
            @"\bEXECUTE\b",          // EXECUTE statements
            @"\bxp_\w+",             // Extended stored procedures
            @"\bsp_\w+",             // System stored procedures
            @"--",                   // SQL comments
            @"/\*",                  // Block comment start
            @"\*/"                   // Block comment end
        };

        var sqlLower = sql.ToLower();
        foreach (var pattern in dangerousPatterns)
        {
            if (Regex.IsMatch(sqlLower, pattern, RegexOptions.IgnoreCase))
            {
                return (false, $"SQL contains potentially dangerous operation: {pattern}");
            }
        }

        // Additional validation for allowed patterns
        var allowedPatterns = new[]
        {
            @"^\s*SELECT\b",         // Must start with SELECT
            @"\bFROM\s+\w+",         // Must have FROM clause
        };

        // Check if SQL follows allowed patterns
        if (!Regex.IsMatch(sqlLower, allowedPatterns[0], RegexOptions.IgnoreCase))
        {
            return (false, "Only SELECT statements are allowed");
        }

        // Validate table names (alphanumeric and underscores only)
        var tablePattern = @"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\b";
        var tableMatches = Regex.Matches(sqlLower, tablePattern, RegexOptions.IgnoreCase);
        
        foreach (Match match in tableMatches)
        {
            var tableName = match.Groups[1].Value;
            if (!IsValidTableName(tableName))
            {
                return (false, $"Invalid table name: {tableName}");
            }
        }

        return await Task.FromResult((true, string.Empty));
    }

    private bool IsValidTableName(string tableName)
    {
        // Allow only alphanumeric characters and underscores
        return Regex.IsMatch(tableName, @"^[a-zA-Z_][a-zA-Z0-9_]*$");
    }
}

// Advanced SQL validation with more sophisticated patterns
public class AdvancedSqlValidator
{
    private readonly Dictionary<string, Regex> _compiledPatterns;

    public AdvancedSqlValidator()
    {
        // Pre-compile regex patterns for better performance
        _compiledPatterns = new Dictionary<string, Regex>
        {
            ["dangerous_operations"] = new Regex(
                @"\b(DELETE|UPDATE|INSERT|DROP|CREATE|ALTER|TRUNCATE|EXEC|EXECUTE)\b", 
                RegexOptions.IgnoreCase | RegexOptions.Compiled),
                
            ["sql_injection"] = new Regex(
                @"('(''|[^'])*')|(\b(OR|AND)\b\s+\b\d+\s*=\s*\d+)|(\bUNION\b)", 
                RegexOptions.IgnoreCase | RegexOptions.Compiled),
                
            ["comments"] = new Regex(
                @"(--.*$|/\*.*?\*/)", 
                RegexOptions.Multiline | RegexOptions.Compiled),
                
            ["valid_select"] = new Regex(
                @"^\s*SELECT\b.*\bFROM\b", 
                RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.Singleline)
        };
    }

    public ValidationResult ValidateQuery(string sql)
    {
        var result = new ValidationResult { IsValid = true };

        // Check for dangerous operations
        if (_compiledPatterns["dangerous_operations"].IsMatch(sql))
        {
            result.IsValid = false;
            result.Errors.Add("Query contains prohibited operations");
        }

        // Check for SQL injection patterns
        if (_compiledPatterns["sql_injection"].IsMatch(sql))
        {
            result.IsValid = false;
            result.Errors.Add("Query contains potential SQL injection patterns");
        }

        // Check for comments (which could hide malicious code)
        if (_compiledPatterns["comments"].IsMatch(sql))
        {
            result.IsValid = false;
            result.Errors.Add("Comments are not allowed in queries");
        }

        // Ensure it's a valid SELECT statement
        if (!_compiledPatterns["valid_select"].IsMatch(sql))
        {
            result.IsValid = false;
            result.Errors.Add("Only SELECT statements with FROM clause are allowed");
        }

        return result;
    }
}

public class ValidationResult
{
    public bool IsValid { get; set; }
    public List<string> Errors { get; set; } = new();
}
```

---

## Question 16: Timer and Background Processing

**Question**: Explain different timer types in .NET and their use cases. How do you implement periodic background tasks safely?

**Answer**:

**Timer Types**:
- **System.Threading.Timer**: Thread pool based, most efficient
- **System.Timers.Timer**: Event-based, good for UI scenarios  
- **System.Windows.Forms.Timer**: UI thread timer for WinForms
- **DispatcherTimer**: WPF UI thread timer

**Example Implementation**:
```csharp
public class ConcurrencyLimiterService : IDisposable
{
    private readonly Timer _healthCheckTimer;
    private const int HealthCheckIntervalMs = 5000; // 5 seconds
    private readonly object _timerLock = new object();
    private volatile bool _disposed = false;

    public ConcurrencyLimiterService(ILogger<ConcurrencyLimiterService> logger)
    {
        _logger = logger;
        
        // System.Threading.Timer - efficient for background tasks
        _healthCheckTimer = new Timer(
            PerformHealthCheck,     // Callback method
            null,                   // State object
            HealthCheckIntervalMs,  // Initial delay
            HealthCheckIntervalMs); // Recurring interval
        
        _logger.LogInformation("Health monitoring started with {Interval}ms interval", HealthCheckIntervalMs);
    }

    private void PerformHealthCheck(object? state)
    {
        // Prevent overlapping executions
        if (!Monitor.TryEnter(_timerLock))
        {
            _logger.LogWarning("Health check skipped - previous check still running");
            return;
        }

        try
        {
            if (_disposed) return;

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
                _logger.LogWarning("System health changed: {Health}. Memory: {MemoryMB} MB, Active queries: {ActiveCount}", 
                    _systemHealthy ? "HEALTHY" : "UNHEALTHY", 
                    currentMemory / (1024 * 1024), 
                    _activeQueries.Count);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during health check");
            _systemHealthy = false;
        }
        finally
        {
            Monitor.Exit(_timerLock);
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            
            // Safely dispose the timer
            _healthCheckTimer?.Dispose();
            
            // Wait for any running health check to complete
            lock (_timerLock)
            {
                _logger.LogInformation("Health monitoring stopped");
            }
        }
    }
}

// Alternative: Using hosted service for background tasks
public class HealthMonitoringService : BackgroundService
{
    private readonly ILogger<HealthMonitoringService> _logger;
    private readonly IConcurrencyLimiterService _concurrencyLimiter;
    private readonly TimeSpan _checkInterval = TimeSpan.FromSeconds(30);

    public HealthMonitoringService(
        ILogger<HealthMonitoringService> logger,
        IConcurrencyLimiterService concurrencyLimiter)
    {
        _logger = logger;
        _concurrencyLimiter = concurrencyLimiter;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Health monitoring service started");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await PerformHealthCheckAsync(stoppingToken);
                await Task.Delay(_checkInterval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Health monitoring service stopping");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in health monitoring service");
                // Continue running even if one check fails
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }
        }
    }

    private async Task PerformHealthCheckAsync(CancellationToken cancellationToken)
    {
        var isHealthy = await _concurrencyLimiter.IsSystemHealthyAsync();
        var stats = _concurrencyLimiter.GetSystemStats();
        
        if (!isHealthy)
        {
            _logger.LogWarning("System unhealthy - Active: {Active}, Memory: {MemoryMB} MB", 
                stats.ActiveConnections, stats.TotalMemoryUsage / (1024 * 1024));
        }
        else
        {
            _logger.LogDebug("System healthy - Active: {Active}, Memory: {MemoryMB} MB", 
                stats.ActiveConnections, stats.TotalMemoryUsage / (1024 * 1024));
        }
    }
}
```

---

## Question 17: String Interpolation and Performance

**Question**: Compare different string formatting approaches in C#. When should you use string interpolation vs StringBuilder vs string.Format?

**Answer**:

**String Interpolation**:
- Modern, readable syntax
- Compile-time type checking
- Good performance for simple scenarios

**StringBuilder**:
- Best for multiple concatenations
- Mutable buffer reduces allocations
- Essential for loops or complex building

**String.Format**:
- Good for localization scenarios
- Template-based approach
- Slightly slower than interpolation

**Example Implementation**:
```csharp
public class QueryLoggingService
{
    private readonly ILogger _logger;

    // String interpolation - best for simple, readable formatting
    public void LogQueryStart(string queryId, string operation)
    {
        _logger.LogInformation($"[QUERY-{queryId}] Starting operation: {operation}");
    }

    // StringBuilder - best for complex string building
    public string BuildComplexQuery(QueryRequest request)
    {
        var query = new StringBuilder(256); // Pre-allocate reasonable capacity
        
        query.Append("SELECT ");
        
        if (request.Columns?.Any() == true)
        {
            query.Append(string.Join(", ", request.Columns));
        }
        else
        {
            query.Append("*");
        }
        
        query.Append(" FROM ");
        query.Append(request.TableName);
        
        if (!string.IsNullOrEmpty(request.WhereClause))
        {
            query.Append(" WHERE ");
            query.Append(request.WhereClause);
        }
        
        if (request.OrderByColumns?.Any() == true)
        {
            query.Append(" ORDER BY ");
            query.Append(string.Join(", ", request.OrderByColumns));
        }
        
        if (request.Limit.HasValue)
        {
            query.Append(" LIMIT ");
            query.Append(request.Limit.Value);
        }
        
        return query.ToString();
    }

    // String.Format - good for templates and localization
    public string FormatErrorMessage(string template, params object[] args)
    {
        return string.Format(template, args);
    }

    // Performance comparison example
    public void StringPerformanceComparison()
    {
        const int iterations = 10000;
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // String concatenation (worst performance for multiple operations)
        stopwatch.Restart();
        var result1 = "";
        for (int i = 0; i < iterations; i++)
        {
            result1 += $"Item {i}, ";
        }
        var concatenationTime = stopwatch.ElapsedMilliseconds;

        // StringBuilder (best for multiple operations)
        stopwatch.Restart();
        var sb = new StringBuilder(iterations * 10); // Pre-allocate
        for (int i = 0; i < iterations; i++)
        {
            sb.Append($"Item {i}, ");
        }
        var result2 = sb.ToString();
        var stringBuilderTime = stopwatch.ElapsedMilliseconds;

        // String.Join (good for collections)
        stopwatch.Restart();
        var items = Enumerable.Range(0, iterations).Select(i => $"Item {i}");
        var result3 = string.Join(", ", items);
        var joinTime = stopwatch.ElapsedMilliseconds;

        _logger.LogInformation("Performance comparison - Concatenation: {Concat}ms, StringBuilder: {SB}ms, Join: {Join}ms",
            concatenationTime, stringBuilderTime, joinTime);
    }
}

// Advanced string interpolation with custom formatters
public static class StringExtensions
{
    public static string ToMemoryString(this long bytes)
    {
        return bytes switch
        {
            < 1024 => $"{bytes} B",
            < 1024 * 1024 => $"{bytes / 1024.0:F1} KB",
            < 1024 * 1024 * 1024 => $"{bytes / (1024.0 * 1024.0):F1} MB",
            _ => $"{bytes / (1024.0 * 1024.0 * 1024.0):F1} GB"
        };
    }

    public static string ToDurationString(this TimeSpan duration)
    {
        return duration.TotalDays >= 1 
            ? $"{duration.TotalDays:F1} days"
            : duration.TotalHours >= 1 
                ? $"{duration.TotalHours:F1} hours"
                : duration.TotalMinutes >= 1 
                    ? $"{duration.TotalMinutes:F1} minutes"
                    : $"{duration.TotalSeconds:F1} seconds";
    }
}

// Usage in logging
public class AnalyticsService
{
    public void LogPerformanceMetrics(long memoryUsage, TimeSpan duration, int rowCount)
    {
        // Using custom formatters with string interpolation
        _logger.LogInformation(
            "Query completed - Memory: {Memory}, Duration: {Duration}, Rows: {Rows:N0}",
            memoryUsage.ToMemoryString(),
            duration.ToDurationString(),
            rowCount);
    }
}
```

---

## Question 18: Tuple Deconstruction and Named Tuples

**Question**: Explain tuple deconstruction and named tuples in C#. How do they improve code readability and when should you use them vs custom classes?

**Answer**:

**Named Tuples**:
- Lightweight data structures
- No boxing/unboxing overhead
- Good for temporary data grouping
- Less overhead than classes

**Custom Classes**:
- Better for complex business logic
- Support inheritance and interfaces
- Better for long-term maintainability
- Can have methods and validation

**Example Implementation**:
```csharp
public class AnalyticsDuckDbService
{
    // Named tuple as return type - clear and efficient
    private async Task<(bool success, string? errorMessage, string? tempFilePath, long fileSize)> PrepareAdlsFileAsync(
        string adlsUri, 
        string? sasToken, 
        FileFormat format, 
        CancellationToken cancellationToken)
    {
        try
        {
            var blobClient = GetBlobClient(adlsUri, sasToken);
            var properties = await blobClient.GetPropertiesAsync(cancellationToken: cancellationToken);
            
            // Named tuple construction
            return (success: true, errorMessage: null, tempFilePath: adlsUri, fileSize: properties.Value.ContentLength);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error preparing ADLS file {Uri}", adlsUri);
            return (success: false, errorMessage: ex.Message, tempFilePath: null, fileSize: 0);
        }
    }

    // Tuple deconstruction in usage
    public async Task<AnalyticsQueryResponse> ExecuteQueryAsync(AnalyticsQueryRequest request, CancellationToken cancellationToken = default)
    {
        // Deconstruct the tuple with meaningful names
        var (success, errorMessage, tempFilePath, fileSize) = await PrepareAdlsFileAsync(
            request.AdlsFileUri, 
            request.SasToken, 
            request.FileFormat, 
            cancellationToken);
            
        if (!success)
        {
            return new AnalyticsQueryResponse
            {
                Success = false,
                ErrorMessage = errorMessage,
                FilePath = request.AdlsFileUri
            };
        }

        // Use the deconstructed values
        _logger.LogInformation("File prepared successfully: {FilePath}, Size: {Size} bytes", tempFilePath, fileSize);
    }

    // Multiple return patterns
    private (bool isValid, string errorMessage) ValidateSqlSafety(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            return (false, "SQL query cannot be empty");
        }

        // Validation logic...
        return (true, string.Empty);
    }

    // Tuple deconstruction with discards
    public async Task ValidateAndExecute(string sql)
    {
        var (isValid, _) = ValidateSqlSafety(sql); // Discard error message
        
        if (isValid)
        {
            await ExecuteQuery(sql);
        }
    }
}

// Advanced tuple usage with switch expressions
public class FileProcessor
{
    public string ProcessFile(string filePath)
    {
        var (extension, size) = GetFileInfo(filePath);
        
        return (extension.ToLower(), size) switch
        {
            (".csv", > 1_000_000) => "Large CSV file - use streaming processor",
            (".csv", <= 1_000_000) => "Small CSV file - load into memory",
            (".parquet", _) => "Parquet file - use columnar processor",
            (".json", > 10_000_000) => "Large JSON file - use streaming JSON processor",
            (".json", _) => "JSON file - use standard processor",
            _ => "Unknown file type"
        };
    }

    private (string extension, long size) GetFileInfo(string filePath)
    {
        var fileInfo = new FileInfo(filePath);
        return (fileInfo.Extension, fileInfo.Length);
    }
}

// Custom deconstruction for existing types
public static class Extensions
{
    // Add deconstruction to FileInfo
    public static void Deconstruct(this FileInfo fileInfo, out string name, out long size, out DateTime lastModified)
    {
        name = fileInfo.Name;
        size = fileInfo.Length;
        lastModified = fileInfo.LastWriteTime;
    }

    // Add deconstruction to KeyValuePair
    public static void Deconstruct<TKey, TValue>(this KeyValuePair<TKey, TValue> kvp, out TKey key, out TValue value)
    {
        key = kvp.Key;
        value = kvp.Value;
    }
}

// Usage of custom deconstruction
public class FileAnalyzer
{
    public void AnalyzeFiles(string directoryPath)
    {
        var directory = new DirectoryInfo(directoryPath);
        
        foreach (var file in directory.GetFiles())
        {
            // Use custom deconstruction
            var (name, size, lastModified) = file;
            
            _logger.LogInformation("File: {Name}, Size: {Size}, Modified: {Modified}", 
                name, size.ToMemoryString(), lastModified);
        }

        // Dictionary deconstruction
        var fileStats = new Dictionary<string, int>
        {
            ["csv"] = 10,
            ["parquet"] = 5,
            ["json"] = 3
        };

        foreach (var (extension, count) in fileStats)
        {
            _logger.LogInformation("Extension: {Ext}, Count: {Count}", extension, count);
        }
    }
}

// When to use tuples vs classes
public class ComparisonExample
{
    // Use tuple for simple, temporary data grouping
    private (int count, double average) CalculateStats(IEnumerable<int> values)
    {
        var list = values.ToList();
        return (list.Count, list.Average());
    }

    // Use class for complex data with behavior
    public class QueryStatistics
    {
        public int Count { get; set; }
        public double Average { get; set; }
        public TimeSpan Duration { get; set; }
        public string Status { get; set; } = "";

        public bool IsSuccessful => Status == "Success";
        public string GetSummary() => $"Processed {Count} items in {Duration.ToDurationString()}";
        
        public void Validate()
        {
            if (Count < 0) throw new InvalidOperationException("Count cannot be negative");
        }
    }
}
```

---

## Question 19: LINQ Performance and Deferred Execution

**Question**: Explain LINQ deferred execution and its performance implications. How do you optimize LINQ queries for large datasets?

**Answer**:

**Deferred Execution**:
- LINQ queries are not executed until enumerated
- Allows for query composition and optimization
- Can lead to multiple executions if not careful
- `ToList()`, `ToArray()`, `Count()` trigger immediate execution

**Performance Optimization**:
- Use `Where` before expensive operations
- Prefer `Any()` over `Count() > 0`
- Use appropriate collection types
- Consider parallel LINQ for CPU-bound operations

**Example Implementation**:
```csharp
public class AnalyticsQueryProcessor
{
    // Deferred execution example
    public IEnumerable<QueryResult> ProcessQueriesDeferred(IEnumerable<string> queries)
    {
        // This is deferred - no execution yet
        var validQueries = queries
            .Where(q => !string.IsNullOrWhiteSpace(q))
            .Where(q => q.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
            .Select(q => new QueryResult { Query = q, Status = "Pending" });

        // Execution happens here during enumeration
        return validQueries;
    }

    // Immediate execution when needed
    public List<QueryResult> ProcessQueriesImmediate(IEnumerable<string> queries)
    {
        // Force immediate execution with ToList()
        return queries
            .Where(q => !string.IsNullOrWhiteSpace(q))
            .Where(q => q.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
            .Select(q => new QueryResult { Query = q, Status = "Pending" })
            .ToList(); // Triggers execution
    }

    // Performance optimization examples
    public void OptimizedLINQExamples(List<ActiveQuery> activeQueries)
    {
        // Bad: Multiple enumerations
        var longRunningQueries = activeQueries
            .Where(q => DateTime.UtcNow - q.StartTime > TimeSpan.FromMinutes(5));
            
        if (longRunningQueries.Count() > 0) // First enumeration
        {
            foreach (var query in longRunningQueries) // Second enumeration
            {
                _logger.LogWarning("Long running query: {QueryId}", query.QueryId);
            }
        }

        // Good: Single enumeration
        var longRunningQueriesList = activeQueries
            .Where(q => DateTime.UtcNow - q.StartTime > TimeSpan.FromMinutes(5))
            .ToList(); // Single enumeration
            
        if (longRunningQueriesList.Count > 0)
        {
            foreach (var query in longRunningQueriesList)
            {
                _logger.LogWarning("Long running query: {QueryId}", query.QueryId);
            }
        }

        // Performance optimizations
        
        // Bad: Use Count() > 0
        var hasStuckQueries = activeQueries
            .Where(q => DateTime.UtcNow - q.StartTime > TimeSpan.FromMinutes(10))
            .Count() > 0;

        // Good: Use Any()
        var hasStuckQueriesOptimized = activeQueries
            .Any(q => DateTime.UtcNow - q.StartTime > TimeSpan.FromMinutes(10));

        // Bad: Multiple Where clauses can be combined
        var filteredQueries = activeQueries
            .Where(q => q.Status == "Running")
            .Where(q => q.CurrentMemoryUsage > 100_000_000)
            .Where(q => DateTime.UtcNow - q.StartTime > TimeSpan.FromMinutes(1));

        // Good: Single Where with compound condition
        var filteredQueriesOptimized = activeQueries
            .Where(q => q.Status == "Running" && 
                       q.CurrentMemoryUsage > 100_000_000 && 
                       DateTime.UtcNow - q.StartTime > TimeSpan.FromMinutes(1));

        // Parallel LINQ for CPU-intensive operations
        var expensiveCalculations = activeQueries
            .AsParallel()
            .Where(q => q.Status == "Running")
            .Select(q => CalculateComplexMetric(q)) // CPU-intensive operation
            .ToList();
    }

    // Streaming LINQ for large datasets
    public async IAsyncEnumerable<ProcessedQuery> ProcessLargeDatasetAsync(
        IAsyncEnumerable<RawQuery> rawQueries,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var rawQuery in rawQueries.WithCancellation(cancellationToken))
        {
            // Process one item at a time instead of loading all into memory
            if (IsValidQuery(rawQuery))
            {
                var processed = await ProcessQueryAsync(rawQuery, cancellationToken);
                yield return processed;
            }
        }
    }

    // Query composition with deferred execution
    public IQueryable<QueryStatistic> BuildDynamicQuery(
        IQueryable<ActiveQuery> baseQuery, 
        QueryFilter filter)
    {
        var query = baseQuery.AsQueryable();

        // Build query dynamically - still deferred
        if (!string.IsNullOrEmpty(filter.Status))
        {
            query = query.Where(q => q.Status == filter.Status);
        }

        if (filter.MinDuration.HasValue)
        {
            query = query.Where(q => DateTime.UtcNow - q.StartTime >= filter.MinDuration.Value);
        }

        if (filter.MinMemoryUsage.HasValue)
        {
            query = query.Where(q => q.CurrentMemoryUsage >= filter.MinMemoryUsage.Value);
        }

        // Project to statistics - still deferred
        return query.Select(q => new QueryStatistic
        {
            QueryId = q.QueryId,
            Duration = DateTime.UtcNow - q.StartTime,
            MemoryUsage = q.CurrentMemoryUsage,
            Status = q.Status
        });
    }

    private double CalculateComplexMetric(ActiveQuery query)
    {
        // Simulate CPU-intensive calculation
        var duration = DateTime.UtcNow - query.StartTime;
        return Math.Sqrt(query.CurrentMemoryUsage) * duration.TotalSeconds;
    }

    private bool IsValidQuery(RawQuery query) => 
        !string.IsNullOrWhiteSpace(query.Sql) && 
        query.Sql.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase);

    private async Task<ProcessedQuery> ProcessQueryAsync(RawQuery rawQuery, CancellationToken cancellationToken)
    {
        // Simulate async processing
        await Task.Delay(10, cancellationToken);
        return new ProcessedQuery { Id = rawQuery.Id, ProcessedAt = DateTime.UtcNow };
    }
}

// Supporting classes
public class QueryResult
{
    public string Query { get; set; } = "";
    public string Status { get; set; } = "";
}

public class QueryStatistic
{
    public string QueryId { get; set; } = "";
    public TimeSpan Duration { get; set; }
    public long MemoryUsage { get; set; }
    public string Status { get; set; } = "";
}

public class QueryFilter
{
    public string? Status { get; set; }
    public TimeSpan? MinDuration { get; set; }
    public long? MinMemoryUsage { get; set; }
}

public class RawQuery
{
    public string Id { get; set; } = "";
    public string Sql { get; set; } = "";
}

public class ProcessedQuery
{
    public string Id { get; set; } = "";
    public DateTime ProcessedAt { get; set; }
}
```

---

## Question 20: Nullable Reference Types

**Question**: Explain nullable reference types in C# 8.0+. How do they improve null safety and what are the best practices for adoption?

**Answer**:

**Nullable Reference Types**:
- Compile-time null safety checks
- Explicit null annotations with `?`
- Warning-based system (not runtime errors)
- Helps prevent `NullReferenceException`

**Best Practices**:
- Enable in new projects from start
- Gradually adopt in existing codebases
- Use null-conditional operators
- Implement proper validation

**Example Implementation**:
```csharp
// Enable nullable reference types at file level
#nullable enable

public class AnalyticsDuckDbService : IAnalyticsDuckDbService
{
    private readonly ILogger<AnalyticsDuckDbService> _logger;
    private readonly BlobServiceClient _blobServiceClient;
    private readonly IConcurrencyLimiterService _concurrencyLimiter;

    public AnalyticsDuckDbService(
        ILogger<AnalyticsDuckDbService> logger,
        BlobServiceClient blobServiceClient,
        IConcurrencyLimiterService concurrencyLimiter)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _blobServiceClient = blobServiceClient ?? throw new ArgumentNullException(nameof(blobServiceClient));
        _concurrencyLimiter = concurrencyLimiter ?? throw new ArgumentNullException(nameof(concurrencyLimiter));
    }

    // Nullable parameters with explicit annotation
    private async Task<(bool success, string? errorMessage, string? tempFilePath, long fileSize)> PrepareAdlsFileAsync(
        string adlsUri, 
        string? sasToken,  // Explicitly nullable
        FileFormat format, 
        CancellationToken cancellationToken)
    {
        try
        {
            var blobClient = GetBlobClient(adlsUri, sasToken);
            var properties = await blobClient.GetPropertiesAsync(cancellationToken: cancellationToken);
            
            return (true, null, adlsUri, properties.Value.ContentLength);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error preparing ADLS file {Uri}", adlsUri);
            return (false, ex.Message, null, 0);
        }
    }

    // Null checking and validation
    public async Task<AnalyticsQueryResponse> ExecuteQueryAsync(
        AnalyticsQueryRequest request, 
        CancellationToken cancellationToken = default)
    {
        // Validate non-null parameters
        ArgumentNullException.ThrowIfNull(request);
        
        if (string.IsNullOrWhiteSpace(request.SqlQuery))
        {
            throw new ArgumentException("SQL query cannot be null or empty", nameof(request));
        }

        var queryId = Guid.NewGuid().ToString();
        var warnings = new List<string>();
        ConcurrencySlot? slot = null; // Explicitly nullable

        try
        {
            // SQL validation with null handling
            var sqlValidation = await ValidateSqlSafetyAsync(request.SqlQuery);
            if (!sqlValidation.isValid)
            {
                return new AnalyticsQueryResponse
                {
                    Success = false,
                    ErrorMessage = sqlValidation.errorMessage, // Could be null, but handled by response type
                    QueryId = queryId,
                    Warnings = warnings
                };
            }

            // Null-conditional operator usage
            slot = await _concurrencyLimiter.TryAcquireSlotAsync(queryId, request.SqlQuery, cancellationToken);
            if (slot?.Success != true) // Safe navigation
            {
                return new AnalyticsQueryResponse
                {
                    Success = false,
                    ErrorMessage = slot?.ErrorMessage ?? "Failed to acquire execution slot",
                    QueryId = queryId
                };
            }

            // File preparation with nullable handling
            var (success, errorMessage, tempFilePath, fileSize) = await PrepareAdlsFileAsync(
                request.AdlsFileUri, 
                request.SasToken, // Nullable in request
                request.FileFormat, 
                cancellationToken);

            if (!success)
            {
                return new AnalyticsQueryResponse
                {
                    Success = false,
                    ErrorMessage = errorMessage ?? "Unknown error during file preparation",
                    QueryId = queryId,
                    FilePath = request.AdlsFileUri
                };
            }

            // Continue with processing...
            return new AnalyticsQueryResponse
            {
                Success = true,
                QueryId = queryId,
                FilePath = tempFilePath, // Could be null, handled by response
                FileSize = fileSize,
                Warnings = warnings
            };
        }
        finally
        {
            // Null-conditional operator in cleanup
            if (slot?.Success == true)
            {
                await _concurrencyLimiter.ReleaseSlotAsync(queryId);
            }
        }
    }

    // Helper methods with proper null handling
    private BlobClient GetBlobClient(string adlsUri, string? sasToken)
    {
        var uri = new Uri(adlsUri);
        
        // Null checking with early return
        if (!string.IsNullOrEmpty(sasToken))
        {
            var uriBuilder = new UriBuilder(uri);
            uriBuilder.Query = sasToken.TrimStart('?');
            return new BlobClient(uriBuilder.Uri);
        }

        return _blobServiceClient.GetBlobContainerClient(GetContainerName(uri))
            .GetBlobClient(GetBlobName(uri));
    }

    private static string GetContainerName(Uri uri)
    {
        var segments = uri.AbsolutePath.Trim('/').Split('/');
        return segments.FirstOrDefault() ?? throw new ArgumentException("Invalid ADLS URI format");
    }

    // Method that can return null with proper annotation
    private string? TryGetConfigurationValue(string key)
    {
        // This method might return null, explicitly annotated
        return Environment.GetEnvironmentVariable(key);
    }

    // Using null-coalescing operators
    private string GetConfigurationValueOrDefault(string key, string defaultValue)
    {
        return TryGetConfigurationValue(key) ?? defaultValue;
    }
}

// Supporting classes with nullable annotations
public class AnalyticsQueryRequest
{
    public string SqlQuery { get; set; } = "";
    public string AdlsFileUri { get; set; } = "";
    public string? SasToken { get; set; } // Explicitly nullable
    public FileFormat FileFormat { get; set; }
    public bool EnableMemoryOptimization { get; set; }
    public int? Limit { get; set; } // Nullable value type
    public int TimeoutSeconds { get; set; } = 300;
}

public class AnalyticsQueryResponse
{
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; } // Nullable for successful responses
    public string QueryId { get; set; } = "";
    public string? FilePath { get; set; } // Nullable
    public long? FileSize { get; set; } // Nullable
    public List<string> Warnings { get; set; } = new();
    public IAsyncEnumerable<Dictionary<string, object?>>? Data { get; set; } // Nullable
}

// Extension methods for null safety
public static class NullSafetyExtensions
{
    public static bool IsNullOrEmpty([NotNullWhen(false)] this string? value)
    {
        return string.IsNullOrEmpty(value);
    }

    public static bool IsNullOrWhiteSpace([NotNullWhen(false)] this string? value)
    {
        return string.IsNullOrWhiteSpace(value);
    }

    public static T ThrowIfNull<T>([NotNull] this T? value, string? paramName = null) where T : class
    {
        return value ?? throw new ArgumentNullException(paramName);
    }
}

// Migration strategy for existing code
#nullable disable // Temporarily disable for legacy code

public class LegacyService
{
    // Existing code without nullable annotations
    public string ProcessData(string input)
    {
        return input.ToUpper(); // Could throw NullReferenceException
    }
}

#nullable enable

public class ModernService
{
    // New code with proper null handling
    public string ProcessData(string? input)
    {
        return input?.ToUpper() ?? string.Empty; // Safe handling
    }
}
```

---

## Question 21: ValueTask vs Task Performance

**Question**: Explain the differences between `Task<T>` and `ValueTask<T>`. When should you use each for optimal performance?

**Answer**:

**Task<T>**:
- Reference type, always allocates on heap
- Good for async operations that usually complete asynchronously
- Standard choice for most scenarios
- Cacheable and can be stored in fields

**ValueTask<T>**:
- Value type, can avoid heap allocation
- Optimal when operations often complete synchronously
- Should not be stored in fields or accessed multiple times
- Better for high-frequency operations

**Example Implementation**:
```csharp
public interface ICacheService
{
    ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default);
    ValueTask SetAsync<T>(string key, T value, CancellationToken cancellationToken = default);
}

public class MemoryCacheService : ICacheService
{
    private readonly ConcurrentDictionary<string, object> _cache = new();
    private readonly ILogger<MemoryCacheService> _logger;

    // ValueTask for frequently synchronous operations
    public ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default)
    {
        // Often completes synchronously - perfect for ValueTask
        if (_cache.TryGetValue(key, out var value) && value is T typedValue)
        {
            _logger.LogDebug("Cache hit for key: {Key}", key);
            // Synchronous completion - no allocation
            return ValueTask.FromResult<T?>(typedValue);
        }

        _logger.LogDebug("Cache miss for key: {Key}", key);
        // Synchronous completion with null - no allocation
        return ValueTask.FromResult<T?>(default);
    }

    public ValueTask SetAsync<T>(string key, T value, CancellationToken cancellationToken = default)
    {
        try
        {
            _cache[key] = value!;
            _logger.LogDebug("Cache set for key: {Key}", key);
            
            // Synchronous completion - no allocation
            return ValueTask.CompletedTask;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error setting cache for key: {Key}", key);
            // Return faulted ValueTask
            return ValueTask.FromException(ex);
        }
    }
}

public class DatabaseCacheService : ICacheService
{
    private readonly IDbConnection _connection;
    private readonly ILogger<DatabaseCacheService> _logger;

    // Use regular Task for operations that are usually async
    public async ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default)
    {
        try
        {
            // Database operations are usually async - but we return ValueTask for interface consistency
            var query = "SELECT value FROM cache WHERE key = @key";
            var result = await _connection.QueryFirstOrDefaultAsync<string>(
                query, 
                new { key }, 
                cancellationToken: cancellationToken);

            if (result != null)
            {
                return JsonSerializer.Deserialize<T>(result);
            }

            return default;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting from database cache");
            return default;
        }
    }

    public async ValueTask SetAsync<T>(string key, T value, CancellationToken cancellationToken = default)
    {
        try
        {
            var json = JsonSerializer.Serialize(value);
            var query = "INSERT OR REPLACE INTO cache (key, value) VALUES (@key, @value)";
            
            await _connection.ExecuteAsync(
                query, 
                new { key, value = json }, 
                cancellationToken: cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error setting database cache");
            throw;
        }
    }
}

// High-performance service using ValueTask
public class HighPerformanceQueryService
{
    private readonly ICacheService _cache;
    private readonly ILogger<HighPerformanceQueryService> _logger;

    public HighPerformanceQueryService(ICacheService cache, ILogger<HighPerformanceQueryService> logger)
    {
        _cache = cache;
        _logger = logger;
    }

    // ValueTask for operations that might complete synchronously
    public async ValueTask<QueryResult> ExecuteQueryAsync(string query, CancellationToken cancellationToken = default)
    {
        var cacheKey = $"query:{HashCode.Combine(query)}";
        
        // Check cache first - might complete synchronously
        var cachedResult = await _cache.GetAsync<QueryResult>(cacheKey, cancellationToken);
        if (cachedResult != null)
        {
            _logger.LogDebug("Query cache hit");
            return cachedResult;
        }

        // Execute query - definitely async
        var result = await ExecuteQueryInternalAsync(query, cancellationToken);
        
        // Cache result - might complete synchronously
        await _cache.SetAsync(cacheKey, result, cancellationToken);
        
        return result;
    }

    // Regular Task for operations that are always async
    private async Task<QueryResult> ExecuteQueryInternalAsync(string query, CancellationToken cancellationToken)
    {
        // Simulate database operation that's always async
        await Task.Delay(100, cancellationToken);
        
        return new QueryResult
        {
            Query = query,
            ExecutedAt = DateTime.UtcNow,
            RowCount = 42
        };
    }

    // Performance comparison
    public async Task PerformanceComparisonAsync()
    {
        const int iterations = 10000;
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Test with ValueTask (cache hits - synchronous completion)
        stopwatch.Restart();
        for (int i = 0; i < iterations; i++)
        {
            await GetCachedValueAsync($"key{i % 100}"); // Many cache hits
        }
        var valueTaskTime = stopwatch.ElapsedMilliseconds;

        // Test with Task (always async)
        stopwatch.Restart();
        for (int i = 0; i < iterations; i++)
        {
            await GetUncachedValueAsync($"key{i}"); // Always async
        }
        var taskTime = stopwatch.ElapsedMilliseconds;

        _logger.LogInformation("Performance - ValueTask (cached): {ValueTask}ms, Task (uncached): {Task}ms",
            valueTaskTime, taskTime);
    }

    private ValueTask<string> GetCachedValueAsync(string key)
    {
        // Simulate cache hit - synchronous completion
        return ValueTask.FromResult($"cached_value_{key}");
    }

    private async Task<string> GetUncachedValueAsync(string key)
    {
        // Simulate database lookup - always async
        await Task.Delay(1);
        return $"db_value_{key}";
    }
}

// Best practices for ValueTask usage
public class ValueTaskBestPractices
{
    // DO: Use ValueTask for operations that often complete synchronously
    public ValueTask<bool> ValidateInputAsync(string input)
    {
        if (string.IsNullOrEmpty(input))
        {
            // Synchronous validation - no allocation
            return ValueTask.FromResult(false);
        }

        // More complex validation might be async
        return PerformComplexValidationAsync(input);
    }

    private async ValueTask<bool> PerformComplexValidationAsync(string input)
    {
        // Simulate async validation
        await Task.Delay(10);
        return input.Length > 5;
    }

    // DON'T: Store ValueTask in fields or access multiple times
    private ValueTask<string> _storedValueTask; // BAD - don't do this

    public async Task BadUsageExample()
    {
        var valueTask = GetValueAsync();
        
        // BAD - accessing ValueTask multiple times
        var result1 = await valueTask;
        var result2 = await valueTask; // This is invalid!
    }

    // DO: Consume ValueTask immediately
    public async Task GoodUsageExample()
    {
        var result = await GetValueAsync(); // Good - immediate consumption
        
        // Or store as Task if you need to access multiple times
        var task = GetValueAsync().AsTask();
        var result1 = await task;
        var result2 = await task; // Valid with Task
    }

    private ValueTask<string> GetValueAsync()
    {
        return ValueTask.FromResult("example");
    }
}

public class QueryResult
{
    public string Query { get; set; } = "";
    public DateTime ExecutedAt { get; set; }
    public int RowCount { get; set; }
}
```

---

## Question 22: ConfigureAwait and Context Switching

**Question**: Explain `ConfigureAwait(false)` and context switching in async operations. When should you use it and what are the performance implications?

**Answer**:

**ConfigureAwait(false)**:
- Tells the await not to capture the current synchronization context
- Improves performance by avoiding context switches
- Essential in library code to prevent deadlocks
- Should not be used in UI code where context is needed

**Context Switching**:
- Cost of switching between threads/contexts
- UI applications need to return to UI thread
- ASP.NET Core doesn't have synchronization context by default
- Library code should avoid capturing context

**Example Implementation**:
```csharp
public class ArrowStreamService : IArrowStreamService
{
    private readonly ILogger<ArrowStreamService> _logger;

    // Library service - always use ConfigureAwait(false)
    public async Task<(bool Success, string? ErrorMessage, ArrowStreamMetadata? Metadata)> ValidateAndPrepareQueryAsync(
        ArrowStreamRequest request, 
        CancellationToken cancellationToken = default)
    {
        try
        {
            // Validation
            var validation = ValidateRequest(request);
            if (!validation.isValid)
            {
                return (false, validation.errorMessage, null);
            }

            // File existence check - ConfigureAwait(false) for performance
            if (!File.Exists(request.FilePath))
            {
                return (false, $"File not found: {request.FilePath}", null);
            }

            using var connection = new DuckDBConnection("Data Source=:memory:");
            // Always use ConfigureAwait(false) in library code
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            // Load file with ConfigureAwait(false)
            var loadResult = await LoadFileIntoTableAsync(connection, request, cancellationToken)
                .ConfigureAwait(false);
            
            if (!loadResult.success)
            {
                return (false, loadResult.errorMessage, null);
            }

            // Get metadata with ConfigureAwait(false)
            var metadata = await GetQueryMetadataAsync(connection, request, loadResult.fileSize, cancellationToken)
                .ConfigureAwait(false);
                
            return (true, null, metadata);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error validating Arrow stream request");
            return (false, ex.Message, null);
        }
    }

    public async Task StreamArrowDataAsync(
        ArrowStreamRequest request,
        Stream outputStream,
        Action<ArrowStreamPerformance>? onPerformanceUpdate = null,
        CancellationToken cancellationToken = default)
    {
        var queryId = Guid.NewGuid().ToString("N")[..8];
        var tempFile = string.Empty;

        try
        {
            using var connection = new DuckDBConnection("Data Source=:memory:");
            // ConfigureAwait(false) to avoid context switching
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            // Load file with ConfigureAwait(false)
            var loadResult = await LoadFileIntoTableAsync(connection, request, cancellationToken)
                .ConfigureAwait(false);
            
            if (!loadResult.success)
            {
                throw new InvalidOperationException($"Failed to load file: {loadResult.errorMessage}");
            }

            tempFile = Path.GetTempFileName() + ".arrow";
            
            try
            {
                var exportQuery = $"COPY ({BuildSelectQuery(request)}) TO '{tempFile}' (FORMAT 'arrow')";
                using var exportCommand = new DuckDBCommand(exportQuery, connection);
                
                // ConfigureAwait(false) for better performance
                await exportCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                // Stream the file with ConfigureAwait(false)
                using var fileStream = new FileStream(tempFile, FileMode.Open, FileAccess.Read, 
                    FileShare.Read, BufferSize, FileOptions.SequentialScan);
                    
                var buffer = new byte[BufferSize];
                int bytesRead;

                while ((bytesRead = await fileStream.ReadAsync(buffer, 0, buffer.Length, cancellationToken)
                    .ConfigureAwait(false)) > 0)
                {
                    await outputStream.WriteAsync(buffer, 0, bytesRead, cancellationToken)
                        .ConfigureAwait(false);
                }

                await outputStream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                // Cleanup with ConfigureAwait(false)
                await CleanupTempFileAsync(tempFile, queryId).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[ARROW-{QueryId}] Error streaming Arrow data", queryId);
            await CleanupTempFileAsync(tempFile, queryId).ConfigureAwait(false);
            throw;
        }
    }

    private async Task<(bool success, string? errorMessage, long fileSize)> LoadFileIntoTableAsync(
        DuckDBConnection connection, 
        ArrowStreamRequest request, 
        CancellationToken cancellationToken)
    {
        try
        {
            var fileInfo = new FileInfo(request.FilePath);
            var fileSize = fileInfo.Length;

            var loadCommand = BuildLoadCommand(request);
            
            using var command = new DuckDBCommand(loadCommand, connection);
            command.CommandTimeout = request.TimeoutSeconds;
            
            // ConfigureAwait(false) for library code
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            return (true, null, fileSize);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading file: {FilePath}", request.FilePath);
            return (false, ex.Message, 0);
        }
    }

    private async Task CleanupTempFileAsync(string tempFile, string queryId)
    {
        if (string.IsNullOrEmpty(tempFile))
            return;

        try
        {
            if (File.Exists(tempFile))
            {
                File.Delete(tempFile);
                _logger.LogDebug("[CLEANUP] Removed temporary Arrow file for query {QueryId}: {TempFile}", 
                    queryId, tempFile);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[CLEANUP] Failed to delete temporary Arrow file for query {QueryId}: {TempFile}", 
                queryId, tempFile);
        }
        
        // Even though this is not truly async, maintain consistency
        await Task.CompletedTask.ConfigureAwait(false);
    }
}

// Comparison: With and without ConfigureAwait
public class ConfigureAwaitComparison
{
    // Bad: Without ConfigureAwait in library code
    public async Task ProcessDataBadAsync(string[] files)
    {
        foreach (var file in files)
        {
            var content = await File.ReadAllTextAsync(file); // Captures context
            var processed = await ProcessContentAsync(content); // Captures context
            await File.WriteAllTextAsync(file + ".processed", processed); // Captures context
        }
    }

    // Good: With ConfigureAwait in library code
    public async Task ProcessDataGoodAsync(string[] files)
    {
        foreach (var file in files)
        {
            var content = await File.ReadAllTextAsync(file).ConfigureAwait(false);
            var processed = await ProcessContentAsync(content).ConfigureAwait(false);
            await File.WriteAllTextAsync(file + ".processed", processed).ConfigureAwait(false);
        }
    }

    private async Task<string> ProcessContentAsync(string content)
    {
        // Simulate processing
        await Task.Delay(10).ConfigureAwait(false);
        return content.ToUpper();
    }
}

// Performance measurement
public class ContextSwitchingPerformance
{
    public async Task MeasurePerformanceAsync()
    {
        const int iterations = 1000;
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Without ConfigureAwait - context switching overhead
        stopwatch.Restart();
        for (int i = 0; i < iterations; i++)
        {
            await SimulateAsyncWorkWithContext();
        }
        var withContextTime = stopwatch.ElapsedMilliseconds;

        // With ConfigureAwait(false) - no context switching
        stopwatch.Restart();
        for (int i = 0; i < iterations; i++)
        {
            await SimulateAsyncWorkWithoutContext();
        }
        var withoutContextTime = stopwatch.ElapsedMilliseconds;

        Console.WriteLine($"With context: {withContextTime}ms, Without context: {withoutContextTime}ms");
        Console.WriteLine($"Performance improvement: {(double)withContextTime / withoutContextTime:F2}x");
    }

    private async Task SimulateAsyncWorkWithContext()
    {
        await Task.Delay(1); // Captures context
    }

    private async Task SimulateAsyncWorkWithoutContext()
    {
        await Task.Delay(1).ConfigureAwait(false); // No context capture
    }
}

// ASP.NET Controller - context is not usually needed
[ApiController]
[Route("api/[controller]")]
public class QueryController : ControllerBase
{
    private readonly IArrowStreamService _arrowStreamService;

    public QueryController(IArrowStreamService arrowStreamService)
    {
        _arrowStreamService = arrowStreamService;
    }

    [HttpPost("stream")]
    public async Task StreamQuery([FromBody] ArrowStreamRequest request)
    {
        // In ASP.NET Core, ConfigureAwait(false) is usually fine since there's no sync context
        var (success, errorMessage, metadata) = await _arrowStreamService
            .ValidateAndPrepareQueryAsync(request, HttpContext.RequestAborted)
            .ConfigureAwait(false);

        if (!success)
        {
            throw new BadRequestException(errorMessage ?? "Validation failed");
        }

        Response.ContentType = "application/vnd.apache.arrow.stream";
        
        await _arrowStreamService.StreamArrowDataAsync(
            request, 
            Response.Body, 
            performance => {
                // Performance callback doesn't need context
            },
            HttpContext.RequestAborted)
            .ConfigureAwait(false);
    }
}

// When NOT to use ConfigureAwait(false)
public partial class MainWindow : Window
{
    // UI code - DO NOT use ConfigureAwait(false)
    private async void Button_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            // Need to return to UI thread to update UI elements
            var result = await FetchDataAsync(); // Don't use ConfigureAwait(false)
            
            // This needs to run on UI thread
            ResultTextBox.Text = result;
            StatusLabel.Content = "Completed";
        }
        catch (Exception ex)
        {
            // This also needs UI thread
            MessageBox.Show($"Error: {ex.Message}");
        }
    }

    private async Task<string> FetchDataAsync()
    {
        // In library calls within UI code, still use ConfigureAwait(false)
        var httpClient = new HttpClient();
        var response = await httpClient.GetStringAsync("https://api.example.com/data")
            .ConfigureAwait(false);
        
        return response;
    }
}
```

---

## Question 23: Record Types and Immutability

**Question**: Explain C# 9.0 record types and their benefits. How do they compare to classes and structs for data modeling?

**Answer**:

**Record Types**:
- Reference types with value semantics
- Immutable by default
- Built-in equality comparison
- Automatic deconstruction support
- `with` expressions for non-destructive mutation

**Comparison**:
- **Classes**: Mutable reference types, identity equality
- **Structs**: Value types, stored on stack/inline
- **Records**: Immutable reference types, value equality

**Example Implementation**:
```csharp
// Record for query performance metrics
public record QueryPerformanceMetrics
{
    public TimeSpan QueryExecutionTime { get; init; }
    public long MemoryPeakDelta { get; init; }
    public bool OptimizationsApplied { get; init; }
    public int RowsProcessed { get; init; }
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
}

// Record with positional parameters (primary constructor)
public record ArrowColumnInfo(string Name, string DataType, bool IsNullable, int Index);

// Record with validation
public record QueryRequest
{
    public string Query { get; init; } = "";
    public string DatasetPath { get; init; } = "";
    public int PageSize { get; init; } = 1000;
    public string? Projection { get; init; }
    public string? Filter { get; init; }

    // Validation in constructor
    public QueryRequest
    {
        if (string.IsNullOrWhiteSpace(Query))
            throw new ArgumentException("Query cannot be empty", nameof(Query));
        
        if (string.IsNullOrWhiteSpace(DatasetPath))
            throw new ArgumentException("Dataset path cannot be empty", nameof(DatasetPath));
            
        if (PageSize <= 0 || PageSize > 10000)
            throw new ArgumentOutOfRangeException(nameof(PageSize), "Page size must be between 1 and 10000");
    }
}

// Record inheritance
public abstract record QueryResponse
{
    public bool Success { get; init; }
    public string? ErrorMessage { get; init; }
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
}

public record AnalyticsQueryResponse : QueryResponse
{
    public string QueryId { get; init; } = "";
    public long InitialMemoryUsage { get; init; }
    public long FinalMemoryUsage { get; init; }
    public long PeakMemoryUsage { get; init; }
    public string? QueryExecuted { get; init; }
    public string? FilePath { get; init; }
    public long? FileSize { get; init; }
    public IAsyncEnumerable<Dictionary<string, object?>>? Data { get; init; }
    public DateTime StartTime { get; init; }
    public DateTime EndTime { get; init; }
    public List<string> Warnings { get; init; } = new();
    public QueryPerformanceMetrics? Performance { get; init; }
}

// Using records in service implementation
public class AnalyticsService
{
    private readonly ILogger<AnalyticsService> _logger;

    public async Task<AnalyticsQueryResponse> ExecuteQueryAsync(QueryRequest request, CancellationToken cancellationToken = default)
    {
        var startTime = DateTime.UtcNow;
        var initialMemory = GC.GetTotalMemory(true);
        var queryId = Guid.NewGuid().ToString();

        try
        {
            // Process query...
            var data = StreamQueryResultsAsync(request, cancellationToken);
            var finalMemory = GC.GetTotalMemory(false);
            var endTime = DateTime.UtcNow;

            // Create performance metrics record
            var performance = new QueryPerformanceMetrics
            {
                QueryExecutionTime = endTime - startTime,
                MemoryPeakDelta = finalMemory - initialMemory,
                OptimizationsApplied = true,
                RowsProcessed = 0, // Would be calculated during processing
                Timestamp = endTime
            };

            // Create response record with immutable data
            return new AnalyticsQueryResponse
            {
                Success = true,
                QueryId = queryId,
                InitialMemoryUsage = initialMemory,
                FinalMemoryUsage = finalMemory,
                PeakMemoryUsage = Math.Max(initialMemory, finalMemory),
                QueryExecuted = request.Query,
                Data = data,
                StartTime = startTime,
                EndTime = endTime,
                Performance = performance
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing query {QueryId}", queryId);
            
            // Create error response using 'with' expression
            return new AnalyticsQueryResponse
            {
                Success = false,
                ErrorMessage = ex.Message,
                QueryId = queryId,
                InitialMemoryUsage = initialMemory,
                FinalMemoryUsage = GC.GetTotalMemory(false),
                StartTime = startTime,
                EndTime = DateTime.UtcNow
            };
        }
    }

    // Demonstrate 'with' expressions for updates
    public AnalyticsQueryResponse AddWarning(AnalyticsQueryResponse response, string warning)
    {
        // Non-destructive mutation using 'with'
        var updatedWarnings = response.Warnings.ToList();
        updatedWarnings.Add(warning);
        
        return response with { Warnings = updatedWarnings };
    }

    // Demonstrate record equality
    public void DemonstrateRecordEquality()
    {
        var metrics1 = new QueryPerformanceMetrics
        {
            QueryExecutionTime = TimeSpan.FromSeconds(5),
            MemoryPeakDelta = 1024,
            OptimizationsApplied = true,
            RowsProcessed = 100
        };

        var metrics2 = new QueryPerformanceMetrics
        {
            QueryExecutionTime = TimeSpan.FromSeconds(5),
            MemoryPeakDelta = 1024,
            OptimizationsApplied = true,
            RowsProcessed = 100
        };

        // Value equality - true even though they're different instances
        var areEqual = metrics1 == metrics2; // true
        var areEqualEquals = metrics1.Equals(metrics2); // true
        var hashCodesEqual = metrics1.GetHashCode() == metrics2.GetHashCode(); // true

        _logger.LogInformation("Records equal: {Equal}, Hash codes equal: {HashEqual}", 
            areEqual, hashCodesEqual);
    }

    // Demonstrate record deconstruction
    public void ProcessColumnInfo(ArrowColumnInfo columnInfo)
    {
        // Deconstruct the record
        var (name, dataType, isNullable, index) = columnInfo;
        
        _logger.LogInformation("Column {Index}: {Name} ({DataType}) - Nullable: {Nullable}", 
            index, name, dataType, isNullable);
    }

    private async IAsyncEnumerable<Dictionary<string, object?>> StreamQueryResultsAsync(
        QueryRequest request, 
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Simulate streaming results
        for (int i = 0; i < 10; i++)
        {
            yield return new Dictionary<string, object?>
            {
                ["id"] = i,
                ["name"] = $"Item {i}",
                ["timestamp"] = DateTime.UtcNow
            };
            
            await Task.Delay(10, cancellationToken);
        }
    }
}

// Record vs Class comparison
public class MutableQueryResult // Class - mutable reference type
{
    public string QueryId { get; set; } = "";
    public bool Success { get; set; }
    public DateTime Timestamp { get; set; }

    // Custom equality implementation needed if desired
    public override bool Equals(object? obj)
    {
        return obj is MutableQueryResult other &&
               QueryId == other.QueryId &&
               Success == other.Success &&
               Timestamp == other.Timestamp;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(QueryId, Success, Timestamp);
    }
}

public record ImmutableQueryResult // Record - immutable reference type
{
    public string QueryId { get; init; } = "";
    public bool Success { get; init; }
    public DateTime Timestamp { get; init; }
    
    // Equality, GetHashCode, ToString automatically generated
}

// Record struct (C# 10.0) for value types
public readonly record struct Point(int X, int Y)
{
    // Value type with record benefits
    public double DistanceFromOrigin => Math.Sqrt(X * X + Y * Y);
}

// Using record structs for performance-critical scenarios
public class PerformanceCriticalService
{
    public void ProcessPoints(IEnumerable<Point> points)
    {
        foreach (var point in points)
        {
            // No heap allocation - stored on stack
            var (x, y) = point; // Deconstruction
            var distance = point.DistanceFromOrigin;
            
            if (distance > 10.0)
            {
                // Create new point using 'with' - no heap allocation
                var scaledPoint = point with { X = x / 2, Y = y / 2 };
                ProcessScaledPoint(scaledPoint);
            }
        }
    }

    private void ProcessScaledPoint(Point point)
    {
        // Process the scaled point
    }
}

// Advanced record patterns with pattern matching
public static class QueryAnalyzer
{
    public static string AnalyzeQuery(QueryResponse response) => response switch
    {
        AnalyticsQueryResponse { Success: true, Performance.QueryExecutionTime: var time } 
            when time > TimeSpan.FromMinutes(5) => "Long-running successful query",
            
        AnalyticsQueryResponse { Success: true, PeakMemoryUsage: var memory } 
            when memory > 500_000_000 => "High memory usage query",
            
        AnalyticsQueryResponse { Success: false, ErrorMessage: var error } 
            when error?.Contains("timeout") == true => "Query timeout",
            
        AnalyticsQueryResponse { Success: false } => "Query failed",
        
        { Success: true } => "Query succeeded",
        
        _ => "Unknown query state"
    };
}
```

---

## Question 24: Structured Logging with ILogger

**Question**: Explain structured logging in .NET and best practices for using ILogger. How do you implement high-performance logging with message templates?

**Answer**:

**Structured Logging**:
- Uses message templates with named parameters
- Creates searchable, queryable log data
- Better than string concatenation for performance
- Supports different log sinks and formats

**Best Practices**:
- Use message templates, not string interpolation
- Include relevant context information
- Use appropriate log levels
- Implement scoped logging for correlated operations

**Example Implementation**:
```csharp
public class AnalyticsDuckDbService : IAnalyticsDuckDbService
{
    private readonly ILogger<AnalyticsDuckDbService> _logger;

    // High-performance logging with compile-time generated code
    public partial class AnalyticsDuckDbService
    {
        // LoggerMessage for high-performance logging
        [LoggerMessage(
            EventId = 1001,
            Level = LogLevel.Information,
            Message = "Starting analytics query {QueryId}: {Query}")]
        private partial void LogQueryStart(string queryId, string query);

        [LoggerMessage(
            EventId = 1002,
            Level = LogLevel.Warning,
            Message = "Memory limit exceeded during query {QueryId}: {MemoryMB} MB")]
        private partial void LogMemoryLimitExceeded(string queryId, long memoryMB);

        [LoggerMessage(
            EventId = 1003,
            Level = LogLevel.Information,
            Message = "Query {QueryId} completed successfully. Peak memory: {PeakMemoryMB} MB, Duration: {Duration}")]
        private partial void LogQueryCompleted(string queryId, long peakMemoryMB, TimeSpan duration);

        [LoggerMessage(
            EventId = 1004,
            Level = LogLevel.Error,
            Message = "Error executing analytics query {QueryId}")]
        private partial void LogQueryError(Exception ex, string queryId);
    }

    public async Task<AnalyticsQueryResponse> ExecuteQueryAsync(AnalyticsQueryRequest request, CancellationToken cancellationToken = default)
    {
        var queryId = Guid.NewGuid().ToString();
        var startTime = DateTime.UtcNow;
        var initialMemory = GC.GetTotalMemory(true);
        var peakMemory = initialMemory;

        // Structured logging with message template
        LogQueryStart(queryId, request.SqlQuery);

        // Create logging scope for correlation
        using var scope = _logger.BeginScope(new Dictionary<string, object>
        {
            ["QueryId"] = queryId,
            ["Operation"] = "ExecuteQuery",
            ["UserId"] = request.UserId ?? "anonymous",
            ["CorrelationId"] = Activity.Current?.Id ?? Guid.NewGuid().ToString()
        });

        try
        {
            // Process with contextual logging
            var memoryAfterLoad = GC.GetTotalMemory(false);
            peakMemory = Math.Max(peakMemory, memoryAfterLoad);

            if (memoryAfterLoad > MaxMemoryBytes)
            {
                // High-performance structured logging
                LogMemoryLimitExceeded(queryId, memoryAfterLoad / (1024 * 1024));
                
                return new AnalyticsQueryResponse
                {
                    Success = false,
                    ErrorMessage = $"Memory limit exceeded: {memoryAfterLoad / (1024 * 1024)} MB",
                    QueryId = queryId
                };
            }

            // Detailed logging with structured data
            _logger.LogDebug("Loading ADLS data for query {QueryId} from {FileUri} with format {FileFormat}",
                queryId, request.AdlsFileUri, request.FileFormat);

            var data = StreamQueryResultsAsync(connection, request.SqlQuery, request.PageSize, queryId, cancellationToken);
            var finalMemory = GC.GetTotalMemory(false);
            peakMemory = Math.Max(peakMemory, finalMemory);
            var duration = DateTime.UtcNow - startTime;

            // Structured completion logging
            LogQueryCompleted(queryId, peakMemory / (1024 * 1024), duration);

            // Performance metrics logging
            _logger.LogInformation("Query performance metrics - " +
                "InitialMemory: {InitialMemoryMB} MB, " +
                "PeakMemory: {PeakMemoryMB} MB, " +
                "FinalMemory: {FinalMemoryMB} MB, " +
                "Duration: {DurationMs} ms, " +
                "FileSize: {FileSizeKB} KB",
                initialMemory / (1024 * 1024),
                peakMemory / (1024 * 1024),
                finalMemory / (1024 * 1024),
                duration.TotalMilliseconds,
                (request.FileSize ?? 0) / 1024);

            return new AnalyticsQueryResponse
            {
                Success = true,
                QueryId = queryId,
                Data = data,
                Performance = new QueryPerformanceMetrics
                {
                    QueryExecutionTime = duration,
                    MemoryPeakDelta = peakMemory - initialMemory
                }
            };
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Query {QueryId} was cancelled by client", queryId);
            throw;
        }
        catch (Exception ex)
        {
            // High-performance error logging
            LogQueryError(ex, queryId);
            
            // Additional context in error scenarios
            _logger.LogError("Query execution context - " +
                "InitialMemory: {InitialMemoryMB} MB, " +
                "PeakMemory: {PeakMemoryMB} MB, " +
                "ElapsedTime: {ElapsedMs} ms",
                initialMemory / (1024 * 1024),
                peakMemory / (1024 * 1024),
                (DateTime.UtcNow - startTime).TotalMilliseconds);

            return new AnalyticsQueryResponse
            {
                Success = false,
                ErrorMessage = ex.Message,
                QueryId = queryId
            };
        }
    }

    // Streaming with contextual logging
    private async IAsyncEnumerable<Dictionary<string, object?>> StreamQueryResultsAsync(
        DuckDBConnection connection,
        string query,
        int pageSize,
        string queryId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var rowCount = 0;
        var batchCount = 0;
        var lastLogTime = DateTime.UtcNow;

        _logger.LogDebug("Starting query execution for {QueryId} with page size {PageSize}", queryId, pageSize);

        using var command = new DuckDBCommand(query, connection);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            var row = new Dictionary<string, object?>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            yield return row;
            rowCount++;

            // Periodic progress logging
            if (rowCount % 1000 == 0)
            {
                var currentTime = DateTime.UtcNow;
                var timeSinceLastLog = currentTime - lastLogTime;
                var currentMemory = GC.GetTotalMemory(false);

                _logger.LogDebug("Query {QueryId} progress: {RowCount} rows processed, " +
                    "Memory: {MemoryMB} MB, " +
                    "Rate: {RowsPerSecond:F1} rows/sec",
                    queryId,
                    rowCount,
                    currentMemory / (1024 * 1024),
                    1000.0 / timeSinceLastLog.TotalSeconds);

                lastLogTime = currentTime;

                // Memory warning
                if (currentMemory > MaxMemoryBytes * 0.8) // 80% threshold
                {
                    _logger.LogWarning("High memory usage during streaming for query {QueryId}: {MemoryMB} MB ({Percentage:F1}%)",
                        queryId, 
                        currentMemory / (1024 * 1024),
                        (double)currentMemory / MaxMemoryBytes * 100);
                }
            }

            if (rowCount % pageSize == 0)
            {
                batchCount++;
                _logger.LogTrace("Completed batch {BatchNumber} for query {QueryId}", batchCount, queryId);
            }
        }

        _logger.LogInformation("Streaming completed for query {QueryId}: {TotalRows} rows in {BatchCount} batches",
            queryId, rowCount, batchCount);
    }
}

// Custom logging extensions
public static class LoggingExtensions
{
    // Extension for logging with performance context
    public static void LogPerformance(this ILogger logger, 
        string operation, 
        TimeSpan duration, 
        long memoryUsed, 
        Dictionary<string, object>? additionalProperties = null)
    {
        var properties = new Dictionary<string, object>
        {
            ["Operation"] = operation,
            ["DurationMs"] = duration.TotalMilliseconds,
            ["MemoryUsedMB"] = memoryUsed / (1024 * 1024),
            ["Timestamp"] = DateTime.UtcNow
        };

        if (additionalProperties != null)
        {
            foreach (var kvp in additionalProperties)
            {
                properties[kvp.Key] = kvp.Value;
            }
        }

        using (logger.BeginScope(properties))
        {
            logger.LogInformation("Performance metric for {Operation}: {DurationMs}ms, {MemoryUsedMB}MB",
                operation, duration.TotalMilliseconds, memoryUsed / (1024 * 1024));
        }
    }

    // Extension for logging query statistics
    public static void LogQueryStatistics(this ILogger logger, QueryStatistics stats)
    {
        logger.LogInformation("Query statistics - " +
            "QueryId: {QueryId}, " +
            "RowsProcessed: {RowsProcessed}, " +
            "Duration: {DurationMs} ms, " +
            "AverageRowsPerSecond: {RowsPerSecond:F2}, " +
            "PeakMemory: {PeakMemoryMB} MB, " +
            "Success: {Success}",
            stats.QueryId,
            stats.RowsProcessed,
            stats.Duration.TotalMilliseconds,
            stats.RowsProcessed / Math.Max(stats.Duration.TotalSeconds, 0.001),
            stats.PeakMemoryUsage / (1024 * 1024),
            stats.Success);
    }
}

// Structured logging configuration
public class LoggingConfiguration
{
    public static void ConfigureLogging(IServiceCollection services, IConfiguration configuration)
    {
        services.AddLogging(builder =>
        {
            // Console logging with structured format
            builder.AddConsole(options =>
            {
                options.FormatterName = "json";
            });

            // File logging with structured format
            builder.AddFile(configuration.GetSection("Logging:File"));

            // Application Insights for production
            builder.AddApplicationInsights(configuration["ApplicationInsights:InstrumentationKey"]);

            // Custom structured logging provider
            builder.AddProvider(new StructuredFileLoggerProvider(
                Path.Combine("logs", "application-{Date}.json")));
        });

        // Configure log levels
        services.Configure<LoggerFilterOptions>(options =>
        {
            options.MinLevel = LogLevel.Debug;
            options.AddFilter("Microsoft", LogLevel.Warning);
            options.AddFilter("System", LogLevel.Warning);
            options.AddFilter("ReportBuilder", LogLevel.Debug);
        });
    }
}

// Performance comparison: LoggerMessage vs traditional logging
public class LoggingPerformanceComparison
{
    private readonly ILogger<LoggingPerformanceComparison> _logger;

    // High-performance compile-time generated logging
    [LoggerMessage(
        EventId = 2001,
        Level = LogLevel.Information,
        Message = "Processing item {ItemId} with value {Value}")]
    private partial void LogItemProcessingOptimized(int itemId, string value);

    public async Task CompareLoggingPerformance()
    {
        const int iterations = 10000;
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Traditional string interpolation (slow)
        stopwatch.Restart();
        for (int i = 0; i < iterations; i++)
        {
            _logger.LogInformation($"Processing item {i} with value {"test"}");
        }
        var traditionalTime = stopwatch.ElapsedMilliseconds;

        // Structured logging with templates (better)
        stopwatch.Restart();
        for (int i = 0; i < iterations; i++)
        {
            _logger.LogInformation("Processing item {ItemId} with value {Value}", i, "test");
        }
        var structuredTime = stopwatch.ElapsedMilliseconds;

        // High-performance LoggerMessage (fastest)
        stopwatch.Restart();
        for (int i = 0; i < iterations; i++)
        {
            LogItemProcessingOptimized(i, "test");
        }
        var optimizedTime = stopwatch.ElapsedMilliseconds;

        _logger.LogInformation("Logging performance comparison - " +
            "Traditional: {TraditionalMs}ms, " +
            "Structured: {StructuredMs}ms, " +
            "Optimized: {OptimizedMs}ms",
            traditionalTime, structuredTime, optimizedTime);
    }
}

public class QueryStatistics
{
    public string QueryId { get; set; } = "";
    public int RowsProcessed { get; set; }
    public TimeSpan Duration { get; set; }
    public long PeakMemoryUsage { get; set; }
    public bool Success { get; set; }
}
```

---

## Question 25: Reflection and Source Generators

**Question**: Compare reflection with source generators for code generation. When should you use each approach and what are the performance implications?

**Answer**:

**Reflection**:
- Runtime type inspection and invocation
- Dynamic but slow performance
- Good for frameworks and libraries
- No compile-time code generation

**Source Generators**:
- Compile-time code generation
- No runtime performance cost
- Type-safe generated code
- Available in C# 9.0+

**Example Implementation**:
```csharp
// Source Generator for high-performance logging
[Generator]
public class LoggerMessageGenerator : ISourceGenerator
{
    public void Initialize(GeneratorInitializationContext context)
    {
        context.RegisterForSyntaxNotifications(() => new LoggerMessageSyntaxReceiver());
    }

    public void Execute(GeneratorExecutionContext context)
    {
        if (!(context.SyntaxContextReceiver is LoggerMessageSyntaxReceiver receiver))
            return;

        foreach (var methodDeclaration in receiver.LoggerMethods)
        {
            var semanticModel = context.Compilation.GetSemanticModel(methodDeclaration.SyntaxTree);
            var methodSymbol = semanticModel.GetDeclaredSymbol(methodDeclaration);
            
            if (methodSymbol?.GetAttributes().Any(a => a.AttributeClass?.Name == "LoggerMessageAttribute") == true)
            {
                var generatedCode = GenerateLoggerImplementation(methodSymbol);
                context.AddSource($"{methodSymbol.ContainingType.Name}_{methodSymbol.Name}.g.cs", generatedCode);
            }
        }
    }

    private string GenerateLoggerImplementation(IMethodSymbol methodSymbol)
    {
        // Generate high-performance logging implementation
        return $@"
using Microsoft.Extensions.Logging;

namespace {methodSymbol.ContainingNamespace}
{{
    partial class {methodSymbol.ContainingType.Name}
    {{
        private static readonly Action<ILogger, {string.Join(", ", methodSymbol.Parameters.Select(p => p.Type.Name))}, Exception?> 
            {methodSymbol.Name}Delegate = LoggerMessage.Define<{string.Join(", ", methodSymbol.Parameters.Select(p => p.Type.Name))}>(
                LogLevel.Information, 
                new EventId(1001), 
                ""{GetMessageTemplate(methodSymbol)}"");

        partial void {methodSymbol.Name}({string.Join(", ", methodSymbol.Parameters.Select(p => $"{p.Type.Name} {p.Name}"))})
        {{
            {methodSymbol.Name}Delegate(_logger, {string.Join(", ", methodSymbol.Parameters.Select(p => p.Name))}, null);
        }}
    }}
}}";
    }

    private string GetMessageTemplate(IMethodSymbol methodSymbol)
    {
        // Extract message template from LoggerMessage attribute
        return "Generated message template";
    }
}

// Usage of generated logging
public partial class AnalyticsService
{
    private readonly ILogger<AnalyticsService> _logger;

    // This will be implemented by the source generator
    [LoggerMessage(Level = LogLevel.Information, Message = "Query {QueryId} started for user {UserId}")]
    partial void LogQueryStarted(string queryId, string userId);

    public async Task ProcessQuery(string queryId, string userId)
    {
        // High-performance generated logging - no reflection
        LogQueryStarted(queryId, userId);
        
        // Continue processing...
    }
}

// Reflection-based approach (traditional)
public class ReflectionBasedService
{
    private readonly ILogger<ReflectionBasedService> _logger;
    private readonly Dictionary<string, MethodInfo> _cachedMethods = new();

    public ReflectionBasedService(ILogger<ReflectionBasedService> logger)
    {
        _logger = logger;
        CacheMethods();
    }

    private void CacheMethods()
    {
        // Cache reflection calls for better performance
        var type = typeof(ReflectionBasedService);
        foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance))
        {
            if (method.GetCustomAttribute<ProcessorMethodAttribute>() != null)
            {
                _cachedMethods[method.Name] = method;
            }
        }
    }

    public async Task<object?> ExecuteMethod(string methodName, params object[] parameters)
    {
        if (!_cachedMethods.TryGetValue(methodName, out var method))
        {
            throw new ArgumentException($"Method {methodName} not found");
        }

        try
        {
            // Reflection-based invocation (slower)
            var result = method.Invoke(this, parameters);
            
            if (result is Task task)
            {
                await task;
                
                // Get result from Task<T> if applicable
                if (task.GetType().IsGenericType)
                {
                    var property = task.GetType().GetProperty("Result");
                    return property?.GetValue(task);
                }
                
                return null;
            }
            
            return result;
        }
        catch (TargetInvocationException ex)
        {
            _logger.LogError(ex.InnerException, "Error executing method {MethodName}", methodName);
            throw ex.InnerException ?? ex;
        }
    }

    [ProcessorMethod]
    public async Task<string> ProcessData(string input)
    {
        await Task.Delay(10);
        return input.ToUpper();
    }

    [ProcessorMethod]
    public async Task<int> CalculateValue(int input)
    {
        await Task.Delay(10);
        return input * 2;
    }
}

// Source generator for method dispatch (faster alternative)
[Generator]
public class MethodDispatchGenerator : ISourceGenerator
{
    public void Initialize(GeneratorInitializationContext context)
    {
        // Implementation for finding methods with ProcessorMethodAttribute
    }

    public void Execute(GeneratorExecutionContext context)
    {
        // Generate compile-time method dispatch code
        var generatedCode = @"
using System;
using System.Threading.Tasks;

namespace ReportBuilder.Generated
{
    public static class MethodDispatcher
    {
        public static async Task<object?> ExecuteMethodAsync(object instance, string methodName, params object[] parameters)
        {
            return methodName switch
            {
                ""ProcessData"" => await ((ReflectionBasedService)instance).ProcessData((string)parameters[0]),
                ""CalculateValue"" => await ((ReflectionBasedService)instance).CalculateValue((int)parameters[0]),
                _ => throw new ArgumentException($""Method {methodName} not found"")
            };
        }
    }
}";

        context.AddSource("MethodDispatcher.g.cs", generatedCode);
    }
}

// Performance comparison
public class PerformanceComparison
{
    private readonly ReflectionBasedService _reflectionService;
    private readonly ILogger<PerformanceComparison> _logger;

    public async Task ComparePerformance()
    {
        const int iterations = 10000;
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Reflection-based approach
        stopwatch.Restart();
        for (int i = 0; i < iterations; i++)
        {
            await _reflectionService.ExecuteMethod("ProcessData", $"input{i}");
        }
        var reflectionTime = stopwatch.ElapsedMilliseconds;

        // Generated code approach (would be much faster)
        stopwatch.Restart();
        for (int i = 0; i < iterations; i++)
        {
            await ReportBuilder.Generated.MethodDispatcher.ExecuteMethodAsync(_reflectionService, "ProcessData", $"input{i}");
        }
        var generatedTime = stopwatch.ElapsedMilliseconds;

        // Direct method calls (fastest)
        stopwatch.Restart();
        for (int i = 0; i < iterations; i++)
        {
            await _reflectionService.ProcessData($"input{i}");
        }
        var directTime = stopwatch.ElapsedMilliseconds;

        _logger.LogInformation("Performance comparison - " +
            "Reflection: {ReflectionMs}ms, " +
            "Generated: {GeneratedMs}ms, " +
            "Direct: {DirectMs}ms",
            reflectionTime, generatedTime, directTime);
    }
}

// Source Generator for JSON serialization
[Generator]
public class JsonSerializerGenerator : ISourceGenerator
{
    public void Initialize(GeneratorInitializationContext context)
    {
        context.RegisterForSyntaxNotifications(() => new JsonSerializableSyntaxReceiver());
    }

    public void Execute(GeneratorExecutionContext context)
    {
        // Generate compile-time JSON serialization code
        var generatedCode = @"
using System.Text.Json;

namespace ReportBuilder.Generated
{
    public static class JsonSerializers
    {
        public static string SerializeQueryRequest(QueryRequest request)
        {
            // Generated serialization code - no reflection
            var writer = new Utf8JsonWriter(stream);
            writer.WriteStartObject();
            writer.WriteString(""query"", request.Query);
            writer.WriteString(""datasetPath"", request.DatasetPath);
            writer.WriteNumber(""pageSize"", request.PageSize);
            // ... more properties
            writer.WriteEndObject();
            writer.Flush();
            return Encoding.UTF8.GetString(stream.ToArray());
        }
    }
}";

        context.AddSource("JsonSerializers.g.cs", generatedCode);
    }
}

// Attribute for marking methods
[AttributeUsage(AttributeTargets.Method)]
public class ProcessorMethodAttribute : Attribute
{
}

// When to use reflection vs source generators
public class UsageGuidelines
{
    // Use reflection for:
    // 1. Framework code that needs to work with unknown types
    // 2. Plugin architectures
    // 3. Configuration binding
    // 4. Attribute-based processing at runtime

    public void ReflectionExample()
    {
        // Good use case: Loading plugins at runtime
        var assemblies = Directory.GetFiles("plugins", "*.dll")
            .Select(Assembly.LoadFrom);

        foreach (var assembly in assemblies)
        {
            var pluginTypes = assembly.GetTypes()
                .Where(t => t.GetInterface(nameof(IPlugin)) != null);

            foreach (var type in pluginTypes)
            {
                var plugin = (IPlugin)Activator.CreateInstance(type)!;
                plugin.Execute();
            }
        }
    }

    // Use source generators for:
    // 1. High-performance scenarios
    // 2. Code that can be determined at compile time
    // 3. Reducing runtime overhead
    // 4. Type-safe code generation

    public void SourceGeneratorExample()
    {
        // Good use case: High-performance logging (LoggerMessage)
        // Good use case: JSON serialization (System.Text.Json source generators)
        // Good use case: Dependency injection container registration
        // Good use case: Mapping between types (compile-time mappers)
    }
}

// Supporting classes
public interface IPlugin
{
    void Execute();
}

public class JsonSerializableSyntaxReceiver : ISyntaxContextReceiver
{
    public void OnVisitSyntaxNode(GeneratorSyntaxContext context)
    {
        // Implementation for finding types to generate JSON serializers for
    }
}

public class LoggerMessageSyntaxReceiver : ISyntaxContextReceiver
{
    public List<MethodDeclarationSyntax> LoggerMethods { get; } = new();

    public void OnVisitSyntaxNode(GeneratorSyntaxContext context)
    {
        if (context.Node is MethodDeclarationSyntax methodDeclaration &&
            methodDeclaration.AttributeLists.Any())
        {
            LoggerMethods.Add(methodDeclaration);
        }
    }
}
```

---

## Question 26: Pattern Matching Evolution

**Question**: Explain the evolution of pattern matching in C# from C# 7.0 to C# 11.0. Show advanced pattern matching techniques and their practical applications.

**Answer**:

**Pattern Matching Evolution**:
- **C# 7.0**: Basic `is` patterns and `switch` with patterns
- **C# 8.0**: Switch expressions, property patterns, tuple patterns
- **C# 9.0**: Relational patterns, logical patterns, type patterns
- **C# 10.0**: Extended property patterns, constant patterns
- **C# 11.0**: List patterns, slice patterns, pattern span matching

**Example Implementation**:
```csharp
// Advanced pattern matching in analytics service
public class QueryAnalysisService
{
    private readonly ILogger<QueryAnalysisService> _logger;

    // C# 8.0+ Switch expressions with property patterns
    public QueryCategory CategorizeQuery(QueryRequest request) => request switch
    {
        // Property patterns with nested matching
        { Query: var q } when q.Contains("COUNT") => QueryCategory.Aggregation,
        { Query: var q } when q.Contains("GROUP BY") => QueryCategory.Grouping,
        { Query: var q } when q.Contains("ORDER BY") => QueryCategory.Sorting,
        { Query: var q } when q.Contains("JOIN") => QueryCategory.Join,
        
        // Tuple patterns for multiple properties
        { PageSize: > 10000, Query: var q } when q.Length > 1000 => QueryCategory.LargeQuery,
        { PageSize: < 100, Query: { Length: < 100 } } => QueryCategory.SmallQuery,
        
        // Range patterns (C# 9.0+)
        { PageSize: >= 1000 and <= 10000 } => QueryCategory.MediumQuery,
        
        _ => QueryCategory.Standard
    };

    // C# 9.0+ Relational and logical patterns
    public QueryOptimization GetOptimizationStrategy(QueryRequest request, QueryStatistics stats) => 
        (request, stats) switch
        {
            // Relational patterns with logical operators
            ({ PageSize: > 5000 }, { RowsProcessed: > 1000000 }) => QueryOptimization.Streaming,
            ({ Query: var q }, { Duration: > TimeSpan.FromMinutes(1) }) when q.Contains("SELECT *") => QueryOptimization.ProjectionPushdown,
            
            // Type patterns with property patterns
            ({ Filter: not null }, { Success: true }) => QueryOptimization.FilterPushdown,
            ({ Projection: not null and not "" }, _) => QueryOptimization.ColumnPruning,
            
            // Negation patterns
            (_, { Success: false, ErrorMessage: not null }) => QueryOptimization.ErrorRecovery,
            
            _ => QueryOptimization.None
        };

    // C# 10.0+ Extended property patterns
    public string AnalyzePerformance(AnalyticsQueryResponse response) => response switch
    {
        // Extended property patterns - deep navigation
        { Performance.QueryExecutionTime: > TimeSpan.FromMinutes(5) } => "Very slow query",
        { Performance.QueryExecutionTime: > TimeSpan.FromSeconds(30) and <= TimeSpan.FromMinutes(5) } => "Slow query",
        { Performance.MemoryPeakDelta: > 100_000_000 } => "High memory usage",
        
        // Nested property patterns
        { Performance: { OptimizationsApplied: true, QueryExecutionTime: < TimeSpan.FromSeconds(10) } } => "Well optimized",
        { Performance: { OptimizationsApplied: false, QueryExecutionTime: > TimeSpan.FromSeconds(30) } } => "Needs optimization",
        
        // Multiple condition patterns
        { Success: true, Warnings.Count: > 0 } => "Successful with warnings",
        { Success: false, ErrorMessage: { Length: > 0 } } => "Failed with error message",
        
        _ => "Normal performance"
    };

    // C# 11.0+ List patterns (when available)
    public string AnalyzeWarnings(List<string> warnings) => warnings switch
    {
        // List patterns
        [] => "No warnings",
        [var single] => $"Single warning: {single}",
        [var first, var second] => $"Two warnings: {first}, {second}",
        [var first, .. var rest] => $"Multiple warnings starting with: {first} (and {rest.Count} more)",
        
        // Slice patterns
        [.., var last] => $"Last warning: {last}",
        ["memory" or "Memory", ..] => "Memory-related warnings detected",
        
        _ => "Complex warning pattern"
    };

    // Advanced file processing with patterns
    public ProcessingStrategy DetermineProcessingStrategy(FileInfo file) => file switch
    {
        // Property patterns with ranges
        { Extension: ".csv", Length: < 1_000_000 } => ProcessingStrategy.InMemory,
        { Extension: ".csv", Length: >= 1_000_000 and < 100_000_000 } => ProcessingStrategy.Streaming,
        { Extension: ".csv", Length: >= 100_000_000 } => ProcessingStrategy.ChunkedProcessing,
        
        // Pattern with when guard
        { Extension: ".parquet" } when file.Name.Contains("compressed") => ProcessingStrategy.CompressedParquet,
        { Extension: ".parquet" } => ProcessingStrategy.ColumnarProcessing,
        
        // Tuple deconstruction in patterns
        var f when (f.Extension, f.Length) is (".json", > 50_000_000) => ProcessingStrategy.JsonStreaming,
        var f when (f.Extension, f.CreationTime) is (".tmp", var created) && DateTime.Now - created > TimeSpan.FromHours(1) => ProcessingStrategy.TempFileCleanup,
        
        _ => ProcessingStrategy.Default
    };

    // Exception handling with pattern matching
    public ErrorCategory CategorizeException(Exception ex) => ex switch
    {
        // Type patterns
        OperationCanceledException => ErrorCategory.Cancellation,
        TimeoutException => ErrorCategory.Timeout,
        OutOfMemoryException => ErrorCategory.ResourceExhaustion,
        
        // Property patterns with exception types
        SqlException { Number: 2 } => ErrorCategory.DatabaseTimeout,
        SqlException { Number: 18456 } => ErrorCategory.Authentication,
        SqlException { Severity: >= 20 } => ErrorCategory.FatalError,
        
        // Nested property patterns
        AggregateException { InnerExceptions: [OperationCanceledException, ..] } => ErrorCategory.Cancellation,
        AggregateException { InnerExceptions.Count: > 5 } => ErrorCategory.MultipleFailures,
        
        // String pattern matching
        { Message: var msg } when msg.Contains("timeout", StringComparison.OrdinalIgnoreCase) => ErrorCategory.Timeout,
        { Message: var msg } when msg.Contains("memory", StringComparison.OrdinalIgnoreCase) => ErrorCategory.ResourceExhaustion,
        
        _ => ErrorCategory.Unknown
    };

    // Complex business logic with pattern matching
    public async Task<ProcessingResult> ProcessQueryRequest(QueryRequest request, CancellationToken cancellationToken)
    {
        return request switch
        {
            // Early validation patterns
            { Query: null or "" } => ProcessingResult.Invalid("Query cannot be empty"),
            { DatasetPath: null or "" } => ProcessingResult.Invalid("Dataset path is required"),
            { PageSize: <= 0 or > 50000 } => ProcessingResult.Invalid("Page size must be between 1 and 50000"),
            
            // Fast path for simple queries
            { Query: var q, PageSize: < 1000 } when IsSimpleQuery(q) => 
                await ProcessSimpleQuery(request, cancellationToken),
            
            // Complex query processing
            { Query: var q, PageSize: >= 1000 } when RequiresOptimization(q) =>
                await ProcessComplexQuery(request, cancellationToken),
            
            // Default processing
            _ => await ProcessStandardQuery(request, cancellationToken)
        };
    }

    // Pattern matching with records
    public record QueryMetrics(TimeSpan Duration, long MemoryUsed, int RowCount, bool Success);

    public string ClassifyQueryPerformance(QueryMetrics metrics) => metrics switch
    {
        // Record deconstruction in patterns
        (var duration, var memory, var rows, true) when duration < TimeSpan.FromSeconds(1) => "Excellent",
        (var duration, var memory, var rows, true) when duration < TimeSpan.FromSeconds(10) && memory < 50_000_000 => "Good",
        (var duration, var memory, var rows, true) when rows > 1_000_000 && duration < TimeSpan.FromMinutes(1) => "Acceptable for large dataset",
        
        // Property patterns with records
        { Success: false } => "Failed",
        { Duration: > TimeSpan.FromMinutes(5) } => "Too slow",
        { MemoryUsed: > 500_000_000 } => "Memory intensive",
        
        _ => "Needs analysis"
    };

    // State machine using pattern matching
    public enum QueryState { Pending, Executing, Streaming, Completed, Failed }
    
    public (QueryState NewState, string Action) ProcessStateTransition(QueryState currentState, string event) =>
        (currentState, event) switch
        {
            (QueryState.Pending, "start") => (QueryState.Executing, "Begin execution"),
            (QueryState.Executing, "data_ready") => (QueryState.Streaming, "Start streaming results"),
            (QueryState.Streaming, "complete") => (QueryState.Completed, "Finalize query"),
            (QueryState.Streaming, "error") => (QueryState.Failed, "Handle error"),
            (QueryState.Executing, "cancel") => (QueryState.Failed, "Cancel execution"),
            
            // Invalid transitions
            (var state, var evt) => (state, $"Invalid transition: {evt} in state {state}")
        };

    // Helper methods
    private bool IsSimpleQuery(string query) => 
        !query.Contains("JOIN", StringComparison.OrdinalIgnoreCase) &&
        !query.Contains("GROUP BY", StringComparison.OrdinalIgnoreCase) &&
        !query.Contains("ORDER BY", StringComparison.OrdinalIgnoreCase);

    private bool RequiresOptimization(string query) =>
        query.Contains("SELECT *", StringComparison.OrdinalIgnoreCase) ||
        query.Length > 1000;

    private async Task<ProcessingResult> ProcessSimpleQuery(QueryRequest request, CancellationToken cancellationToken)
    {
        // Simple query processing logic
        await Task.Delay(100, cancellationToken);
        return ProcessingResult.Success("Simple query processed");
    }

    private async Task<ProcessingResult> ProcessComplexQuery(QueryRequest request, CancellationToken cancellationToken)
    {
        // Complex query processing logic
        await Task.Delay(1000, cancellationToken);
        return ProcessingResult.Success("Complex query processed");
    }

    private async Task<ProcessingResult> ProcessStandardQuery(QueryRequest request, CancellationToken cancellationToken)
    {
        // Standard query processing logic
        await Task.Delay(500, cancellationToken);
        return ProcessingResult.Success("Standard query processed");
    }
}

// Supporting types and enums
public enum QueryCategory
{
    Standard, Aggregation, Grouping, Sorting, Join, LargeQuery, MediumQuery, SmallQuery
}

public enum QueryOptimization
{
    None, Streaming, ProjectionPushdown, FilterPushdown, ColumnPruning, ErrorRecovery
}

public enum ProcessingStrategy
{
    Default, InMemory, Streaming, ChunkedProcessing, CompressedParquet, 
    ColumnarProcessing, JsonStreaming, TempFileCleanup
}

public enum ErrorCategory
{
    Unknown, Cancellation, Timeout, ResourceExhaustion, DatabaseTimeout, 
    Authentication, FatalError, MultipleFailures
}

public record ProcessingResult(bool Success, string Message)
{
    public static ProcessingResult Success(string message) => new(true, message);
    public static ProcessingResult Invalid(string message) => new(false, message);
}

// Pattern matching performance considerations
public class PatternMatchingPerformance
{
    // Efficient pattern matching - ordered by frequency
    public string OptimizedQueryClassification(QueryRequest request) => request switch
    {
        // Most common cases first for better performance
        { Query: var q } when q.StartsWith("SELECT") && q.Length < 200 => "Simple select",
        { Query: var q } when q.StartsWith("SELECT") => "Complex select",
        { Query: var q } when q.StartsWith("INSERT") => "Insert operation",
        { Query: var q } when q.StartsWith("UPDATE") => "Update operation",
        { Query: var q } when q.StartsWith("DELETE") => "Delete operation",
        _ => "Unknown query type"
    };

    // Less efficient - complex patterns that are expensive to evaluate
    public string ExpensivePatternMatching(QueryRequest request) => request switch
    {
        // Expensive: Regular expressions in patterns
        { Query: var q } when Regex.IsMatch(q, @"SELECT\s+\*\s+FROM\s+\w+") => "Select all pattern",
        
        // Expensive: Complex calculations in guards
        { Query: var q } when CalculateComplexity(q) > 100 => "High complexity",
        
        // Better: Simple property checks
        { Query: { Length: > 1000 } } => "Long query",
        
        _ => "Standard query"
    };

    private int CalculateComplexity(string query)
    {
        // Expensive calculation - avoid in pattern guards
        return query.Split(' ').Length * query.Count(c => c == '(');
    }
}
```

This completes the 30 comprehensive C# interview questions covering advanced concepts found in your ReportBuilder service classes. Each question includes detailed explanations and practical code examples that demonstrate real-world usage of these concepts.