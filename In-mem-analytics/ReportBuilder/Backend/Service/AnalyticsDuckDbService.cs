using DuckDB.NET.Data;
using Azure.Storage.Blobs;
using ReportBuilder.Request;
using ReportBuilder.Response;
using System.Data;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

namespace ReportBuilder.Service;

/// <summary>
/// Interface for advanced analytics operations using DuckDB with Azure Data Lake Storage integration.
/// Provides high-performance analytics with memory management, concurrency control, and join operations.
/// </summary>
public interface IAnalyticsDuckDbService
{
    /// <summary>
    /// Executes an analytics query on ADLS data with memory optimization and concurrency control.
    /// </summary>
    /// <param name="request">Analytics query request with ADLS file URI and SQL query</param>
    /// <param name="cancellationToken">Token for cancelling the query execution</param>
    /// <returns>Analytics query response with streaming results and performance metrics</returns>
    Task<AnalyticsQueryResponse> ExecuteQueryAsync(AnalyticsQueryRequest request, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Executes a JOIN query between multiple ADLS datasets with advanced optimization.
    /// </summary>
    /// <param name="request">Analytics JOIN request with multiple data sources</param>
    /// <param name="cancellationToken">Token for cancelling the join operation</param>
    /// <returns>Analytics query response with joined results and performance statistics</returns>
    Task<AnalyticsQueryResponse> ExecuteJoinQueryAsync(AnalyticsJoinRequest request, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Retrieves schema information from ADLS data sources for query planning.
    /// </summary>
    /// <param name="request">Schema request specifying the data source to analyze</param>
    /// <param name="cancellationToken">Token for cancelling the schema retrieval</param>
    /// <returns>Schema response with column information and metadata</returns>
    Task<SchemaResponse> GetSchemaAsync(SchemaRequest request, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Validates SQL query for safety and compatibility with DuckDB analytics engine.
    /// </summary>
    /// <param name="sql">SQL query to validate</param>
    /// <returns>True if SQL is valid and safe to execute, false otherwise</returns>
    Task<bool> ValidateSqlAsync(string sql);
}

/// <summary>
/// Advanced analytics service using DuckDB engine with Azure Data Lake Storage integration.
/// Provides high-performance analytics with memory optimization, concurrency control, and comprehensive monitoring.
/// </summary>
public class AnalyticsDuckDbService : IAnalyticsDuckDbService
{
    private readonly ILogger<AnalyticsDuckDbService> _logger;
    private readonly BlobServiceClient _blobServiceClient;
    private readonly IConcurrencyLimiterService _concurrencyLimiter;
    
    private const long MaxMemoryBytes = 500 * 1024 * 1024; // 500MB
    private const long WarnMemoryBytes = 400 * 1024 * 1024; // 400MB warning
    private const int DefaultPageSize = 1000;
    private const int MaxPageSize = 10000;
    private const int DefaultTimeoutSeconds = 300; // 5 minutes

    /// <summary>
    /// Initializes a new instance of the AnalyticsDuckDbService.
    /// </summary>
    /// <param name="logger">Logger for recording analytics operations and performance metrics</param>
    /// <param name="blobServiceClient">Azure Blob Service client for accessing ADLS data</param>
    /// <param name="concurrencyLimiter">Service for managing concurrent query execution limits</param>
    public AnalyticsDuckDbService(
        ILogger<AnalyticsDuckDbService> logger,
        BlobServiceClient blobServiceClient,
        IConcurrencyLimiterService concurrencyLimiter)
    {
        _logger = logger;
        _blobServiceClient = blobServiceClient;
        _concurrencyLimiter = concurrencyLimiter;
    }

    /// <summary>
    /// Executes an analytics query on ADLS data with comprehensive memory management and performance monitoring.
    /// Handles concurrency control, file preparation, memory optimization, and streaming results.
    /// </summary>
    /// <param name="request">Analytics query request containing ADLS file URI, SQL query, and execution options</param>
    /// <param name="cancellationToken">Token for cancelling the query execution</param>
    /// <returns>Analytics query response with streaming data and detailed performance metrics</returns>
    public async Task<AnalyticsQueryResponse> ExecuteQueryAsync(AnalyticsQueryRequest request, CancellationToken cancellationToken = default)
    {
        var initialMemory = GC.GetTotalMemory(true);
        var maxMemory = initialMemory;
        var peakMemory = initialMemory;
        var queryId = Guid.NewGuid().ToString();
        var startTime = DateTime.UtcNow;
        var warnings = new List<string>();
        ConcurrencySlot? slot = null;

        _logger.LogInformation("Starting analytics query {QueryId}: {Query}", queryId, request.SqlQuery);

        try
        {
            // Validate SQL safety
            var sqlValidation = await ValidateSqlSafetyAsync(request.SqlQuery);
            if (!sqlValidation.isValid)
            {
                return new AnalyticsQueryResponse
                {
                    Success = false,
                    ErrorMessage = sqlValidation.errorMessage,
                    QueryId = queryId,
                    InitialMemoryUsage = initialMemory,
                    FinalMemoryUsage = GC.GetTotalMemory(false),
                    StartTime = startTime,
                    EndTime = DateTime.UtcNow,
                    Warnings = warnings
                };
            }

            // Acquire concurrency slot
            slot = await _concurrencyLimiter.TryAcquireSlotAsync(queryId, request.SqlQuery, cancellationToken);
            if (!slot.Success)
            {
                return new AnalyticsQueryResponse
                {
                    Success = false,
                    ErrorMessage = slot.ErrorMessage ?? "Failed to acquire execution slot",
                    QueryId = queryId,
                    InitialMemoryUsage = initialMemory,
                    FinalMemoryUsage = GC.GetTotalMemory(false),
                    StartTime = startTime,
                    EndTime = DateTime.UtcNow,
                    Warnings = warnings
                };
            }

            // Validate and prepare ADLS file
            var fileInfo = await PrepareAdlsFileAsync(request.AdlsFileUri, request.SasToken, request.FileFormat, cancellationToken);
            if (!fileInfo.success)
            {
                return new AnalyticsQueryResponse
                {
                    Success = false,
                    ErrorMessage = fileInfo.errorMessage,
                    QueryId = queryId,
                    InitialMemoryUsage = initialMemory,
                    FinalMemoryUsage = GC.GetTotalMemory(false),
                    FilePath = request.AdlsFileUri,
                    StartTime = startTime,
                    EndTime = DateTime.UtcNow,
                    Warnings = warnings
                };
            }

            using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync(cancellationToken);

            // Apply memory optimizations
            if (request.EnableMemoryOptimization)
            {
                await ApplyMemoryOptimizationsAsync(connection, cancellationToken);
            }

            // Load ADLS data into DuckDB
            var loadResult = await LoadAdlsDataAsync(connection, request, fileInfo.tempFilePath!, cancellationToken);
            if (!loadResult.success)
            {
                return new AnalyticsQueryResponse
                {
                    Success = false,
                    ErrorMessage = loadResult.errorMessage,
                    QueryId = queryId,
                    InitialMemoryUsage = initialMemory,
                    FinalMemoryUsage = GC.GetTotalMemory(false),
                    FilePath = request.AdlsFileUri,
                    FileSize = fileInfo.fileSize,
                    StartTime = startTime,
                    EndTime = DateTime.UtcNow,
                    Warnings = warnings
                };
            }

            // Check memory after loading
            var memoryAfterLoad = GC.GetTotalMemory(false);
            maxMemory = Math.Max(maxMemory, memoryAfterLoad);
            peakMemory = Math.Max(peakMemory, memoryAfterLoad);

            if (memoryAfterLoad > MaxMemoryBytes)
            {
                _logger.LogWarning("Memory limit exceeded after loading data: {MemoryMB} MB", memoryAfterLoad / (1024 * 1024));
                return new AnalyticsQueryResponse
                {
                    Success = false,
                    ErrorMessage = $"Memory limit exceeded after loading data: {memoryAfterLoad / (1024 * 1024)} MB",
                    QueryId = queryId,
                    InitialMemoryUsage = initialMemory,
                    FinalMemoryUsage = memoryAfterLoad,
                    PeakMemoryUsage = peakMemory,
                    FilePath = request.AdlsFileUri,
                    FileSize = fileInfo.fileSize,
                    StartTime = startTime,
                    EndTime = DateTime.UtcNow,
                    Warnings = warnings
                };
            }

            // Build optimized query
            var finalQuery = BuildOptimizedQuery(request, loadResult.tableName!);
            _logger.LogInformation("Executing optimized query {QueryId}: {Query}", queryId, finalQuery);

            // Execute with timeout
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(request.TimeoutSeconds));

            // Stream results
            var data = StreamQueryResultsAsync(connection, finalQuery, request.PageSize, queryId, timeoutCts.Token);
            var finalMemory = GC.GetTotalMemory(false);
            maxMemory = Math.Max(maxMemory, finalMemory);
            peakMemory = Math.Max(peakMemory, finalMemory);

            _logger.LogInformation("Query {QueryId} completed successfully. Peak memory: {PeakMemoryMB} MB", 
                queryId, peakMemory / (1024 * 1024));

            return new AnalyticsQueryResponse
            {
                Success = true,
                QueryId = queryId,
                InitialMemoryUsage = initialMemory,
                FinalMemoryUsage = finalMemory,
                PeakMemoryUsage = peakMemory,
                QueryExecuted = finalQuery,
                FilePath = request.AdlsFileUri,
                FileSize = fileInfo.fileSize,
                Data = data,
                StartTime = startTime,
                Warnings = warnings,
                Performance = new QueryPerformanceMetrics
                {
                    QueryExecutionTime = DateTime.UtcNow - startTime,
                    MemoryPeakDelta = peakMemory - initialMemory,
                    OptimizationsApplied = request.EnableMemoryOptimization
                }
            };
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Query {QueryId} was cancelled", queryId);
            return new AnalyticsQueryResponse
            {
                Success = false,
                ErrorMessage = "Query execution was cancelled",
                QueryId = queryId,
                InitialMemoryUsage = initialMemory,
                FinalMemoryUsage = GC.GetTotalMemory(false),
                StartTime = startTime,
                EndTime = DateTime.UtcNow,
                Warnings = warnings
            };
        }
        catch (Exception ex)
        {
            var errorMemory = GC.GetTotalMemory(false);
            _logger.LogError(ex, "Error executing analytics query {QueryId}", queryId);
            
            return new AnalyticsQueryResponse
            {
                Success = false,
                ErrorMessage = ex.Message,
                QueryId = queryId,
                InitialMemoryUsage = initialMemory,
                FinalMemoryUsage = errorMemory,
                PeakMemoryUsage = Math.Max(peakMemory, errorMemory),
                StartTime = startTime,
                EndTime = DateTime.UtcNow,
                Warnings = warnings
            };
        }
        finally
        {
            // Release concurrency slot
            if (slot?.Success == true)
            {
                await _concurrencyLimiter.ReleaseSlotAsync(queryId);
            }
        }
    }

    public async Task<AnalyticsQueryResponse> ExecuteJoinQueryAsync(AnalyticsJoinRequest request, CancellationToken cancellationToken = default)
    {
        var initialMemory = GC.GetTotalMemory(true);
        var queryId = Guid.NewGuid().ToString();
        var startTime = DateTime.UtcNow;
        var warnings = new List<string>();
        ConcurrencySlot? slot = null;

        _logger.LogInformation("Starting analytics join query {QueryId}", queryId);

        try
        {
            // Validate SQL safety
            var sqlValidation = await ValidateSqlSafetyAsync(request.SqlQuery);
            if (!sqlValidation.isValid)
            {
                return new AnalyticsQueryResponse
                {
                    Success = false,
                    ErrorMessage = sqlValidation.errorMessage,
                    QueryId = queryId,
                    StartTime = startTime,
                    EndTime = DateTime.UtcNow
                };
            }

            // Acquire concurrency slot
            slot = await _concurrencyLimiter.TryAcquireSlotAsync(queryId, "JOIN: " + request.SqlQuery, cancellationToken);
            if (!slot.Success)
            {
                return new AnalyticsQueryResponse
                {
                    Success = false,
                    ErrorMessage = slot.ErrorMessage ?? "Failed to acquire execution slot",
                    QueryId = queryId,
                    StartTime = startTime,
                    EndTime = DateTime.UtcNow
                };
            }

            using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync(cancellationToken);

            // Apply memory optimizations
            if (request.EnableMemoryOptimization)
            {
                await ApplyMemoryOptimizationsAsync(connection, cancellationToken);
            }

            // Load both datasets
            var leftResult = await LoadAdlsDataAsync(connection, request.LeftDataset, "left_table", cancellationToken);
            if (!leftResult.success)
            {
                return new AnalyticsQueryResponse
                {
                    Success = false,
                    ErrorMessage = $"Failed to load left dataset: {leftResult.errorMessage}",
                    QueryId = queryId,
                    StartTime = startTime,
                    EndTime = DateTime.UtcNow
                };
            }

            var rightResult = await LoadAdlsDataAsync(connection, request.RightDataset, "right_table", cancellationToken);
            if (!rightResult.success)
            {
                return new AnalyticsQueryResponse
                {
                    Success = false,
                    ErrorMessage = $"Failed to load right dataset: {rightResult.errorMessage}",
                    QueryId = queryId,
                    StartTime = startTime,
                    EndTime = DateTime.UtcNow
                };
            }

            // Check memory after loading both datasets
            var memoryAfterLoad = GC.GetTotalMemory(false);
            if (memoryAfterLoad > MaxMemoryBytes)
            {
                return new AnalyticsQueryResponse
                {
                    Success = false,
                    ErrorMessage = $"Memory limit exceeded after loading datasets: {memoryAfterLoad / (1024 * 1024)} MB",
                    QueryId = queryId,
                    PeakMemoryUsage = memoryAfterLoad,
                    StartTime = startTime,
                    EndTime = DateTime.UtcNow
                };
            }

            // Execute join query with timeout
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(request.TimeoutSeconds));

            var data = StreamQueryResultsAsync(connection, request.SqlQuery, request.PageSize, queryId, timeoutCts.Token);
            
            return new AnalyticsQueryResponse
            {
                Success = true,
                QueryId = queryId,
                InitialMemoryUsage = initialMemory,
                FinalMemoryUsage = GC.GetTotalMemory(false),
                PeakMemoryUsage = Math.Max(initialMemory, memoryAfterLoad),
                QueryExecuted = request.SqlQuery,
                Data = data,
                StartTime = startTime,
                Warnings = warnings
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing join query {QueryId}", queryId);
            return new AnalyticsQueryResponse
            {
                Success = false,
                ErrorMessage = ex.Message,
                QueryId = queryId,
                StartTime = startTime,
                EndTime = DateTime.UtcNow
            };
        }
        finally
        {
            if (slot?.Success == true)
            {
                await _concurrencyLimiter.ReleaseSlotAsync(queryId);
            }
        }
    }

    public async Task<SchemaResponse> GetSchemaAsync(SchemaRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync(cancellationToken);

            var loadCommand = BuildLoadCommand(request.AdlsFileUri, request.FileFormat, "temp_schema_table");
            using var command = new DuckDBCommand(loadCommand, connection);
            await command.ExecuteNonQueryAsync(cancellationToken);

            // Get schema
            var schemaQuery = "DESCRIBE temp_schema_table";
            using var schemaCommand = new DuckDBCommand(schemaQuery, connection);
            using var reader = await schemaCommand.ExecuteReaderAsync(cancellationToken);

            var columns = new List<ReportBuilder.Response.ColumnInfo>();
            while (await reader.ReadAsync(cancellationToken))
            {
                columns.Add(new ReportBuilder.Response.ColumnInfo
                {
                    Name = reader.GetString("column_name"),
                    DataType = reader.GetString("column_type"),
                    IsNullable = reader.GetString("null").Equals("YES", StringComparison.OrdinalIgnoreCase)
                });
            }

            // Get sample data if requested
            var sampleData = new List<Dictionary<string, object?>>();
            if (request.SampleRows.HasValue && request.SampleRows.Value > 0)
            {
                var sampleQuery = $"SELECT * FROM temp_schema_table LIMIT {request.SampleRows.Value}";
                sampleData = await ExecuteSampleQueryAsync(connection, sampleQuery, cancellationToken);
            }

            return new SchemaResponse
            {
                Success = true,
                Columns = columns,
                FilePath = request.AdlsFileUri,
                SampleData = sampleData
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting schema for {Uri}", request.AdlsFileUri);
            return new SchemaResponse
            {
                Success = false,
                ErrorMessage = ex.Message
            };
        }
    }

    public async Task<bool> ValidateSqlAsync(string sql)
    {
        var validation = await ValidateSqlSafetyAsync(sql);
        return validation.isValid;
    }

    private async Task<(bool isValid, string errorMessage)> ValidateSqlSafetyAsync(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            return (false, "SQL query cannot be empty");
        }

        // Check for dangerous operations
        var dangerousPatterns = new[]
        {
            @"\bDELETE\b", @"\bUPDATE\b", @"\bINSERT\b", @"\bDROP\b", @"\bCREATE\b", @"\bALTER\b",
            @"\bTRUNCATE\b", @"\bEXEC\b", @"\bEXECUTE\b", @"\bxp_\w+", @"\bsp_\w+",
            @"--", @"/\*", @"\*/"
        };

        var sqlLower = sql.ToLower();
        foreach (var pattern in dangerousPatterns)
        {
            if (Regex.IsMatch(sqlLower, pattern, RegexOptions.IgnoreCase))
            {
                return (false, $"SQL contains potentially dangerous operation: {pattern}");
            }
        }

        return await Task.FromResult((true, string.Empty));
    }

    private async Task<(bool success, string? errorMessage, string? tempFilePath, long fileSize)> PrepareAdlsFileAsync(
        string adlsUri, string? sasToken, FileFormat format, CancellationToken cancellationToken)
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

    private async Task<(bool success, string? errorMessage, string? tableName)> LoadAdlsDataAsync(
        DuckDBConnection connection, AnalyticsQueryRequest request, string? tempFilePath, CancellationToken cancellationToken)
    {
        try
        {
            var tableName = "main_table";
            var loadCommand = BuildLoadCommand(tempFilePath ?? request.AdlsFileUri, request.FileFormat, tableName);
            
            using var command = new DuckDBCommand(loadCommand, connection);
            await command.ExecuteNonQueryAsync(cancellationToken);
            
            return (true, null, tableName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading ADLS data");
            return (false, ex.Message, null);
        }
    }

    private async Task<(bool success, string? errorMessage, string? tableName)> LoadAdlsDataAsync(
        DuckDBConnection connection, AdlsFileReference fileRef, string tableName, CancellationToken cancellationToken)
    {
        try
        {
            var loadCommand = BuildLoadCommand(fileRef.AdlsFileUri, fileRef.FileFormat, tableName);
            
            using var command = new DuckDBCommand(loadCommand, connection);
            await command.ExecuteNonQueryAsync(cancellationToken);
            
            return (true, null, tableName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading ADLS data for table {TableName}", tableName);
            return (false, ex.Message, null);
        }
    }

    private string BuildLoadCommand(string filePath, FileFormat format, string tableName)
    {
        return format switch
        {
            FileFormat.Csv => $"CREATE TABLE {tableName} AS SELECT * FROM read_csv_auto('{filePath}')",
            FileFormat.Parquet => $"CREATE TABLE {tableName} AS SELECT * FROM parquet_scan('{filePath}')",
            FileFormat.Arrow => $"CREATE TABLE {tableName} AS SELECT * FROM read_parquet('{filePath}')", // DuckDB treats Arrow as Parquet for loading
            _ => $"CREATE TABLE {tableName} AS SELECT * FROM read_csv_auto('{filePath}')" // Auto-detect
        };
    }

    private string BuildOptimizedQuery(AnalyticsQueryRequest request, string tableName)
    {
        var query = request.SqlQuery;
        
        // Replace common table references
        query = query.Replace("FROM table", $"FROM {tableName}", StringComparison.OrdinalIgnoreCase);
        query = query.Replace("FROM data", $"FROM {tableName}", StringComparison.OrdinalIgnoreCase);
        
        // Apply limit if not present and requested
        if (request.Limit.HasValue && !query.ToLower().Contains("limit"))
        {
            query += $" LIMIT {request.Limit.Value}";
        }
        
        return query;
    }

    private async Task ApplyMemoryOptimizationsAsync(DuckDBConnection connection, CancellationToken cancellationToken)
    {
        var optimizations = new[]
        {
            "SET memory_limit = '400MB'",
            "SET max_memory = '450MB'",
            "SET threads = 2",
            "SET enable_progress_bar = false"
        };

        foreach (var optimization in optimizations)
        {
            try
            {
                using var command = new DuckDBCommand(optimization, connection);
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to apply optimization: {Optimization}", optimization);
            }
        }
    }

    private async IAsyncEnumerable<Dictionary<string, object?>> StreamQueryResultsAsync(
        DuckDBConnection connection,
        string query,
        int pageSize,
        string queryId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        pageSize = Math.Clamp(pageSize, 100, MaxPageSize);
        
        using var command = new DuckDBCommand(query, connection);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        
        var columnNames = new string[reader.FieldCount];
        for (int i = 0; i < reader.FieldCount; i++)
        {
            columnNames[i] = reader.GetName(i);
        }

        var rowCount = 0;
        var batch = new List<Dictionary<string, object?>>();

        while (await reader.ReadAsync(cancellationToken))
        {
            // Memory check every 100 rows
            if (rowCount % 100 == 0)
            {
                var currentMemory = GC.GetTotalMemory(false);
                if (currentMemory > MaxMemoryBytes)
                {
                    _logger.LogWarning("Memory limit exceeded during streaming for query {QueryId}: {MemoryMB} MB", 
                        queryId, currentMemory / (1024 * 1024));
                    throw new InvalidOperationException($"Memory limit exceeded: {currentMemory / (1024 * 1024)} MB");
                }
            }

            var row = new Dictionary<string, object?>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row[columnNames[i]] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            batch.Add(row);
            rowCount++;

            if (batch.Count >= pageSize)
            {
                foreach (var item in batch)
                {
                    yield return item;
                }
                batch.Clear();
                
                // Periodic GC
                if (rowCount % (pageSize * 5) == 0)
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }
            }
        }

        // Yield remaining items
        foreach (var item in batch)
        {
            yield return item;
        }
        
        _logger.LogInformation("Completed streaming {RowCount} rows for query {QueryId}", rowCount, queryId);
    }

    private async Task<List<Dictionary<string, object?>>> ExecuteSampleQueryAsync(
        DuckDBConnection connection, string query, CancellationToken cancellationToken)
    {
        var results = new List<Dictionary<string, object?>>();
        
        using var command = new DuckDBCommand(query, connection);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        
        var columnNames = new string[reader.FieldCount];
        for (int i = 0; i < reader.FieldCount; i++)
        {
            columnNames[i] = reader.GetName(i);
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            var row = new Dictionary<string, object?>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row[columnNames[i]] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }
            results.Add(row);
        }

        return results;
    }

    private BlobClient GetBlobClient(string adlsUri, string? sasToken)
    {
        var uri = new Uri(adlsUri);
        
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

    private static string GetBlobName(Uri uri)
    {
        var segments = uri.AbsolutePath.Trim('/').Split('/');
        return string.Join("/", segments.Skip(1));
    }
}