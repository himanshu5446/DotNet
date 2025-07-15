using DuckDB.NET.Data;
using ReportBuilder.Request;
using ReportBuilder.Response;
using System.Data;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Runtime.CompilerServices;
using System.Text;

namespace ReportBuilder.Service;

/// <summary>
/// Interface for streaming query results in Apache Arrow format with high-performance capabilities.
/// Provides methods for query validation, metadata extraction, and efficient Arrow IPC streaming.
/// </summary>
public interface IArrowStreamService
{
    /// <summary>
    /// Validates query parameters and prepares metadata without executing the full query.
    /// </summary>
    /// <param name="request">Arrow stream request containing query and file information</param>
    /// <param name="cancellationToken">Token for cancelling the validation operation</param>
    /// <returns>Validation result with success status, error message, and query metadata</returns>
    Task<(bool Success, string? ErrorMessage, ArrowStreamMetadata? Metadata)> ValidateAndPrepareQueryAsync(
        ArrowStreamRequest request, 
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams query results directly in Apache Arrow IPC format to the output stream.
    /// </summary>
    /// <param name="request">Arrow stream request with query and configuration options</param>
    /// <param name="outputStream">Target stream for writing Arrow IPC data</param>
    /// <param name="onPerformanceUpdate">Optional callback for receiving performance metrics during streaming</param>
    /// <param name="cancellationToken">Token for cancelling the streaming operation</param>
    /// <returns>Task representing the streaming operation</returns>
    Task StreamArrowDataAsync(
        ArrowStreamRequest request,
        Stream outputStream,
        Action<ArrowStreamPerformance>? onPerformanceUpdate = null,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// High-performance service for streaming query results in Apache Arrow IPC format.
/// Provides memory-efficient streaming with performance monitoring and comprehensive error handling.
/// </summary>
public class ArrowStreamService : IArrowStreamService
{
    private readonly ILogger<ArrowStreamService> _logger;
    private const long MaxMemoryBytes = 500 * 1024 * 1024; // 500MB
    private const int BufferSize = 65536; // 64KB buffer for streaming

    /// <summary>
    /// Initializes a new instance of the ArrowStreamService.
    /// </summary>
    /// <param name="logger">Logger for recording streaming operations and performance metrics</param>
    public ArrowStreamService(ILogger<ArrowStreamService> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Validates Arrow stream request parameters and extracts query metadata for optimization.
    /// Loads data into DuckDB, validates the query, and returns metadata without executing the full query.
    /// </summary>
    /// <param name="request">Arrow stream request containing file path, query, and validation options</param>
    /// <param name="cancellationToken">Token for cancelling the validation operation</param>
    /// <returns>Tuple with validation success, error message if failed, and extracted metadata</returns>
    public async Task<(bool Success, string? ErrorMessage, ArrowStreamMetadata? Metadata)> ValidateAndPrepareQueryAsync(
        ArrowStreamRequest request, 
        CancellationToken cancellationToken = default)
    {
        try
        {
            // Validate request
            var validation = ValidateRequest(request);
            if (!validation.isValid)
            {
                return (false, validation.errorMessage, null);
            }

            // Check if file exists
            if (!File.Exists(request.FilePath))
            {
                return (false, $"File not found: {request.FilePath}", null);
            }

            using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync(cancellationToken);

            // Load file into DuckDB
            var loadResult = await LoadFileIntoTableAsync(connection, request, cancellationToken);
            if (!loadResult.success)
            {
                return (false, loadResult.errorMessage, null);
            }

            // Get metadata about the query
            var metadata = await GetQueryMetadataAsync(connection, request, loadResult.fileSize, cancellationToken);
            return (true, null, metadata);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error validating Arrow stream request");
            return (false, ex.Message, null);
        }
    }

    /// <summary>
    /// Executes query and streams results directly in Apache Arrow IPC format with real-time performance monitoring.
    /// Handles file loading, query execution, Arrow export, and streaming with memory management.
    /// </summary>
    /// <param name="request">Arrow stream request with query, file path, and streaming configuration</param>
    /// <param name="outputStream">Target stream for writing Arrow IPC formatted data</param>
    /// <param name="onPerformanceUpdate">Optional callback for receiving real-time performance metrics</param>
    /// <param name="cancellationToken">Token for cancelling the streaming operation</param>
    /// <returns>Task representing the asynchronous streaming operation</returns>
    public async Task StreamArrowDataAsync(
        ArrowStreamRequest request,
        Stream outputStream,
        Action<ArrowStreamPerformance>? onPerformanceUpdate = null,
        CancellationToken cancellationToken = default)
    {
        var queryId = Guid.NewGuid().ToString("N")[..8];
        var startTime = DateTime.UtcNow;
        var initialMemory = GC.GetTotalMemory(true);
        var peakMemory = initialMemory;
        var initialGcCount = GC.CollectionCount(0);
        var bytesStreamed = 0L;
        var tempFile = string.Empty;

        if (request.EnableMemoryLogging)
        {
            _logger.LogInformation("[ARROW-{QueryId}] Starting Arrow stream. Initial memory: {MemoryMB} MB", queryId, initialMemory / (1024 * 1024));
        }

        try
        {
            using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync(cancellationToken);

            var loadStartTime = DateTime.UtcNow;
            
            // Load file into DuckDB with cancellation support
            _logger.LogDebug("[ARROW-{QueryId}] Loading file: {FilePath}", queryId, request.FilePath);
            var loadResult = await LoadFileIntoTableAsync(connection, request, cancellationToken);
            if (!loadResult.success)
            {
                throw new InvalidOperationException($"Failed to load file: {loadResult.errorMessage}");
            }

            var loadEndTime = DateTime.UtcNow;
            var queryStartTime = DateTime.UtcNow;

            // Build the query for Arrow export
            var arrowQuery = BuildArrowExportQuery(request);
            _logger.LogDebug("[ARROW-{QueryId}] Executing Arrow export query: {Query}", queryId, arrowQuery);

            // DuckDB doesn't support COPY TO STDOUT directly via .NET driver,
            // so we'll use a different approach: export to a temporary file first
            tempFile = Path.GetTempFileName() + ".arrow";
            _logger.LogDebug("[ARROW-{QueryId}] Creating temporary Arrow file: {TempFile}", queryId, tempFile);
            
            try
            {
                // Export to temporary Arrow file with cancellation support
                var exportQuery = $"COPY ({BuildSelectQuery(request)}) TO '{tempFile}' (FORMAT 'arrow')";
                _logger.LogDebug("[ARROW-{QueryId}] Executing export query: {Query}", queryId, exportQuery);
                
                using var exportCommand = new DuckDBCommand(exportQuery, connection);
                exportCommand.CommandTimeout = request.TimeoutSeconds;
                
                try
                {
                    await exportCommand.ExecuteNonQueryAsync(cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    _logger.LogWarning("[CANCELLED] Arrow export for query {QueryId} aborted by client", queryId);
                    throw new TaskCanceledException("[CANCELLED] Arrow export aborted by client");
                }

                var queryEndTime = DateTime.UtcNow;
                var streamStartTime = DateTime.UtcNow;

                // Stream the Arrow file to output with cancellation support
                _logger.LogDebug("[ARROW-{QueryId}] Starting file streaming from: {TempFile}", queryId, tempFile);
                using var fileStream = new FileStream(tempFile, FileMode.Open, FileAccess.Read, FileShare.Read, BufferSize, FileOptions.SequentialScan);
                var buffer = new byte[BufferSize];
                int bytesRead;

                try
                {
                    while ((bytesRead = await fileStream.ReadAsync(buffer, 0, buffer.Length, cancellationToken)) > 0)
                    {
                        await outputStream.WriteAsync(buffer, 0, bytesRead, cancellationToken);
                        bytesStreamed += bytesRead;

                        // Memory monitoring every 1MB
                        if (bytesStreamed % (1024 * 1024) == 0)
                        {
                            var currentMemory = GC.GetTotalMemory(false);
                            peakMemory = Math.Max(peakMemory, currentMemory);
                            
                            if (currentMemory > MaxMemoryBytes)
                            {
                                _logger.LogWarning("[ARROW-{QueryId}] Memory usage high during streaming: {MemoryMB} MB", queryId, currentMemory / (1024 * 1024));
                            }
                        }
                    }

                    await outputStream.FlushAsync(cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    _logger.LogWarning("[CANCELLED] Arrow streaming for query {QueryId} aborted by client at {BytesStreamed} bytes", queryId, bytesStreamed);
                    throw new TaskCanceledException("[CANCELLED] Arrow streaming aborted by client");
                }
                var streamEndTime = DateTime.UtcNow;
                var finalMemory = GC.GetTotalMemory(false);
                var finalGcCount = GC.CollectionCount(0);

                // Calculate performance metrics
                var performance = new ArrowStreamPerformance
                {
                    InitialMemoryUsage = initialMemory,
                    FinalMemoryUsage = finalMemory,
                    PeakMemoryUsage = peakMemory,
                    MemoryDelta = finalMemory - initialMemory,
                    QueryExecutionTime = queryEndTime - queryStartTime,
                    DataLoadTime = loadEndTime - loadStartTime,
                    StreamingTime = streamEndTime - streamStartTime,
                    BytesStreamed = bytesStreamed,
                    StreamingRateMBps = bytesStreamed / (1024.0 * 1024.0) / (streamEndTime - streamStartTime).TotalSeconds,
                    MemoryOptimized = true,
                    GarbageCollections = finalGcCount - initialGcCount
                };

                onPerformanceUpdate?.Invoke(performance);

                if (request.EnableMemoryLogging)
                {
                    _logger.LogInformation("[ARROW-{QueryId}] Arrow stream completed. Bytes streamed: {BytesMB} MB, " +
                        "Final memory: {MemoryMB} MB, Peak: {PeakMemoryMB} MB, Rate: {RateMBps:F2} MB/s",
                        queryId, bytesStreamed / (1024 * 1024), 
                        finalMemory / (1024 * 1024), 
                        peakMemory / (1024 * 1024),
                        performance.StreamingRateMBps);
                }
            }
            finally
            {
                // Clean up temporary file
                await CleanupTempFileAsync(tempFile, queryId);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning("[CANCELLED] Arrow stream for query {QueryId} was cancelled by client", queryId);
            await CleanupTempFileAsync(tempFile, queryId);
            throw new TaskCanceledException("[CANCELLED] Query aborted by client");
        }
        catch (Exception ex)
        {
            var errorMemory = GC.GetTotalMemory(false);
            peakMemory = Math.Max(peakMemory, errorMemory);
            
            _logger.LogError(ex, "[ARROW-{QueryId}] Error streaming Arrow data", queryId);
            await CleanupTempFileAsync(tempFile, queryId);
            throw;
        }
    }

    private (bool isValid, string errorMessage) ValidateRequest(ArrowStreamRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.SqlQuery))
        {
            return (false, "SQL query is required");
        }

        if (string.IsNullOrWhiteSpace(request.FilePath))
        {
            return (false, "File path is required");
        }

        if (request.TimeoutSeconds < 1 || request.TimeoutSeconds > 3600)
        {
            return (false, "Timeout must be between 1 and 3600 seconds");
        }

        if (request.LimitRows.HasValue && request.LimitRows.Value < 1)
        {
            return (false, "Limit rows must be greater than 0");
        }

        // Validate SQL for dangerous operations
        var sqlValidation = ValidateSqlSafety(request.SqlQuery);
        if (!sqlValidation.isValid)
        {
            return sqlValidation;
        }

        return (true, string.Empty);
    }

    private (bool isValid, string errorMessage) ValidateSqlSafety(string sql)
    {
        var dangerousPatterns = new[]
        {
            @"\bDELETE\b", @"\bUPDATE\b", @"\bINSERT\b", @"\bDROP\b", 
            @"\bCREATE\b", @"\bALTER\b", @"\bTRUNCATE\b", @"\bEXEC\b"
        };

        foreach (var pattern in dangerousPatterns)
        {
            if (Regex.IsMatch(sql, pattern, RegexOptions.IgnoreCase))
            {
                return (false, $"SQL contains prohibited operation: {pattern}");
            }
        }

        return (true, string.Empty);
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

            // Determine load command based on file format
            var loadCommand = BuildLoadCommand(request);
            
            _logger.LogDebug("Loading file with command: {Command}", loadCommand);

            using var command = new DuckDBCommand(loadCommand, connection);
            command.CommandTimeout = request.TimeoutSeconds;
            
            try
            {
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _logger.LogWarning("[CANCELLED] File loading aborted by client: {FilePath}", request.FilePath);
                throw;
            }

            _logger.LogInformation("Successfully loaded file: {FilePath}, Size: {SizeKB} KB", 
                request.FilePath, fileSize / 1024);

            return (true, null, fileSize);
        }
        catch (OperationCanceledException)
        {
            throw;
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
                _logger.LogDebug("[CLEANUP] Removed temporary Arrow file for query {QueryId}: {TempFile}", queryId, tempFile);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[CLEANUP] Failed to delete temporary Arrow file for query {QueryId}: {TempFile}", queryId, tempFile);
        }
        
        await Task.CompletedTask;
    }

    private string BuildLoadCommand(ArrowStreamRequest request)
    {
        var tableName = "data_table";
        
        // Build projection if specified
        var selectClause = "*";
        if (request.ProjectionColumns.Any())
        {
            selectClause = string.Join(", ", request.ProjectionColumns.Select(col => $"'{col}'"));
        }

        // Build WHERE clause if specified
        var whereClause = string.Empty;
        if (!string.IsNullOrEmpty(request.WhereClause))
        {
            whereClause = $" WHERE {request.WhereClause}";
        }

        return request.FileFormat switch
        {
            ArrowFileFormat.Csv => $"CREATE TABLE {tableName} AS SELECT {selectClause} FROM read_csv_auto('{request.FilePath}'){whereClause}",
            ArrowFileFormat.Parquet => $"CREATE TABLE {tableName} AS SELECT {selectClause} FROM parquet_scan('{request.FilePath}'){whereClause}",
            ArrowFileFormat.Arrow => $"CREATE TABLE {tableName} AS SELECT {selectClause} FROM read_parquet('{request.FilePath}'){whereClause}", // DuckDB treats Arrow as Parquet
            _ => DetermineFormatAndBuildCommand(request.FilePath, selectClause, whereClause)
        };
    }

    private string DetermineFormatAndBuildCommand(string filePath, string selectClause, string whereClause)
    {
        var extension = Path.GetExtension(filePath).ToLower();
        var tableName = "data_table";

        return extension switch
        {
            ".csv" => $"CREATE TABLE {tableName} AS SELECT {selectClause} FROM read_csv_auto('{filePath}'){whereClause}",
            ".parquet" => $"CREATE TABLE {tableName} AS SELECT {selectClause} FROM parquet_scan('{filePath}'){whereClause}",
            ".arrow" => $"CREATE TABLE {tableName} AS SELECT {selectClause} FROM read_parquet('{filePath}'){whereClause}",
            _ => $"CREATE TABLE {tableName} AS SELECT {selectClause} FROM read_csv_auto('{filePath}'){whereClause}" // Default to CSV
        };
    }

    private string BuildSelectQuery(ArrowStreamRequest request)
    {
        var baseQuery = request.SqlQuery.Trim();
        
        // Replace common table references
        baseQuery = baseQuery.Replace("FROM table", "FROM data_table", StringComparison.OrdinalIgnoreCase);
        baseQuery = baseQuery.Replace("FROM data", "FROM data_table", StringComparison.OrdinalIgnoreCase);
        
        // Add ORDER BY if specified
        if (!baseQuery.ToLower().Contains("order by") && !string.IsNullOrEmpty(request.OrderBy))
        {
            baseQuery += $" ORDER BY {request.OrderBy}";
        }

        // Add LIMIT if specified
        if (request.LimitRows.HasValue)
        {
            if (!baseQuery.ToLower().Contains("limit"))
            {
                baseQuery += $" LIMIT {request.LimitRows.Value}";
            }
        }
        
        return baseQuery;
    }

    private string BuildArrowExportQuery(ArrowStreamRequest request)
    {
        var selectQuery = BuildSelectQuery(request);
        return $"COPY ({selectQuery}) TO STDOUT (FORMAT 'arrow')";
    }

    private async Task<ArrowStreamMetadata> GetQueryMetadataAsync(
        DuckDBConnection connection, 
        ArrowStreamRequest request, 
        long fileSize,
        CancellationToken cancellationToken)
    {
        var metadata = new ArrowStreamMetadata
        {
            OriginalQuery = request.SqlQuery,
            ExecutedQuery = BuildSelectQuery(request),
            FilePath = request.FilePath,
            FileFormat = request.FileFormat.ToString(),
            FileSize = fileSize,
            QueryStartTime = DateTime.UtcNow
        };

        try
        {
            // Get column information
            var schemaQuery = $"DESCRIBE ({metadata.ExecutedQuery})";
            using var command = new DuckDBCommand(schemaQuery, connection);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);

            var columns = new List<ArrowColumnInfo>();
            var columnIndex = 0;

            while (await reader.ReadAsync(cancellationToken))
            {
                columns.Add(new ArrowColumnInfo
                {
                    Name = reader.GetString("column_name"),
                    DataType = reader.GetString("column_type"),
                    IsNullable = reader.GetString("null").Equals("YES", StringComparison.OrdinalIgnoreCase),
                    Index = columnIndex++
                });
            }

            metadata.Columns = columns;

            // Get estimated row count
            try
            {
                var countQuery = $"SELECT COUNT(*) FROM ({metadata.ExecutedQuery}) AS count_subquery";
                using var countCommand = new DuckDBCommand(countQuery, connection);
                var countResult = await countCommand.ExecuteScalarAsync(cancellationToken);
                metadata.EstimatedRows = Convert.ToInt64(countResult);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Could not get estimated row count");
                metadata.EstimatedRows = -1;
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not get complete query metadata");
        }

        return metadata;
    }
}