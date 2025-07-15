using DuckDB.NET.Data;
using ReportBuilder.Request;
using ReportBuilder.Response;
using System.Data;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Diagnostics;

namespace ReportBuilder.Service;

public interface IPaginatedQueryService
{
    Task<PaginatedResponse<Dictionary<string, object?>>> ExecutePaginatedQueryAsync(
        PaginatedQueryRequest request, 
        CancellationToken cancellationToken = default);
}

public class PaginatedQueryService : IPaginatedQueryService
{
    private readonly ILogger<PaginatedQueryService> _logger;
    private const long MaxMemoryBytes = 500 * 1024 * 1024; // 500MB
    private const int MaxPageSize = 10000;
    private const int DefaultPageSize = 1000;

    public PaginatedQueryService(ILogger<PaginatedQueryService> logger)
    {
        _logger = logger;
    }

    public async Task<PaginatedResponse<Dictionary<string, object?>>> ExecutePaginatedQueryAsync(
        PaginatedQueryRequest request, 
        CancellationToken cancellationToken = default)
    {
        var startTime = DateTime.UtcNow;
        var initialMemory = GC.GetTotalMemory(true);
        var peakMemory = initialMemory;
        var initialGcCount = GC.CollectionCount(0);

        if (request.EnableMemoryLogging)
        {
            _logger.LogInformation("Starting paginated query. Page: {PageNumber}, Size: {PageSize}, Initial memory: {MemoryMB} MB", 
                request.PageNumber, request.PageSize, initialMemory / (1024 * 1024));
        }

        try
        {
            // Validate request
            var validation = ValidateRequest(request);
            if (!validation.isValid)
            {
                return new PaginatedResponse<Dictionary<string, object?>>
                {
                    Success = false,
                    ErrorMessage = validation.errorMessage,
                    Performance = new PerformanceMetadata
                    {
                        InitialMemoryUsage = initialMemory,
                        FinalMemoryUsage = GC.GetTotalMemory(false)
                    }
                };
            }

            using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync(cancellationToken);

            var loadStartTime = DateTime.UtcNow;
            
            // Load file into DuckDB
            var loadResult = await LoadFileIntoTableAsync(connection, request, cancellationToken);
            if (!loadResult.success)
            {
                return new PaginatedResponse<Dictionary<string, object?>>
                {
                    Success = false,
                    ErrorMessage = loadResult.errorMessage,
                    Performance = new PerformanceMetadata
                    {
                        InitialMemoryUsage = initialMemory,
                        FinalMemoryUsage = GC.GetTotalMemory(false)
                    }
                };
            }

            var loadEndTime = DateTime.UtcNow;
            var currentMemory = GC.GetTotalMemory(false);
            peakMemory = Math.Max(peakMemory, currentMemory);

            // Get total count if requested (parallel query)
            Task<long?> totalCountTask = Task.FromResult<long?>(null);
            if (request.IncludeTotalCount)
            {
                totalCountTask = GetTotalRowCountAsync(connection, request, cancellationToken);
            }

            // Build paginated query
            var paginatedQuery = BuildPaginatedQuery(request);
            _logger.LogDebug("Executing paginated query: {Query}", paginatedQuery);

            var queryStartTime = DateTime.UtcNow;

            // Execute paginated query with streaming
            var data = StreamPaginatedResultsAsync(connection, paginatedQuery, request.PageSize, cancellationToken);
            
            var queryEndTime = DateTime.UtcNow;
            var finalMemory = GC.GetTotalMemory(false);
            peakMemory = Math.Max(peakMemory, finalMemory);
            var finalGcCount = GC.CollectionCount(0);

            // Wait for total count if requested
            var totalCount = await totalCountTask;

            // Calculate pagination metadata
            var pagination = CalculatePaginationMetadata(request, totalCount);
            
            if (request.EnableMemoryLogging)
            {
                _logger.LogInformation("Paginated query completed. Page: {PageNumber}, Final memory: {MemoryMB} MB, Peak: {PeakMemoryMB} MB", 
                    request.PageNumber, finalMemory / (1024 * 1024), peakMemory / (1024 * 1024));
            }

            return new PaginatedResponse<Dictionary<string, object?>>
            {
                Success = true,
                Data = data,
                Pagination = pagination,
                Query = new QueryMetadata
                {
                    OriginalQuery = request.SqlQuery,
                    ExecutedQuery = paginatedQuery,
                    FilePath = request.FilePath,
                    FileFormat = request.FileFormat.ToString(),
                    FileSize = loadResult.fileSize,
                    QueryStartTime = startTime,
                    QueryEndTime = queryEndTime
                },
                Performance = new PerformanceMetadata
                {
                    InitialMemoryUsage = initialMemory,
                    FinalMemoryUsage = finalMemory,
                    PeakMemoryUsage = peakMemory,
                    MemoryDelta = finalMemory - initialMemory,
                    QueryExecutionTime = queryEndTime - queryStartTime,
                    DataLoadTime = loadEndTime - loadStartTime,
                    MemoryOptimized = true,
                    GarbageCollections = finalGcCount - initialGcCount
                }
            };
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Paginated query was cancelled for page {PageNumber}", request.PageNumber);
            return new PaginatedResponse<Dictionary<string, object?>>
            {
                Success = false,
                ErrorMessage = "Query was cancelled",
                Performance = new PerformanceMetadata
                {
                    InitialMemoryUsage = initialMemory,
                    FinalMemoryUsage = GC.GetTotalMemory(false)
                }
            };
        }
        catch (Exception ex)
        {
            var errorMemory = GC.GetTotalMemory(false);
            peakMemory = Math.Max(peakMemory, errorMemory);
            
            _logger.LogError(ex, "Error executing paginated query for page {PageNumber}", request.PageNumber);
            
            return new PaginatedResponse<Dictionary<string, object?>>
            {
                Success = false,
                ErrorMessage = ex.Message,
                Performance = new PerformanceMetadata
                {
                    InitialMemoryUsage = initialMemory,
                    FinalMemoryUsage = errorMemory,
                    PeakMemoryUsage = peakMemory
                }
            };
        }
    }

    private (bool isValid, string errorMessage) ValidateRequest(PaginatedQueryRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.SqlQuery))
        {
            return (false, "SQL query is required");
        }

        if (string.IsNullOrWhiteSpace(request.FilePath))
        {
            return (false, "File path is required");
        }

        if (request.PageNumber < 1)
        {
            return (false, "Page number must be greater than 0");
        }

        if (request.PageSize < 1 || request.PageSize > MaxPageSize)
        {
            return (false, $"Page size must be between 1 and {MaxPageSize}");
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
        PaginatedQueryRequest request, 
        CancellationToken cancellationToken)
    {
        try
        {
            // Check if file exists
            if (!System.IO.File.Exists(request.FilePath))
            {
                return (false, $"File not found: {request.FilePath}", 0);
            }

            var fileInfo = new FileInfo(request.FilePath);
            var fileSize = fileInfo.Length;

            // Determine load command based on file format
            var loadCommand = BuildLoadCommand(request);
            
            _logger.LogDebug("Loading file with command: {Command}", loadCommand);

            using var command = new DuckDBCommand(loadCommand, connection);
            await command.ExecuteNonQueryAsync(cancellationToken);

            _logger.LogInformation("Successfully loaded file: {FilePath}, Size: {SizeKB} KB", 
                request.FilePath, fileSize / 1024);

            return (true, null, fileSize);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading file: {FilePath}", request.FilePath);
            return (false, ex.Message, 0);
        }
    }

    private string BuildLoadCommand(PaginatedQueryRequest request)
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
            PaginatedFileFormat.Csv => $"CREATE TABLE {tableName} AS SELECT {selectClause} FROM read_csv_auto('{request.FilePath}'){whereClause}",
            PaginatedFileFormat.Parquet => $"CREATE TABLE {tableName} AS SELECT {selectClause} FROM parquet_scan('{request.FilePath}'){whereClause}",
            PaginatedFileFormat.Arrow => $"CREATE TABLE {tableName} AS SELECT {selectClause} FROM read_parquet('{request.FilePath}'){whereClause}", // DuckDB treats Arrow as Parquet
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

    private string BuildPaginatedQuery(PaginatedQueryRequest request)
    {
        var baseQuery = request.SqlQuery.Trim();
        
        // Replace common table references
        baseQuery = baseQuery.Replace("FROM table", "FROM data_table", StringComparison.OrdinalIgnoreCase);
        baseQuery = baseQuery.Replace("FROM data", "FROM data_table", StringComparison.OrdinalIgnoreCase);
        
        // Add ORDER BY if not present (required for consistent pagination)
        if (!baseQuery.ToLower().Contains("order by") && !string.IsNullOrEmpty(request.OrderBy))
        {
            baseQuery += $" ORDER BY {request.OrderBy}";
        }
        else if (!baseQuery.ToLower().Contains("order by"))
        {
            // Add a default ORDER BY to ensure consistent pagination
            baseQuery += " ORDER BY 1"; // Order by first column
        }

        // Calculate offset
        var offset = (request.PageNumber - 1) * request.PageSize;
        
        // Remove existing LIMIT/OFFSET if present
        var limitPattern = @"\s+LIMIT\s+\d+(\s+OFFSET\s+\d+)?";
        baseQuery = Regex.Replace(baseQuery, limitPattern, string.Empty, RegexOptions.IgnoreCase);
        
        // Add pagination
        baseQuery += $" LIMIT {request.PageSize} OFFSET {offset}";
        
        return baseQuery;
    }

    private async Task<long?> GetTotalRowCountAsync(
        DuckDBConnection connection, 
        PaginatedQueryRequest request, 
        CancellationToken cancellationToken)
    {
        try
        {
            var stopwatch = Stopwatch.StartNew();
            
            // Build count query from original query
            var countQuery = BuildCountQuery(request.SqlQuery);
            
            _logger.LogDebug("Executing count query: {CountQuery}", countQuery);

            using var command = new DuckDBCommand(countQuery, connection);
            var result = await command.ExecuteScalarAsync(cancellationToken);
            
            stopwatch.Stop();
            _logger.LogDebug("Count query completed in {ElapsedMs} ms", stopwatch.ElapsedMilliseconds);
            
            return Convert.ToInt64(result);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to get total row count, continuing without it");
            return null;
        }
    }

    private string BuildCountQuery(string originalQuery)
    {
        // Replace table references
        var countQuery = originalQuery.Replace("FROM table", "FROM data_table", StringComparison.OrdinalIgnoreCase);
        countQuery = countQuery.Replace("FROM data", "FROM data_table", StringComparison.OrdinalIgnoreCase);
        
        // Extract the FROM clause and WHERE clause if present
        var fromMatch = Regex.Match(countQuery, @"FROM\s+[\w_]+(?:\s+AS\s+[\w_]+)?(?:\s+WHERE\s+.+?)?(?:\s+ORDER\s+BY|$)", RegexOptions.IgnoreCase);
        
        if (fromMatch.Success)
        {
            var fromClause = fromMatch.Value;
            // Remove ORDER BY if present
            fromClause = Regex.Replace(fromClause, @"\s+ORDER\s+BY.*$", string.Empty, RegexOptions.IgnoreCase);
            return $"SELECT COUNT(*) {fromClause}";
        }
        
        // Fallback: wrap the original query
        return $"SELECT COUNT(*) FROM ({countQuery.TrimEnd(';')}) AS count_subquery";
    }

    private PaginationMetadata CalculatePaginationMetadata(PaginatedQueryRequest request, long? totalCount)
    {
        var metadata = new PaginationMetadata
        {
            CurrentPage = request.PageNumber,
            PageSize = request.PageSize,
            TotalRows = totalCount,
            HasPreviousPage = request.PageNumber > 1,
            StartRow = (request.PageNumber - 1) * request.PageSize + 1
        };

        if (totalCount.HasValue)
        {
            metadata.TotalPages = (int)Math.Ceiling((double)totalCount.Value / request.PageSize);
            metadata.HasNextPage = request.PageNumber < metadata.TotalPages;
            metadata.EndRow = Math.Min(request.PageNumber * request.PageSize, (int)totalCount.Value);
            metadata.RowsInCurrentPage = metadata.EndRow - metadata.StartRow + 1;
        }
        else
        {
            // Without total count, we estimate based on page size
            metadata.EndRow = request.PageNumber * request.PageSize;
            metadata.RowsInCurrentPage = request.PageSize;
            metadata.HasNextPage = true; // Assume there might be more data
        }

        return metadata;
    }

    private async IAsyncEnumerable<Dictionary<string, object?>> StreamPaginatedResultsAsync(
        DuckDBConnection connection,
        string query,
        int expectedPageSize,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var command = new DuckDBCommand(query, connection);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        
        var columnNames = new string[reader.FieldCount];
        for (int i = 0; i < reader.FieldCount; i++)
        {
            columnNames[i] = reader.GetName(i);
        }

        var rowCount = 0;
        var batchSize = Math.Min(expectedPageSize, DefaultPageSize);
        var batch = new List<Dictionary<string, object?>>();

        while (await reader.ReadAsync(cancellationToken) && rowCount < expectedPageSize)
        {
            // Memory check every 100 rows
            if (rowCount % 100 == 0)
            {
                var currentMemory = GC.GetTotalMemory(false);
                if (currentMemory > MaxMemoryBytes)
                {
                    _logger.LogWarning("Memory limit approaching during pagination: {MemoryMB} MB", currentMemory / (1024 * 1024));
                    // Don't throw, just log warning since we're streaming limited rows
                }
            }

            var row = new Dictionary<string, object?>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row[columnNames[i]] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            batch.Add(row);
            rowCount++;

            // Yield in smaller batches for better memory efficiency
            if (batch.Count >= batchSize)
            {
                foreach (var item in batch)
                {
                    yield return item;
                }
                batch.Clear();
            }
        }

        // Yield remaining items
        foreach (var item in batch)
        {
            yield return item;
        }
        
        _logger.LogDebug("Streamed {RowCount} rows for current page", rowCount);
    }
}