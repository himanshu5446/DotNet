using DuckDB.NET.Data;
using ReportBuilder.Request;
using ReportBuilder.Response;
using System.Data;
using System.Runtime.CompilerServices;

namespace ReportBuilder.Service;

/// <summary>
/// Service interface for executing queries against CSV files using DuckDB
/// </summary>
public interface ICsvDuckDbService
{
    /// <summary>
    /// Executes a query against an uploaded CSV file with projection and filtering
    /// </summary>
    /// <param name="request">CSV query request with file, columns, and optional filters</param>
    /// <param name="cancellationToken">Token to cancel the operation</param>
    /// <returns>Query response with streaming results or error information</returns>
    Task<CsvQueryResponse> ExecuteCsvQueryAsync(CsvQueryRequest request, CancellationToken cancellationToken = default);
}

public class CsvDuckDbService : ICsvDuckDbService
{
    private readonly ILogger<CsvDuckDbService> _logger;
    private const long MaxMemoryBytes = 300 * 1024 * 1024; // 300MB
    private const int PageSize = 1000;

    /// <summary>
    /// Initializes a new instance of the CsvDuckDbService
    /// </summary>
    /// <param name="logger">Logger for recording service activities</param>
    public CsvDuckDbService(ILogger<CsvDuckDbService> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Executes a streaming query against a CSV file with memory monitoring
    /// </summary>
    /// <param name="request">CSV query request containing file and query parameters</param>
    /// <param name="cancellationToken">Token to cancel the operation</param>
    /// <returns>Response containing streaming results and execution metadata</returns>
    public async Task<CsvQueryResponse> ExecuteCsvQueryAsync(CsvQueryRequest request, CancellationToken cancellationToken = default)
    {
        var initialMemory = GC.GetTotalMemory(true);
        var maxMemory = initialMemory;
        string? tempFilePath = null;

        _logger.LogInformation("Starting CSV query execution. Initial memory: {MemoryMB} MB", initialMemory / (1024 * 1024));

        try
        {
            // Save uploaded file temporarily
            tempFilePath = await SaveTempFileAsync(request.CsvFile, cancellationToken);
            
            using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync(cancellationToken);

            // Get available columns first
            var availableColumns = await GetCsvColumnsAsync(connection, tempFilePath, cancellationToken);
            
            // Validate requested columns exist
            var missingColumns = request.Columns.Except(availableColumns, StringComparer.OrdinalIgnoreCase).ToList();
            if (missingColumns.Any())
            {
                return new CsvQueryResponse
                {
                    Success = false,
                    ErrorMessage = $"Column(s) not found: {string.Join(", ", missingColumns)}",
                    InitialMemoryUsage = initialMemory,
                    FinalMemoryUsage = GC.GetTotalMemory(false)
                };
            }

            // Build projection query
            var projectedColumns = request.Columns.Select(c => $"'{c}'").ToList();
            var columnList = string.Join(", ", projectedColumns);
            var query = $"SELECT {columnList} FROM read_csv_auto('{tempFilePath}')";
            
            // Add WHERE clause if provided
            if (!string.IsNullOrEmpty(request.WhereClause))
            {
                query += $" WHERE {request.WhereClause}";
            }
            
            // Add ORDER BY if provided
            if (!string.IsNullOrEmpty(request.OrderBy))
            {
                query += $" ORDER BY {request.OrderBy}";
            }
            
            // Add LIMIT if provided
            if (request.Limit.HasValue)
            {
                query += $" LIMIT {request.Limit.Value}";
            }

            _logger.LogInformation("Executing query: {Query}", query);

            // Check memory after loading
            var memoryAfterLoad = GC.GetTotalMemory(false);
            maxMemory = Math.Max(maxMemory, memoryAfterLoad);
            
            if (memoryAfterLoad > MaxMemoryBytes)
            {
                _logger.LogWarning("Memory limit exceeded after loading CSV: {MemoryMB} MB", memoryAfterLoad / (1024 * 1024));
                return new CsvQueryResponse
                {
                    Success = false,
                    ErrorMessage = $"Memory limit exceeded after loading CSV: {memoryAfterLoad / (1024 * 1024)} MB",
                    InitialMemoryUsage = initialMemory,
                    FinalMemoryUsage = memoryAfterLoad,
                    MaxMemoryUsage = maxMemory
                };
            }

            // Execute streaming query
            var data = StreamQueryResults(connection, query, request.PageSize, cancellationToken);
            var finalMemory = GC.GetTotalMemory(false);
            maxMemory = Math.Max(maxMemory, finalMemory);

            _logger.LogInformation("CSV query completed. Final memory: {MemoryMB} MB, Max memory: {MaxMemoryMB} MB", 
                finalMemory / (1024 * 1024), maxMemory / (1024 * 1024));

            return new CsvQueryResponse
            {
                Success = true,
                InitialMemoryUsage = initialMemory,
                FinalMemoryUsage = finalMemory,
                MaxMemoryUsage = maxMemory,
                Columns = request.Columns,
                QueryExecuted = query,
                Data = data
            };
        }
        catch (Exception ex)
        {
            var errorMemory = GC.GetTotalMemory(false);
            maxMemory = Math.Max(maxMemory, errorMemory);
            _logger.LogError(ex, "Error executing CSV query");
            
            return new CsvQueryResponse
            {
                Success = false,
                ErrorMessage = ex.Message,
                InitialMemoryUsage = initialMemory,
                FinalMemoryUsage = errorMemory,
                MaxMemoryUsage = maxMemory
            };
        }
        finally
        {
            // Clean up temp file
            if (tempFilePath != null && System.IO.File.Exists(tempFilePath))
            {
                try
                {
                    System.IO.File.Delete(tempFilePath);
                    _logger.LogDebug("Deleted temporary file: {FilePath}", tempFilePath);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to delete temporary file: {FilePath}", tempFilePath);
                }
            }
        }
    }

    /// <summary>
    /// Saves uploaded CSV file to a temporary location for processing
    /// </summary>
    /// <param name="file">Uploaded CSV file</param>
    /// <param name="cancellationToken">Token to cancel the operation</param>
    /// <returns>Path to the temporary file</returns>
    private async Task<string> SaveTempFileAsync(IFormFile file, CancellationToken cancellationToken)
    {
        var tempPath = Path.GetTempFileName();
        var tempCsvPath = Path.ChangeExtension(tempPath, ".csv");
        
        // Delete the temp file created by GetTempFileName and use our CSV extension
        System.IO.File.Delete(tempPath);
        
        using var fileStream = new FileStream(tempCsvPath, FileMode.Create, FileAccess.Write);
        await file.CopyToAsync(fileStream, cancellationToken);
        
        _logger.LogDebug("Saved uploaded file to: {FilePath}, Size: {SizeKB} KB", 
            tempCsvPath, file.Length / 1024);
        
        return tempCsvPath;
    }

    /// <summary>
    /// Retrieves column names from a CSV file using DuckDB introspection
    /// </summary>
    /// <param name="connection">Active DuckDB connection</param>
    /// <param name="filePath">Path to the CSV file</param>
    /// <param name="cancellationToken">Token to cancel the operation</param>
    /// <returns>List of column names in the CSV file</returns>
    private async Task<List<string>> GetCsvColumnsAsync(DuckDBConnection connection, string filePath, CancellationToken cancellationToken)
    {
        var query = $"DESCRIBE SELECT * FROM read_csv_auto('{filePath}') LIMIT 1";
        using var command = new DuckDBCommand(query, connection);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        
        var columns = new List<string>();
        while (await reader.ReadAsync(cancellationToken))
        {
            columns.Add(reader.GetString("column_name"));
        }
        
        _logger.LogDebug("CSV columns detected: {Columns}", string.Join(", ", columns));
        return columns;
    }

    /// <summary>
    /// Streams query results with memory monitoring and batching
    /// </summary>
    /// <param name="connection">Active DuckDB connection</param>
    /// <param name="query">SQL query to execute</param>
    /// <param name="pageSize">Number of rows to batch before yielding</param>
    /// <param name="cancellationToken">Token to cancel the operation</param>
    /// <returns>Async enumerable of result rows</returns>
    private async IAsyncEnumerable<Dictionary<string, object?>> StreamQueryResults(
        DuckDBConnection connection, 
        string query, 
        int pageSize, 
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
        var batch = new List<Dictionary<string, object?>>();

        while (await reader.ReadAsync(cancellationToken))
        {
            // Check memory usage periodically
            if (rowCount % 500 == 0)
            {
                var currentMemory = GC.GetTotalMemory(false);
                if (currentMemory > MaxMemoryBytes)
                {
                    _logger.LogWarning("Memory limit exceeded during streaming: {MemoryMB} MB", currentMemory / (1024 * 1024));
                    throw new InvalidOperationException($"Memory limit exceeded during streaming: {currentMemory / (1024 * 1024)} MB");
                }
            }

            var row = new Dictionary<string, object?>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row[columnNames[i]] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            batch.Add(row);
            rowCount++;

            // Yield batch when page size is reached
            if (batch.Count >= pageSize)
            {
                foreach (var item in batch)
                {
                    yield return item;
                }
                batch.Clear();
                
                // Force garbage collection every 5 batches to keep memory low
                if (rowCount % (pageSize * 5) == 0)
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    _logger.LogDebug("Forced GC after {RowCount} rows", rowCount);
                }
            }
        }

        // Yield remaining items
        foreach (var item in batch)
        {
            yield return item;
        }
        
        _logger.LogInformation("Streamed {RowCount} rows total", rowCount);
    }
}