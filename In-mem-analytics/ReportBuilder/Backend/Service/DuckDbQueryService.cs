using DuckDB.NET.Data;
using ReportBuilder.Request;
using ReportBuilder.Response;
using System.Data;
using System.Runtime.CompilerServices;

namespace ReportBuilder.Service;

/// <summary>
/// Service interface for executing DuckDB queries with memory management
/// </summary>
public interface IDuckDbQueryService
{
    /// <summary>
    /// Executes a query against a dataset with memory monitoring and streaming results
    /// </summary>
    /// <param name="request">Query request containing dataset path and query details</param>
    /// <param name="cancellationToken">Token to cancel the operation</param>
    /// <returns>Query response with results or error information</returns>
    Task<QueryResponse> ExecuteQueryAsync(QueryRequest request, CancellationToken cancellationToken = default);
}

public class DuckDbQueryService : IDuckDbQueryService
{
    private readonly ILogger<DuckDbQueryService> _logger;
    private const long MaxMemoryBytes = 500 * 1024 * 1024; // 500MB

    /// <summary>
    /// Initializes a new instance of the DuckDbQueryService
    /// </summary>
    /// <param name="logger">Logger for recording service activities</param>
    public DuckDbQueryService(ILogger<DuckDbQueryService> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Executes a query with memory monitoring and streaming results
    /// </summary>
    /// <param name="request">Query request with dataset path, query, and optional parameters</param>
    /// <param name="cancellationToken">Token to cancel the operation</param>
    /// <returns>Query response containing streamed results or error details</returns>
    public async Task<QueryResponse> ExecuteQueryAsync(QueryRequest request, CancellationToken cancellationToken = default)
    {
        var initialMemory = GC.GetTotalMemory(true);
        _logger.LogInformation("Initial memory usage: {MemoryMB} MB", initialMemory / (1024 * 1024));

        try
        {
            using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync(cancellationToken);

            // Load dataset based on file extension
            var loadCommand = CreateLoadCommand(request.DatasetPath, request.Projection, request.Filter);
            using var loadCmd = new DuckDBCommand(loadCommand, connection);
            await loadCmd.ExecuteNonQueryAsync(cancellationToken);

            // Check memory after loading
            var memoryAfterLoad = GC.GetTotalMemory(false);
            if (memoryAfterLoad > MaxMemoryBytes)
            {
                _logger.LogWarning("Memory limit exceeded after loading data: {MemoryMB} MB", memoryAfterLoad / (1024 * 1024));
                return new QueryResponse
                {
                    Success = false,
                    ErrorMessage = $"Memory limit exceeded after loading data: {memoryAfterLoad / (1024 * 1024)} MB",
                    InitialMemoryUsage = initialMemory,
                    FinalMemoryUsage = memoryAfterLoad
                };
            }

            // Execute query with streaming
            var data = ExecuteStreamingQuery(connection, request.Query, request.PageSize, cancellationToken);
            var finalMemory = GC.GetTotalMemory(false);

            _logger.LogInformation("Final memory usage: {MemoryMB} MB", finalMemory / (1024 * 1024));

            return new QueryResponse
            {
                Success = true,
                InitialMemoryUsage = initialMemory,
                FinalMemoryUsage = finalMemory,
                Data = data
            };
        }
        catch (Exception ex)
        {
            var errorMemory = GC.GetTotalMemory(false);
            _logger.LogError(ex, "Error executing query");
            
            return new QueryResponse
            {
                Success = false,
                ErrorMessage = ex.Message,
                InitialMemoryUsage = initialMemory,
                FinalMemoryUsage = errorMemory
            };
        }
    }

    /// <summary>
    /// Creates appropriate load command based on file extension (CSV or Parquet)
    /// </summary>
    /// <param name="datasetPath">Path to the dataset file</param>
    /// <param name="projection">Optional column projection</param>
    /// <param name="filter">Optional WHERE clause filter</param>
    /// <returns>DuckDB command string for loading the dataset</returns>
    private string CreateLoadCommand(string datasetPath, string? projection, string? filter)
    {
        var extension = Path.GetExtension(datasetPath).ToLower();
        
        return extension switch
        {
            ".csv" => CreateCsvLoadCommand(datasetPath, projection, filter),
            ".parquet" => CreateParquetLoadCommand(datasetPath, projection, filter),
            _ => throw new ArgumentException($"Unsupported file format: {extension}")
        };
    }

    /// <summary>
    /// Creates DuckDB command for loading CSV files with optional projection and filtering
    /// </summary>
    /// <param name="datasetPath">Path to the CSV file</param>
    /// <param name="projection">Optional column projection</param>
    /// <param name="filter">Optional WHERE clause filter</param>
    /// <returns>DuckDB command string for CSV loading</returns>
    private string CreateCsvLoadCommand(string datasetPath, string? projection, string? filter)
    {
        var selectClause = string.IsNullOrEmpty(projection) ? "*" : projection;
        var whereClause = string.IsNullOrEmpty(filter) ? "" : $" WHERE {filter}";
        
        return $"CREATE TABLE dataset AS SELECT {selectClause} FROM read_csv_auto('{datasetPath}'){whereClause}";
    }

    /// <summary>
    /// Creates DuckDB command for loading Parquet files with optional projection and filtering
    /// </summary>
    /// <param name="datasetPath">Path to the Parquet file</param>
    /// <param name="projection">Optional column projection</param>
    /// <param name="filter">Optional WHERE clause filter</param>
    /// <returns>DuckDB command string for Parquet loading</returns>
    private string CreateParquetLoadCommand(string datasetPath, string? projection, string? filter)
    {
        var selectClause = string.IsNullOrEmpty(projection) ? "*" : projection;
        var whereClause = string.IsNullOrEmpty(filter) ? "" : $" WHERE {filter}";
        
        return $"CREATE TABLE dataset AS SELECT {selectClause} FROM parquet_scan('{datasetPath}'){whereClause}";
    }

    /// <summary>
    /// Executes query with streaming results and memory monitoring
    /// </summary>
    /// <param name="connection">Active DuckDB connection</param>
    /// <param name="query">SQL query to execute</param>
    /// <param name="pageSize">Number of rows to batch before yielding</param>
    /// <param name="cancellationToken">Token to cancel the operation</param>
    /// <returns>Async enumerable of result rows</returns>
    private async IAsyncEnumerable<Dictionary<string, object?>> ExecuteStreamingQuery(
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
            if (rowCount % 100 == 0)
            {
                var currentMemory = GC.GetTotalMemory(false);
                if (currentMemory > MaxMemoryBytes)
                {
                    _logger.LogWarning("Memory limit exceeded during query execution: {MemoryMB} MB", currentMemory / (1024 * 1024));
                    throw new InvalidOperationException($"Memory limit exceeded during query execution: {currentMemory / (1024 * 1024)} MB");
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
                
                // Force garbage collection to keep memory usage low
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
    }
}