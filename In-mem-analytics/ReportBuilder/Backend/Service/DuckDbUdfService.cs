using DuckDB.NET.Data;
using System.Data;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Text;

namespace ReportBuilder.Service;

public interface IDuckDbUdfService
{
    Task<T> ExecuteQueryWithUdfAsync<T>(
        string csvFilePath,
        string query,
        Func<DuckDBDataReader, CancellationToken, Task<T>> resultProcessor,
        CancellationToken cancellationToken = default);

    Task<IAsyncEnumerable<Dictionary<string, object?>>> StreamQueryWithUdfAsync(
        string csvFilePath,
        string query,
        int batchSize = 10000,
        CancellationToken cancellationToken = default);

    Task RegisterCustomUdfsAsync(DuckDBConnection connection, CancellationToken cancellationToken = default);
}

public class DuckDbUdfService : IDuckDbUdfService
{
    private readonly ILogger<DuckDbUdfService> _logger;
    private const long MaxMemoryBytes = 500 * 1024 * 1024; // 500MB
    private const int DefaultBatchSize = 10000;

    public DuckDbUdfService(ILogger<DuckDbUdfService> logger)
    {
        _logger = logger;
    }

    public async Task<T> ExecuteQueryWithUdfAsync<T>(
        string csvFilePath,
        string query,
        Func<DuckDBDataReader, CancellationToken, Task<T>> resultProcessor,
        CancellationToken cancellationToken = default)
    {
        var queryId = Guid.NewGuid().ToString("N")[..8];
        var initialMemory = GC.GetTotalMemory(true);
        
        _logger.LogInformation("[UDF-{QueryId}] Starting UDF query execution", queryId);
        _logger.LogInformation("[UDF-{QueryId}] Memory usage before: {MemoryMB} MB", queryId, initialMemory / (1024 * 1024));

        try
        {
            // Validate file exists
            if (!File.Exists(csvFilePath))
            {
                throw new FileNotFoundException($"CSV file not found: {csvFilePath}");
            }

            var fileInfo = new FileInfo(csvFilePath);
            _logger.LogInformation("[UDF-{QueryId}] Processing CSV file: {FilePath}, Size: {SizeKB} KB", 
                queryId, csvFilePath, fileInfo.Length / 1024);

            using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync(cancellationToken);

            // Register custom UDFs
            await RegisterCustomUdfsAsync(connection, cancellationToken);

            // Load CSV into DuckDB
            await LoadCsvFileAsync(connection, csvFilePath, queryId, cancellationToken);

            // Check memory after loading
            var memoryAfterLoad = GC.GetTotalMemory(false);
            _logger.LogInformation("[UDF-{QueryId}] Memory usage after CSV load: {MemoryMB} MB", 
                queryId, memoryAfterLoad / (1024 * 1024));

            if (memoryAfterLoad > MaxMemoryBytes)
            {
                _logger.LogWarning("[UDF-{QueryId}] Memory usage exceeded limit after CSV load: {MemoryMB} MB > {LimitMB} MB", 
                    queryId, memoryAfterLoad / (1024 * 1024), MaxMemoryBytes / (1024 * 1024));
            }

            // Execute query with UDF
            _logger.LogDebug("[UDF-{QueryId}] Executing query: {Query}", queryId, query);
            
            using var command = new DuckDBCommand(query, connection);
            command.CommandTimeout = 600; // 10 minutes for large datasets
            
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            var result = await resultProcessor((DuckDBDataReader)reader, cancellationToken);

            var finalMemory = GC.GetTotalMemory(false);
            _logger.LogInformation("[UDF-{QueryId}] Memory usage after query: {MemoryMB} MB", 
                queryId, finalMemory / (1024 * 1024));
            _logger.LogInformation("[UDF-{QueryId}] Memory delta: {DeltaMB} MB", 
                queryId, (finalMemory - initialMemory) / (1024 * 1024));

            return result;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning("[CANCELLED] UDF query {QueryId} aborted by client", queryId);
            throw;
        }
        catch (Exception ex)
        {
            var errorMemory = GC.GetTotalMemory(false);
            _logger.LogError(ex, "[UDF-{QueryId}] Error executing UDF query. Memory: {MemoryMB} MB", 
                queryId, errorMemory / (1024 * 1024));
            throw;
        }
    }

    public async Task<IAsyncEnumerable<Dictionary<string, object?>>> StreamQueryWithUdfAsync(
        string csvFilePath,
        string query,
        int batchSize = DefaultBatchSize,
        CancellationToken cancellationToken = default)
    {
        return await ExecuteQueryWithUdfAsync(csvFilePath, query, async (reader, ct) =>
        {
            return StreamResultsWithMemoryMonitoring(reader, batchSize, ct);
        }, cancellationToken);
    }

    public async Task RegisterCustomUdfsAsync(DuckDBConnection connection, CancellationToken cancellationToken = default)
    {
        _logger.LogDebug("Registering custom UDFs...");

        try
        {
            // Note: DuckDB.NET doesn't directly support registering C# functions as UDFs
            // Instead, we'll create UDFs using SQL functions that call built-in functions
            
            var udfCommands = new[]
            {
                // NormalizeText UDF - comprehensive text normalization
                @"CREATE OR REPLACE FUNCTION NormalizeText(input_text VARCHAR) 
                  RETURNS VARCHAR AS (
                    CASE 
                      WHEN input_text IS NULL THEN NULL
                      ELSE TRIM(
                        REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            REGEXP_REPLACE(
                              LOWER(input_text), 
                              '[^a-z0-9\s]', '', 'g'
                            ), 
                            '\s+', ' ', 'g'
                          ), 
                          '^\s+|\s+$', '', 'g'
                        )
                      )
                    END
                  )",

                // CleanPhone UDF - phone number normalization
                @"CREATE OR REPLACE FUNCTION CleanPhone(phone_text VARCHAR)
                  RETURNS VARCHAR AS (
                    CASE 
                      WHEN phone_text IS NULL THEN NULL
                      ELSE REGEXP_REPLACE(phone_text, '[^0-9]', '', 'g')
                    END
                  )",

                // ExtractDomain UDF - extract domain from email
                @"CREATE OR REPLACE FUNCTION ExtractDomain(email VARCHAR)
                  RETURNS VARCHAR AS (
                    CASE 
                      WHEN email IS NULL OR email = '' THEN NULL
                      WHEN POSITION('@' IN email) = 0 THEN NULL
                      ELSE LOWER(SUBSTRING(email FROM POSITION('@' IN email) + 1))
                    END
                  )",

                // SafeSubstring UDF - safe substring with bounds checking
                @"CREATE OR REPLACE FUNCTION SafeSubstring(input_text VARCHAR, start_pos INTEGER, length INTEGER)
                  RETURNS VARCHAR AS (
                    CASE 
                      WHEN input_text IS NULL OR start_pos < 1 OR length < 0 THEN NULL
                      WHEN start_pos > LENGTH(input_text) THEN ''
                      ELSE SUBSTRING(input_text FROM start_pos FOR length)
                    END
                  )",

                // WordCount UDF - count words in text
                @"CREATE OR REPLACE FUNCTION WordCount(input_text VARCHAR)
                  RETURNS INTEGER AS (
                    CASE 
                      WHEN input_text IS NULL OR TRIM(input_text) = '' THEN 0
                      ELSE ARRAY_LENGTH(STRING_SPLIT(REGEXP_REPLACE(TRIM(input_text), '\s+', ' ', 'g'), ' '))
                    END
                  )"
            };

            foreach (var udfCommand in udfCommands)
            {
                using var command = new DuckDBCommand(udfCommand, connection);
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            _logger.LogInformation("Successfully registered {Count} custom UDFs", udfCommands.Length);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to register custom UDFs");
            throw;
        }
    }

    private async Task LoadCsvFileAsync(
        DuckDBConnection connection, 
        string csvFilePath, 
        string queryId,
        CancellationToken cancellationToken)
    {
        try
        {
            var loadCommand = $"CREATE TABLE input_data AS SELECT * FROM read_csv_auto('{csvFilePath}')";
            _logger.LogDebug("[UDF-{QueryId}] Loading CSV with command: {Command}", queryId, loadCommand);

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            using var command = new DuckDBCommand(loadCommand, connection);
            command.CommandTimeout = 600; // 10 minutes for large files
            await command.ExecuteNonQueryAsync(cancellationToken);
            
            stopwatch.Stop();

            // Get row count
            using var countCommand = new DuckDBCommand("SELECT COUNT(*) FROM input_data", connection);
            var rowCount = Convert.ToInt64(await countCommand.ExecuteScalarAsync(cancellationToken));

            _logger.LogInformation("[UDF-{QueryId}] CSV loaded successfully. Rows: {RowCount:N0}, Load time: {LoadTime:F2}s", 
                queryId, rowCount, stopwatch.Elapsed.TotalSeconds);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[UDF-{QueryId}] Failed to load CSV file: {FilePath}", queryId, csvFilePath);
            throw;
        }
    }

    private async IAsyncEnumerable<Dictionary<string, object?>> StreamResultsWithMemoryMonitoring(
        DuckDBDataReader reader,
        int batchSize,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var processedRows = 0L;
        var batchStartTime = DateTime.UtcNow;
        var totalStartTime = DateTime.UtcNow;
        var lastMemoryCheck = GC.GetTotalMemory(false);

        // Get column names
        var columnNames = new string[reader.FieldCount];
        for (int i = 0; i < reader.FieldCount; i++)
        {
            columnNames[i] = reader.GetName(i);
        }

        _logger.LogDebug("[UDF] Starting to stream results. Columns: {ColumnCount}, Batch size: {BatchSize}", 
            reader.FieldCount, batchSize);

        var results = new List<Dictionary<string, object?>>();
        
        while (await reader.ReadAsync(cancellationToken))
        {
            var row = new Dictionary<string, object?>();
            
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row[columnNames[i]] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            results.Add(row);
            processedRows++;

            // Yield batch when full
            if (results.Count >= batchSize)
            {
                foreach (var item in results)
                {
                    yield return item;
                }
                results.Clear();

                // Performance monitoring
                var currentTime = DateTime.UtcNow;
                var batchDuration = currentTime - batchStartTime;
                var currentMemory = GC.GetTotalMemory(false);
                var memoryDelta = currentMemory - lastMemoryCheck;
                var rowsPerSecond = batchSize / Math.Max(batchDuration.TotalSeconds, 0.001);
                
                _logger.LogInformation("[UDF] Processed {ProcessedRows:N0} rows. " +
                    "Batch time: {BatchTime:F2}s, Rate: {Rate:F0} rows/sec, " +
                    "Memory: {MemoryMB} MB (Δ{DeltaMB:+#;-#;0} MB)", 
                    processedRows, batchDuration.TotalSeconds, rowsPerSecond,
                    currentMemory / (1024 * 1024), memoryDelta / (1024 * 1024));

                // Check memory limit
                if (currentMemory > MaxMemoryBytes)
                {
                    _logger.LogWarning("[UDF] Memory limit exceeded: {MemoryMB} MB > {LimitMB} MB. " +
                        "Processed {ProcessedRows:N0} rows before cancellation.", 
                        currentMemory / (1024 * 1024), MaxMemoryBytes / (1024 * 1024), processedRows);
                    
                    throw new InvalidOperationException($"Memory limit exceeded: {currentMemory / (1024 * 1024)} MB > {MaxMemoryBytes / (1024 * 1024)} MB");
                }

                batchStartTime = currentTime;
                lastMemoryCheck = currentMemory;

                // Force garbage collection every 100k rows
                if (processedRows % (batchSize * 10) == 0)
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    var memoryAfterGc = GC.GetTotalMemory(true);
                    _logger.LogDebug("[UDF] Forced GC at {ProcessedRows:N0} rows. Memory: {MemoryMB} MB", 
                        processedRows, memoryAfterGc / (1024 * 1024));
                }
            }
        }

        // Yield remaining items
        foreach (var item in results)
        {
            yield return item;
        }

        var totalDuration = DateTime.UtcNow - totalStartTime;
        var overallRate = processedRows / Math.Max(totalDuration.TotalSeconds, 0.001);
        var finalMemory = GC.GetTotalMemory(false);

        _logger.LogInformation("[UDF] Streaming completed. Total rows: {TotalRows:N0}, " +
            "Duration: {Duration:F2}s, Overall rate: {Rate:F0} rows/sec, " +
            "Final memory: {MemoryMB} MB", 
            processedRows, totalDuration.TotalSeconds, overallRate, finalMemory / (1024 * 1024));
    }
}

// Extension methods for common UDF patterns
public static class DuckDbUdfExtensions
{
    public static async Task<List<Dictionary<string, object?>>> ExecuteUdfToListAsync(
        this IDuckDbUdfService service,
        string csvFilePath,
        string query,
        CancellationToken cancellationToken = default)
    {
        return await service.ExecuteQueryWithUdfAsync(csvFilePath, query, async (reader, ct) =>
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

    public static async Task<object?> ExecuteUdfScalarAsync(
        this IDuckDbUdfService service,
        string csvFilePath,
        string query,
        CancellationToken cancellationToken = default)
    {
        return await service.ExecuteQueryWithUdfAsync(csvFilePath, query, async (reader, ct) =>
        {
            if (await reader.ReadAsync(ct))
            {
                return reader.IsDBNull(0) ? null : reader.GetValue(0);
            }
            return null;
        }, cancellationToken);
    }
}