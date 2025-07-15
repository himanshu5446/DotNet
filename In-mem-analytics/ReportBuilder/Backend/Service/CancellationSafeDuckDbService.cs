using DuckDB.NET.Data;
using System.Data;
using System.Runtime.CompilerServices;
using System.Collections.Concurrent;

namespace ReportBuilder.Service;

public interface ICancellationSafeDuckDbService
{
    Task<T> ExecuteQueryAsync<T>(
        string query,
        Func<DuckDBDataReader, CancellationToken, Task<T>> resultProcessor,
        CancellationToken cancellationToken = default);

    Task<T> ExecuteQueryWithFileAsync<T>(
        string filePath,
        string query,
        Func<DuckDBDataReader, CancellationToken, Task<T>> resultProcessor,
        string fileFormat = "Auto",
        CancellationToken cancellationToken = default);

    Task ExecuteNonQueryAsync(string query, CancellationToken cancellationToken = default);
}

public class CancellationSafeDuckDbService : ICancellationSafeDuckDbService
{
    private readonly ILogger<CancellationSafeDuckDbService> _logger;
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _activeCancellations = new();
    private readonly ConcurrentDictionary<string, string> _tempFiles = new();

    public CancellationSafeDuckDbService(ILogger<CancellationSafeDuckDbService> logger)
    {
        _logger = logger;
    }

    public async Task<T> ExecuteQueryAsync<T>(
        string query,
        Func<DuckDBDataReader, CancellationToken, Task<T>> resultProcessor,
        CancellationToken cancellationToken = default)
    {
        var queryId = Guid.NewGuid().ToString("N")[..8];
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        
        _activeCancellations[queryId] = linkedCts;
        
        try
        {
            _logger.LogInformation("[QUERY-{QueryId}] Starting query execution", queryId);
            
            using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync(linkedCts.Token);
            
            using var command = new DuckDBCommand(query, connection);
            
            // Set command timeout and cancellation
            command.CommandTimeout = 300; // 5 minutes default
            
            try
            {
                using var reader = await command.ExecuteReaderAsync(linkedCts.Token);
                var result = await resultProcessor(reader, linkedCts.Token);
                
                _logger.LogInformation("[QUERY-{QueryId}] Query completed successfully", queryId);
                return result;
            }
            catch (OperationCanceledException) when (linkedCts.Token.IsCancellationRequested)
            {
                _logger.LogWarning("[CANCELLED] Query {QueryId} aborted by client", queryId);
                throw new TaskCanceledException("[CANCELLED] Query aborted by client");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[QUERY-{QueryId}] Query execution failed", queryId);
                throw;
            }
        }
        finally
        {
            _activeCancellations.TryRemove(queryId, out _);
            linkedCts.Dispose();
        }
    }

    public async Task<T> ExecuteQueryWithFileAsync<T>(
        string filePath,
        string query,
        Func<DuckDBDataReader, CancellationToken, Task<T>> resultProcessor,
        string fileFormat = "Auto",
        CancellationToken cancellationToken = default)
    {
        var queryId = Guid.NewGuid().ToString("N")[..8];
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        
        _activeCancellations[queryId] = linkedCts;
        
        try
        {
            _logger.LogInformation("[QUERY-{QueryId}] Starting query with file: {FilePath}", queryId, filePath);
            
            // Validate file exists
            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException($"File not found: {filePath}");
            }

            using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync(linkedCts.Token);
            
            // Load file into DuckDB with cancellation support
            await LoadFileWithCancellationAsync(connection, filePath, fileFormat, queryId, linkedCts.Token);
            
            // Execute query with cancellation support
            using var command = new DuckDBCommand(query, connection);
            command.CommandTimeout = 300; // 5 minutes default
            
            try
            {
                using var reader = await command.ExecuteReaderAsync(linkedCts.Token);
                var result = await resultProcessor(reader, linkedCts.Token);
                
                _logger.LogInformation("[QUERY-{QueryId}] Query with file completed successfully", queryId);
                return result;
            }
            catch (OperationCanceledException) when (linkedCts.Token.IsCancellationRequested)
            {
                _logger.LogWarning("[CANCELLED] Query {QueryId} aborted by client during execution", queryId);
                await CleanupResourcesAsync(queryId, linkedCts.Token);
                throw new TaskCanceledException("[CANCELLED] Query aborted by client");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[QUERY-{QueryId}] Query with file execution failed", queryId);
                await CleanupResourcesAsync(queryId, CancellationToken.None);
                throw;
            }
        }
        finally
        {
            _activeCancellations.TryRemove(queryId, out _);
            await CleanupResourcesAsync(queryId, CancellationToken.None);
            linkedCts.Dispose();
        }
    }

    public async Task ExecuteNonQueryAsync(string query, CancellationToken cancellationToken = default)
    {
        var queryId = Guid.NewGuid().ToString("N")[..8];
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        
        _activeCancellations[queryId] = linkedCts;
        
        try
        {
            _logger.LogInformation("[QUERY-{QueryId}] Starting non-query execution", queryId);
            
            using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync(linkedCts.Token);
            
            using var command = new DuckDBCommand(query, connection);
            command.CommandTimeout = 300;
            
            try
            {
                await command.ExecuteNonQueryAsync(linkedCts.Token);
                _logger.LogInformation("[QUERY-{QueryId}] Non-query completed successfully", queryId);
            }
            catch (OperationCanceledException) when (linkedCts.Token.IsCancellationRequested)
            {
                _logger.LogWarning("[CANCELLED] Non-query {QueryId} aborted by client", queryId);
                throw new TaskCanceledException("[CANCELLED] Non-query aborted by client");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[QUERY-{QueryId}] Non-query execution failed", queryId);
                throw;
            }
        }
        finally
        {
            _activeCancellations.TryRemove(queryId, out _);
            linkedCts.Dispose();
        }
    }

    private async Task LoadFileWithCancellationAsync(
        DuckDBConnection connection,
        string filePath,
        string fileFormat,
        string queryId,
        CancellationToken cancellationToken)
    {
        try
        {
            var loadCommand = BuildLoadCommand(filePath, fileFormat);
            _logger.LogDebug("[QUERY-{QueryId}] Loading file with command: {Command}", queryId, loadCommand);
            
            using var command = new DuckDBCommand(loadCommand, connection);
            command.CommandTimeout = 300;
            
            await command.ExecuteNonQueryAsync(cancellationToken);
            
            _logger.LogInformation("[QUERY-{QueryId}] File loaded successfully: {FilePath}", queryId, filePath);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning("[CANCELLED] File loading for query {QueryId} aborted by client", queryId);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[QUERY-{QueryId}] Failed to load file: {FilePath}", queryId, filePath);
            throw;
        }
    }

    private string BuildLoadCommand(string filePath, string fileFormat)
    {
        var tableName = "data_table";
        var extension = Path.GetExtension(filePath).ToLower();
        
        return (fileFormat.ToLower(), extension) switch
        {
            ("csv", _) or (_, ".csv") => $"CREATE TABLE {tableName} AS SELECT * FROM read_csv_auto('{filePath}')",
            ("parquet", _) or (_, ".parquet") => $"CREATE TABLE {tableName} AS SELECT * FROM parquet_scan('{filePath}')",
            ("arrow", _) or (_, ".arrow") => $"CREATE TABLE {tableName} AS SELECT * FROM read_parquet('{filePath}')",
            _ => $"CREATE TABLE {tableName} AS SELECT * FROM read_csv_auto('{filePath}')" // Default to CSV
        };
    }

    private async Task CleanupResourcesAsync(string queryId, CancellationToken cancellationToken)
    {
        try
        {
            // Cleanup temp files associated with this query
            var tempFilesToRemove = new List<string>();
            
            foreach (var kvp in _tempFiles)
            {
                if (kvp.Key.StartsWith(queryId))
                {
                    tempFilesToRemove.Add(kvp.Key);
                }
            }
            
            foreach (var tempFileKey in tempFilesToRemove)
            {
                if (_tempFiles.TryRemove(tempFileKey, out var tempFilePath))
                {
                    try
                    {
                        if (File.Exists(tempFilePath))
                        {
                            File.Delete(tempFilePath);
                            _logger.LogDebug("[CLEANUP] Removed temp file: {TempFile}", tempFilePath);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "[CLEANUP] Failed to remove temp file: {TempFile}", tempFilePath);
                    }
                }
            }
            
            // Additional cleanup can be added here
            await Task.CompletedTask;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[CLEANUP] Error during resource cleanup for query {QueryId}", queryId);
        }
    }

    // Helper method to register temp files for cleanup
    public void RegisterTempFile(string queryId, string tempFilePath)
    {
        var key = $"{queryId}_{Guid.NewGuid().ToString("N")[..8]}";
        _tempFiles[key] = tempFilePath;
    }

    // Helper method to get active query count
    public int GetActiveQueryCount()
    {
        return _activeCancellations.Count;
    }

    // Helper method to cancel all active queries (for graceful shutdown)
    public void CancelAllQueries()
    {
        _logger.LogInformation("[SHUTDOWN] Cancelling all active queries ({Count})", _activeCancellations.Count);
        
        foreach (var kvp in _activeCancellations)
        {
            try
            {
                kvp.Value.Cancel();
                _logger.LogDebug("[SHUTDOWN] Cancelled query: {QueryId}", kvp.Key);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[SHUTDOWN] Error cancelling query: {QueryId}", kvp.Key);
            }
        }
        
        _activeCancellations.Clear();
    }
}

// Extension methods for common query patterns
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

    private static async IAsyncEnumerable<Dictionary<string, object?>> StreamResults(
        DuckDBDataReader reader,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (await reader.ReadAsync(cancellationToken))
        {
            var row = new Dictionary<string, object?>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }
            yield return row;
        }
    }
}