using DuckDB.NET.Data;
using Microsoft.Extensions.Options;
using ReportBuilder.Configuration;
using ReportBuilder.Request;
using System.Data;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ReportBuilder.Service;

public interface IMemorySafeJsonStreamingService
{
    IAsyncEnumerable<string> StreamQueryResultsAsJsonAsync(
        QueryRequest request, 
        CancellationToken cancellationToken = default);
        
    IAsyncEnumerable<string> StreamQueryResultsAsNdjsonAsync(
        QueryRequest request, 
        CancellationToken cancellationToken = default);
}

public class MemorySafeJsonStreamingService : IMemorySafeJsonStreamingService
{
    private readonly ILogger<MemorySafeJsonStreamingService> _logger;
    private readonly JsonStreamingSettings _jsonSettings;
    private readonly JsonSerializerOptions _serializerOptions;
    private const long MaxMemoryBytes = 500 * 1024 * 1024; // 500MB

    public MemorySafeJsonStreamingService(
        ILogger<MemorySafeJsonStreamingService> logger,
        IOptions<JsonStreamingSettings> jsonSettings)
    {
        _logger = logger;
        _jsonSettings = jsonSettings.Value;
        
        _serializerOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = _jsonSettings.EnablePropertyNameCaseInsensitive,
            WriteIndented = _jsonSettings.WriteIndented,
            MaxDepth = _jsonSettings.MaxDepth,
            AllowTrailingCommas = _jsonSettings.AllowTrailingCommas,
            ReferenceHandler = ReferenceHandler.IgnoreCycles, // Handle cyclic references
            DefaultIgnoreCondition = JsonIgnoreCondition.Never,
            NumberHandling = JsonNumberHandling.AllowReadingFromString
        };
    }

    public async IAsyncEnumerable<string> StreamQueryResultsAsJsonAsync(
        QueryRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var totalRows = 0;
        var totalSerializationTime = 0L;
        var startTime = Stopwatch.GetTimestamp();

        _logger.LogInformation("Starting JSON streaming for dataset: {Dataset}", request.DatasetPath);

        // Yield opening bracket
        yield return "[";

        var isFirstRow = true;
        await foreach (var row in StreamDuckDbResultsAsync(request, cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                // Add comma separator for non-first rows
                if (!isFirstRow)
                {
                    yield return ",";
                }

                // Serialize individual row with latency measurement
                var rowJson = await SerializeRowSafelyAsync(row, cancellationToken);
                yield return rowJson;

                isFirstRow = false;
                totalRows++;

                // Log progress periodically
                if (totalRows % 1000 == 0)
                {
                    var elapsed = Stopwatch.GetElapsedTime(startTime);
                    var avgLatency = _jsonSettings.MeasureSerializationLatency ? 
                        totalSerializationTime / (double)totalRows / 10000.0 : 0; // Convert to ms
                    
                    _logger.LogInformation("Processed {RowCount} rows in {ElapsedMs}ms. Avg serialization: {LatencyMs}ms/row", 
                        totalRows, elapsed.TotalMilliseconds, avgLatency);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error serializing row {RowNumber}", totalRows + 1);
                
                // Yield error object instead of crashing
                var errorJson = JsonSerializer.Serialize(new { 
                    __error = true, 
                    __message = ex.Message,
                    __rowNumber = totalRows + 1 
                }, _serializerOptions);
                
                if (!isFirstRow) yield return ",";
                yield return errorJson;
                isFirstRow = false;
                totalRows++;
            }
        }

        // Yield closing bracket
        yield return "]";

        var totalElapsed = Stopwatch.GetElapsedTime(startTime);
        _logger.LogInformation("JSON streaming completed. Total rows: {RowCount}, Total time: {ElapsedMs}ms", 
            totalRows, totalElapsed.TotalMilliseconds);
    }

    public async IAsyncEnumerable<string> StreamQueryResultsAsNdjsonAsync(
        QueryRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var totalRows = 0;
        var totalSerializationTime = 0L;
        var startTime = Stopwatch.GetTimestamp();

        _logger.LogInformation("Starting NDJSON streaming for dataset: {Dataset}", request.DatasetPath);

        await foreach (var row in StreamDuckDbResultsAsync(request, cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                // Serialize individual row with latency measurement
                var rowJson = await SerializeRowSafelyAsync(row, cancellationToken);
                yield return rowJson + "\n"; // NDJSON format: each line is a complete JSON object

                totalRows++;

                // Log progress periodically
                if (totalRows % 1000 == 0)
                {
                    var elapsed = Stopwatch.GetElapsedTime(startTime);
                    var avgLatency = _jsonSettings.MeasureSerializationLatency ? 
                        totalSerializationTime / (double)totalRows / 10000.0 : 0; // Convert to ms
                    
                    _logger.LogInformation("NDJSON processed {RowCount} rows in {ElapsedMs}ms. Avg serialization: {LatencyMs}ms/row", 
                        totalRows, elapsed.TotalMilliseconds, avgLatency);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error serializing row {RowNumber} in NDJSON", totalRows + 1);
                
                // Yield error line in NDJSON format
                var errorJson = JsonSerializer.Serialize(new { 
                    __error = true, 
                    __message = ex.Message,
                    __rowNumber = totalRows + 1 
                }, _serializerOptions);
                
                yield return errorJson + "\n";
                totalRows++;
            }
        }

        var totalElapsed = Stopwatch.GetElapsedTime(startTime);
        _logger.LogInformation("NDJSON streaming completed. Total rows: {RowCount}, Total time: {ElapsedMs}ms", 
            totalRows, totalElapsed.TotalMilliseconds);
    }

    private async Task<string> SerializeRowSafelyAsync(
        Dictionary<string, object?> row, 
        CancellationToken cancellationToken)
    {
        if (_jsonSettings.MeasureSerializationLatency)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                var result = await SerializeRowWithExceptionHandlingAsync(row, cancellationToken);
                sw.Stop();
                return result;
            }
            finally
            {
                sw.Stop();
            }
        }
        else
        {
            return await SerializeRowWithExceptionHandlingAsync(row, cancellationToken);
        }
    }

    private async Task<string> SerializeRowWithExceptionHandlingAsync(
        Dictionary<string, object?> row, 
        CancellationToken cancellationToken)
    {
        try
        {
            // Handle potential large values by checking size first
            var sanitizedRow = SanitizeRowForSerialization(row);
            
            // Use MemoryStream for controlled memory usage
            using var memoryStream = new MemoryStream(_jsonSettings.DefaultBufferSize);
            await JsonSerializer.SerializeAsync(memoryStream, sanitizedRow, _serializerOptions, cancellationToken);
            
            memoryStream.Position = 0;
            using var reader = new StreamReader(memoryStream);
            return await reader.ReadToEndAsync(cancellationToken);
        }
        catch (JsonException ex) when (ex.Message.Contains("cycle"))
        {
            _logger.LogWarning("Cyclic reference detected in row, replacing with safe representation");
            return JsonSerializer.Serialize(new { __error = "cyclic_reference", __original_keys = row.Keys }, _serializerOptions);
        }
        catch (JsonException ex) when (ex.Message.Contains("too large") || ex.Message.Contains("depth"))
        {
            _logger.LogWarning("Value too large or deep for serialization, truncating");
            return JsonSerializer.Serialize(new { __error = "value_too_large", __row_summary = GetRowSummary(row) }, _serializerOptions);
        }
        catch (OutOfMemoryException)
        {
            _logger.LogError("Out of memory during serialization, creating minimal representation");
            return JsonSerializer.Serialize(new { __error = "out_of_memory", __field_count = row.Count }, _serializerOptions);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error during row serialization");
            return JsonSerializer.Serialize(new { __error = "serialization_failed", __exception = ex.GetType().Name }, _serializerOptions);
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
                
                // Handle potentially problematic types
                if (value is byte[] byteArray)
                {
                    // Convert large byte arrays to base64 with size limit
                    if (byteArray.Length > 1024 * 1024) // 1MB limit
                    {
                        sanitized[kvp.Key] = new { __type = "large_binary", __size = byteArray.Length };
                    }
                    else
                    {
                        sanitized[kvp.Key] = Convert.ToBase64String(byteArray);
                    }
                }
                else if (value is string str && str.Length > 100000) // 100KB string limit
                {
                    sanitized[kvp.Key] = new { __type = "large_string", __size = str.Length, __preview = str[..Math.Min(1000, str.Length)] };
                }
                else
                {
                    sanitized[kvp.Key] = value;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Error sanitizing field {FieldName}: {Error}", kvp.Key, ex.Message);
                sanitized[kvp.Key] = new { __error = "sanitization_failed", __type = kvp.Value?.GetType().Name };
            }
        }
        
        return sanitized;
    }

    private object GetRowSummary(Dictionary<string, object?> row)
    {
        return new
        {
            field_count = row.Count,
            field_names = row.Keys.Take(10).ToArray(),
            has_more_fields = row.Count > 10
        };
    }

    private async IAsyncEnumerable<Dictionary<string, object?>> StreamDuckDbResultsAsync(
        QueryRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var initialMemory = GC.GetTotalMemory(true);
        _logger.LogInformation("Starting DuckDB query. Initial memory: {MemoryMB} MB", initialMemory / (1024 * 1024));

        try
        {
            using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync(cancellationToken);

            // Load dataset
            var loadCommand = CreateLoadCommand(request.DatasetPath, request.Projection, request.Filter);
            using var loadCmd = new DuckDBCommand(loadCommand, connection);
            await loadCmd.ExecuteNonQueryAsync(cancellationToken);

            // Check memory after loading
            var memoryAfterLoad = GC.GetTotalMemory(false);
            if (memoryAfterLoad > MaxMemoryBytes)
            {
                throw new InvalidOperationException($"Memory limit exceeded after loading data: {memoryAfterLoad / (1024 * 1024)} MB");
            }

            // Execute streaming query
            await foreach (var row in ExecuteStreamingQueryAsync(connection, request.Query, cancellationToken))
            {
                yield return row;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during DuckDB query execution");
            throw;
        }
    }

    private async IAsyncEnumerable<Dictionary<string, object?>> ExecuteStreamingQueryAsync(
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

        var rowCount = 0;

        while (await reader.ReadAsync(cancellationToken))
        {
            // Check memory usage periodically
            if (rowCount % 1000 == 0)
            {
                var currentMemory = GC.GetTotalMemory(false);
                if (currentMemory > MaxMemoryBytes)
                {
                    throw new InvalidOperationException($"Memory limit exceeded: {currentMemory / (1024 * 1024)} MB");
                }
            }

            var row = new Dictionary<string, object?>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row[columnNames[i]] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            rowCount++;
            yield return row;

            // Periodic garbage collection for large datasets
            if (rowCount % 5000 == 0)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
        }
    }

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

    private string CreateCsvLoadCommand(string datasetPath, string? projection, string? filter)
    {
        var selectClause = string.IsNullOrEmpty(projection) ? "*" : projection;
        var whereClause = string.IsNullOrEmpty(filter) ? "" : $" WHERE {filter}";

        return $"CREATE TABLE dataset AS SELECT {selectClause} FROM read_csv_auto('{datasetPath}'){whereClause}";
    }

    private string CreateParquetLoadCommand(string datasetPath, string? projection, string? filter)
    {
        var selectClause = string.IsNullOrEmpty(projection) ? "*" : projection;
        var whereClause = string.IsNullOrEmpty(filter) ? "" : $" WHERE {filter}";

        return $"CREATE TABLE dataset AS SELECT {selectClause} FROM parquet_scan('{datasetPath}'){whereClause}";
    }
}