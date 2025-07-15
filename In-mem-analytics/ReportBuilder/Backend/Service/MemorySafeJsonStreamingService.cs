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
            ReferenceHandler = ReferenceHandler.IgnoreCycles,
            DefaultIgnoreCondition = JsonIgnoreCondition.Never,
            NumberHandling = JsonNumberHandling.AllowReadingFromString
        };
    }

    public async IAsyncEnumerable<string> StreamQueryResultsAsJsonAsync(
        QueryRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var totalRows = 0;
        var startTime = Stopwatch.GetTimestamp();

        _logger.LogInformation("Starting JSON streaming for dataset: {Dataset}", request.DatasetPath);

        // Yield opening bracket
        yield return "[";

        var isFirstRow = true;
        
        await foreach (var row in StreamDuckDbResultsAsync(request, cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var rowResult = await ProcessRowSafely(row, isFirstRow, totalRows + 1);
            
            if (!string.IsNullOrEmpty(rowResult))
            {
                yield return rowResult;
                isFirstRow = false;
                totalRows++;

                // Log progress periodically
                if (totalRows % 1000 == 0)
                {
                    var elapsed = Stopwatch.GetElapsedTime(startTime);
                    _logger.LogInformation("Processed {RowCount} rows in {ElapsedMs}ms", 
                        totalRows, elapsed.TotalMilliseconds);
                }
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
        var startTime = Stopwatch.GetTimestamp();

        _logger.LogInformation("Starting NDJSON streaming for dataset: {Dataset}", request.DatasetPath);

        await foreach (var row in StreamDuckDbResultsAsync(request, cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var rowJson = await SerializeRowSafely(row);
            yield return rowJson + "\n";

            totalRows++;

            // Log progress periodically
            if (totalRows % 1000 == 0)
            {
                var elapsed = Stopwatch.GetElapsedTime(startTime);
                _logger.LogInformation("NDJSON processed {RowCount} rows in {ElapsedMs}ms", 
                    totalRows, elapsed.TotalMilliseconds);
            }
        }

        var totalElapsed = Stopwatch.GetElapsedTime(startTime);
        _logger.LogInformation("NDJSON streaming completed. Total rows: {RowCount}, Total time: {ElapsedMs}ms", 
            totalRows, totalElapsed.TotalMilliseconds);
    }

    private async Task<string> ProcessRowSafely(Dictionary<string, object?> row, bool isFirstRow, int rowNumber)
    {
        try
        {
            var comma = !isFirstRow ? "," : "";
            var rowJson = await SerializeRowSafely(row);
            return comma + rowJson;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing row {RowNumber}", rowNumber);
            
            var errorJson = JsonSerializer.Serialize(new { 
                __error = true, 
                __message = ex.Message,
                __rowNumber = rowNumber 
            }, _serializerOptions);
            
            var comma = !isFirstRow ? "," : "";
            return comma + errorJson;
        }
    }

    private async Task<string> SerializeRowSafely(Dictionary<string, object?> row)
    {
        try
        {
            var sanitizedRow = SanitizeRowForSerialization(row);
            
            using var memoryStream = new MemoryStream(_jsonSettings.DefaultBufferSize);
            await JsonSerializer.SerializeAsync(memoryStream, sanitizedRow, _serializerOptions);
            
            memoryStream.Position = 0;
            using var reader = new StreamReader(memoryStream);
            return await reader.ReadToEndAsync();
        }
        catch (JsonException ex) when (ex.Message.Contains("cycle"))
        {
            _logger.LogWarning("Cyclic reference detected in row");
            return JsonSerializer.Serialize(new { __error = "cyclic_reference", __original_keys = row.Keys }, _serializerOptions);
        }
        catch (JsonException ex) when (ex.Message.Contains("too large") || ex.Message.Contains("depth"))
        {
            _logger.LogWarning("Value too large or deep for serialization");
            return JsonSerializer.Serialize(new { __error = "value_too_large", __row_summary = GetRowSummary(row) }, _serializerOptions);
        }
        catch (OutOfMemoryException)
        {
            _logger.LogError("Out of memory during serialization");
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
                
                if (value is byte[] byteArray)
                {
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
        using var connection = new DuckDBConnection("Data Source=:memory:");
        await connection.OpenAsync(cancellationToken);

        // Load dataset
        var loadCommand = CreateLoadCommand(request.DatasetPath, request.Projection, request.Filter);
        using var loadCmd = new DuckDBCommand(loadCommand, connection);
        await loadCmd.ExecuteNonQueryAsync(cancellationToken);

        // Execute streaming query
        await foreach (var row in ExecuteStreamingQueryAsync(connection, request.Query, cancellationToken))
        {
            yield return row;
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