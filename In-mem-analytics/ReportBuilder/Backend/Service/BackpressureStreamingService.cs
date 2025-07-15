using DuckDB.NET.Data;
using Microsoft.Extensions.Options;
using ReportBuilder.Configuration;
using ReportBuilder.Request;
using System.Data;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace ReportBuilder.Service;

/// <summary>
/// Interface for streaming query results with advanced backpressure management and flow control.
/// Provides efficient streaming with configurable throttling and memory-safe operations.
/// </summary>
public interface IBackpressureStreamingService
{
    /// <summary>
    /// Streams query results with intelligent backpressure management and flow control.
    /// </summary>
    /// <param name="request">Query request containing SQL and dataset information</param>
    /// <param name="cancellationToken">Token for cancelling the streaming operation</param>
    /// <returns>Asynchronous enumerable of dictionary rows with backpressure-controlled delivery</returns>
    IAsyncEnumerable<Dictionary<string, object?>> StreamQueryResultsAsync(
        QueryRequest request, 
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Advanced streaming service with intelligent backpressure management and flow control.
/// Uses bounded channels and configurable throttling to prevent memory issues and system overload.
/// </summary>
public class BackpressureStreamingService : IBackpressureStreamingService
{
    private readonly ILogger<BackpressureStreamingService> _logger;
    private readonly BackpressureSettings _settings;
    private const long MaxMemoryBytes = 500 * 1024 * 1024; // 500MB

    /// <summary>
    /// Initializes a new instance of the BackpressureStreamingService.
    /// </summary>
    /// <param name="logger">Logger for recording streaming operations and backpressure events</param>
    /// <param name="settings">Configuration settings for backpressure behavior and throttling</param>
    public BackpressureStreamingService(
        ILogger<BackpressureStreamingService> logger,
        IOptions<BackpressureSettings> settings)
    {
        _logger = logger;
        _settings = settings.Value;
    }

    /// <summary>
    /// Streams query results using bounded channels with intelligent backpressure management.
    /// Implements producer-consumer pattern with configurable throttling and memory monitoring.
    /// </summary>
    /// <param name="request">Query request containing SQL query and dataset information</param>
    /// <param name="cancellationToken">Token for cancelling the streaming operation</param>
    /// <returns>Asynchronous enumerable of dictionary rows with controlled flow rate</returns>
    public async IAsyncEnumerable<Dictionary<string, object?>> StreamQueryResultsAsync(
        QueryRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = Channel.CreateBounded<Dictionary<string, object?>>(new BoundedChannelOptions(_settings.MaxChannelBufferSize)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true
        });

        var reader = channel.Reader;
        var writer = channel.Writer;

        // Start the producer task
        var producerTask = ProduceDataAsync(request, writer, cancellationToken);

        // Consume data with backpressure control
        await foreach (var item in reader.ReadAllAsync(cancellationToken))
        {
            // Apply throttling if enabled
            if (_settings.EnableThrottling && _settings.ThrottleDelayMs > 0)
            {
                await Task.Delay(_settings.ThrottleDelayMs, cancellationToken);
            }

            // Check queue threshold
            if (reader.Count > _settings.MaxQueueThreshold)
            {
                _logger.LogWarning("Queue threshold exceeded: {Count} items. Cancelling query.", reader.Count);
                writer.TryComplete();
                throw new OperationCanceledException("Queue threshold exceeded, cancelling query to prevent memory issues");
            }

            yield return item;
        }

        // Wait for producer to complete
        try
        {
            await producerTask;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error from producer task");
            throw;
        }
    }

    /// <summary>
    /// Producer task that executes the query and feeds results into the bounded channel.
    /// Handles data loading, query execution, and writing to channel with memory monitoring.
    /// </summary>
    /// <param name="request">Query request to execute</param>
    /// <param name="writer">Channel writer for sending results to the consumer</param>
    /// <param name="cancellationToken">Token for cancelling the production operation</param>
    /// <returns>Task representing the data production operation</returns>
    private async Task ProduceDataAsync(
        QueryRequest request,
        ChannelWriter<Dictionary<string, object?>> writer,
        CancellationToken cancellationToken)
    {
        var initialMemory = GC.GetTotalMemory(true);
        _logger.LogInformation("Starting data production. Initial memory: {MemoryMB} MB", initialMemory / (1024 * 1024));

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
                _logger.LogWarning("Memory limit exceeded after loading data: {MemoryMB} MB", memoryAfterLoad / (1024 * 1024));
                throw new InvalidOperationException($"Memory limit exceeded: {memoryAfterLoad / (1024 * 1024)} MB");
            }

            // Execute streaming query
            var rowCount = 0;
            await foreach (var row in ExecuteStreamingQueryAsync(connection, request.Query, cancellationToken))
            {
                rowCount++;
                
                // Log throttling periodically (can't check exact count on writer)
                if (rowCount % 1000 == 0)
                {
                    _logger.LogInformation("Processed {RowCount} rows. Backpressure may be active if channel is full.", rowCount);
                }

                // Write to channel (this will block if channel is full, providing backpressure)
                await writer.WriteAsync(row, cancellationToken);
            }

            writer.TryComplete();
            _logger.LogInformation("Data production completed successfully");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during data production");
            writer.TryComplete(ex);
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
                    _logger.LogWarning("Memory limit exceeded during query execution: {MemoryMB} MB", currentMemory / (1024 * 1024));
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

            // Periodic garbage collection
            if (rowCount % 5000 == 0)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
        }

        _logger.LogInformation("Processed {RowCount} rows from query", rowCount);
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