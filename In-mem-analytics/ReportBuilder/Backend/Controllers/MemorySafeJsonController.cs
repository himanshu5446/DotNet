using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using ReportBuilder.Configuration;
using ReportBuilder.Request;
using ReportBuilder.Service;
using System.Diagnostics;

namespace ReportBuilder.Controllers;

/// <summary>
/// Controller for memory-safe JSON streaming operations with multiple output formats.
/// Provides endpoints for streaming query results as JSON, NDJSON, and chunked JSON with performance monitoring.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public class MemorySafeJsonController : ControllerBase
{
    private readonly IMemorySafeJsonStreamingService _jsonStreamingService;
    private readonly ILogger<MemorySafeJsonController> _logger;
    private readonly JsonStreamingSettings _jsonSettings;

    /// <summary>
    /// Initializes a new instance of the MemorySafeJsonController.
    /// </summary>
    /// <param name="jsonStreamingService">Service for handling memory-safe JSON streaming operations</param>
    /// <param name="logger">Logger for recording streaming performance and events</param>
    /// <param name="jsonSettings">Configuration settings for JSON streaming behavior</param>
    public MemorySafeJsonController(
        IMemorySafeJsonStreamingService jsonStreamingService,
        ILogger<MemorySafeJsonController> logger,
        IOptions<JsonStreamingSettings> jsonSettings)
    {
        _jsonStreamingService = jsonStreamingService;
        _logger = logger;
        _jsonSettings = jsonSettings.Value;
    }

    /// <summary>
    /// Streams query results as a single JSON array with memory-safe processing.
    /// Provides real-time throughput monitoring and cancellation support.
    /// </summary>
    /// <param name="request">Query request containing SQL and dataset information</param>
    /// <param name="cancellationToken">Token for cancelling the streaming operation</param>
    [HttpPost("stream-json")]
    public async Task StreamJsonResults([FromBody] QueryRequest request, CancellationToken cancellationToken)
    {
        Response.ContentType = "application/json";
        Response.Headers.Add("Cache-Control", "no-cache");
        Response.Headers.Add("Connection", "keep-alive");
        Response.Headers.Add("X-Content-Type-Options", "nosniff");

        var startTime = Stopwatch.GetTimestamp();
        var chunkCount = 0;
        var totalBytes = 0L;

        _logger.LogInformation("Starting memory-safe JSON streaming for dataset: {Dataset}", request.DatasetPath);

        try
        {
            await foreach (var jsonChunk in _jsonStreamingService.StreamQueryResultsAsJsonAsync(request, cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();

                var chunkBytes = System.Text.Encoding.UTF8.GetBytes(jsonChunk);
                await Response.Body.WriteAsync(chunkBytes, cancellationToken);
                await Response.Body.FlushAsync(cancellationToken);

                chunkCount++;
                totalBytes += chunkBytes.Length;

                // Log progress for large responses
                if (chunkCount % 1000 == 0)
                {
                    var elapsed = Stopwatch.GetElapsedTime(startTime);
                    var throughputMbps = (totalBytes / (1024.0 * 1024.0)) / elapsed.TotalSeconds;
                    
                    _logger.LogInformation("Streamed {ChunkCount} chunks, {TotalMB} MB in {ElapsedMs}ms. Throughput: {ThroughputMbps:F2} MB/s",
                        chunkCount, totalBytes / (1024.0 * 1024.0), elapsed.TotalMilliseconds, throughputMbps);
                }
            }

            var totalElapsed = Stopwatch.GetElapsedTime(startTime);
            var finalThroughput = (totalBytes / (1024.0 * 1024.0)) / totalElapsed.TotalSeconds;
            
            _logger.LogInformation("JSON streaming completed. Chunks: {ChunkCount}, Size: {TotalMB} MB, Time: {ElapsedMs}ms, Throughput: {ThroughputMbps:F2} MB/s",
                chunkCount, totalBytes / (1024.0 * 1024.0), totalElapsed.TotalMilliseconds, finalThroughput);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("JSON streaming cancelled after {ChunkCount} chunks ({TotalMB} MB)", 
                chunkCount, totalBytes / (1024.0 * 1024.0));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during JSON streaming after {ChunkCount} chunks", chunkCount);
            
            // Write error in valid JSON format
            var errorBytes = System.Text.Encoding.UTF8.GetBytes($",{{\"__stream_error\":\"{ex.Message}\"}}]");
            await Response.Body.WriteAsync(errorBytes, cancellationToken);
        }
    }

    /// <summary>
    /// Streams query results as Newline Delimited JSON (NDJSON) format.
    /// Each result row is written as a separate JSON line for efficient processing.
    /// </summary>
    /// <param name="request">Query request containing SQL and dataset information</param>
    /// <param name="cancellationToken">Token for cancelling the streaming operation</param>
    [HttpPost("stream-ndjson")]
    public async Task StreamNdjsonResults([FromBody] QueryRequest request, CancellationToken cancellationToken)
    {
        Response.ContentType = "application/x-ndjson";
        Response.Headers.Add("Cache-Control", "no-cache");
        Response.Headers.Add("Connection", "keep-alive");
        Response.Headers.Add("X-Content-Type-Options", "nosniff");

        var startTime = Stopwatch.GetTimestamp();
        var lineCount = 0;
        var totalBytes = 0L;

        _logger.LogInformation("Starting memory-safe NDJSON streaming for dataset: {Dataset}", request.DatasetPath);

        try
        {
            await foreach (var ndjsonLine in _jsonStreamingService.StreamQueryResultsAsNdjsonAsync(request, cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();

                var lineBytes = System.Text.Encoding.UTF8.GetBytes(ndjsonLine);
                await Response.Body.WriteAsync(lineBytes, cancellationToken);
                await Response.Body.FlushAsync(cancellationToken);

                lineCount++;
                totalBytes += lineBytes.Length;

                // Log progress for large responses
                if (lineCount % 1000 == 0)
                {
                    var elapsed = Stopwatch.GetElapsedTime(startTime);
                    var throughputMbps = (totalBytes / (1024.0 * 1024.0)) / elapsed.TotalSeconds;
                    
                    _logger.LogInformation("Streamed {LineCount} lines, {TotalMB} MB in {ElapsedMs}ms. Throughput: {ThroughputMbps:F2} MB/s",
                        lineCount, totalBytes / (1024.0 * 1024.0), elapsed.TotalMilliseconds, throughputMbps);
                }
            }

            var totalElapsed = Stopwatch.GetElapsedTime(startTime);
            var finalThroughput = (totalBytes / (1024.0 * 1024.0)) / totalElapsed.TotalSeconds;
            
            _logger.LogInformation("NDJSON streaming completed. Lines: {LineCount}, Size: {TotalMB} MB, Time: {ElapsedMs}ms, Throughput: {ThroughputMbps:F2} MB/s",
                lineCount, totalBytes / (1024.0 * 1024.0), totalElapsed.TotalMilliseconds, finalThroughput);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("NDJSON streaming cancelled after {LineCount} lines ({TotalMB} MB)", 
                lineCount, totalBytes / (1024.0 * 1024.0));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during NDJSON streaming after {LineCount} lines", lineCount);
            
            // Write error as NDJSON line
            var errorLine = $"{{\"__stream_error\":\"{ex.Message}\",\"__line_number\":{lineCount}}}\n";
            var errorBytes = System.Text.Encoding.UTF8.GetBytes(errorLine);
            await Response.Body.WriteAsync(errorBytes, cancellationToken);
        }
    }

    /// <summary>
    /// Streams query results as JSON using HTTP chunked transfer encoding.
    /// Optimizes client responsiveness by forcing chunk boundaries at regular intervals.
    /// </summary>
    /// <param name="request">Query request containing SQL and dataset information</param>
    /// <param name="cancellationToken">Token for cancelling the streaming operation</param>
    [HttpPost("stream-json-chunked")]
    public async Task StreamJsonResultsChunked([FromBody] QueryRequest request, CancellationToken cancellationToken)
    {
        Response.ContentType = "application/json";
        Response.Headers.Add("Transfer-Encoding", "chunked");
        Response.Headers.Add("Cache-Control", "no-cache");
        Response.Headers.Add("Connection", "keep-alive");

        var startTime = Stopwatch.GetTimestamp();
        var chunkCount = 0;
        var rowsInChunk = 0;

        _logger.LogInformation("Starting chunked JSON streaming for dataset: {Dataset}", request.DatasetPath);

        try
        {
            await foreach (var jsonChunk in _jsonStreamingService.StreamQueryResultsAsJsonAsync(request, cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();

                await Response.WriteAsync(jsonChunk, cancellationToken);
                
                // Force chunk boundary every few items for better client responsiveness
                if (jsonChunk.Contains("},{") || jsonChunk == "]")
                {
                    await Response.Body.FlushAsync(cancellationToken);
                    chunkCount++;
                    
                    if (chunkCount % 100 == 0)
                    {
                        var elapsed = Stopwatch.GetElapsedTime(startTime);
                        _logger.LogDebug("Sent {ChunkCount} chunks in {ElapsedMs}ms", chunkCount, elapsed.TotalMilliseconds);
                    }
                }

                rowsInChunk++;
            }

            var totalElapsed = Stopwatch.GetElapsedTime(startTime);
            _logger.LogInformation("Chunked JSON streaming completed. Chunks: {ChunkCount}, Rows: {RowCount}, Time: {ElapsedMs}ms",
                chunkCount, rowsInChunk, totalElapsed.TotalMilliseconds);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Chunked JSON streaming cancelled after {ChunkCount} chunks", chunkCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during chunked JSON streaming");
            await Response.WriteAsync($",{{\"__stream_error\":\"{ex.Message}\"}}]", cancellationToken);
        }
    }

    /// <summary>
    /// Generates and streams a synthetic test dataset for performance testing.
    /// Creates configurable test data with various field types including optional binary blobs.
    /// </summary>
    /// <param name="rowCount">Number of rows to generate (default: 10000)</param>
    /// <param name="fieldCount">Number of fields per row (default: 20)</param>
    /// <param name="includeBlobs">Whether to include binary blob fields for testing large payloads</param>
    /// <param name="cancellationToken">Token for cancelling the generation</param>
    /// <returns>Success result when generation completes or is cancelled</returns>
    [HttpGet("test-large-dataset")]
    public async Task<IActionResult> TestLargeDataset(
        [FromQuery] int rowCount = 10000,
        [FromQuery] int fieldCount = 20,
        [FromQuery] bool includeBlobs = false,
        CancellationToken cancellationToken = default)
    {
        Response.ContentType = "application/x-ndjson";
        
        _logger.LogInformation("Generating test dataset: {RowCount} rows, {FieldCount} fields, Blobs: {IncludeBlobs}", 
            rowCount, fieldCount, includeBlobs);

        try
        {
            var random = new Random();
            
            for (int i = 0; i < rowCount; i++)
            {
                var testRow = new Dictionary<string, object?>();
                
                for (int j = 0; j < fieldCount; j++)
                {
                    var fieldName = $"field_{j}";
                    
                    testRow[fieldName] = j switch
                    {
                        0 => i, // ID field
                        1 => $"Test string value for row {i} field {j}",
                        2 => random.NextDouble() * 10000,
                        3 => DateTime.UtcNow.AddSeconds(random.Next(-86400, 86400)),
                        4 => random.Next(0, 2) == 1,
                        5 when includeBlobs => GenerateTestBlob(random.Next(100, 1000)),
                        6 when includeBlobs => GenerateLargeString(random.Next(1000, 10000)),
                        _ => $"Value_{i}_{j}_{random.Next(1000, 9999)}"
                    };
                }

                // Serialize to NDJSON
                var jsonLine = System.Text.Json.JsonSerializer.Serialize(testRow) + "\n";
                await Response.WriteAsync(jsonLine, cancellationToken);
                
                if (i % 1000 == 0)
                {
                    await Response.Body.FlushAsync(cancellationToken);
                    _logger.LogDebug("Generated {RowCount} test rows", i);
                }
            }

            _logger.LogInformation("Test dataset generation completed: {RowCount} rows", rowCount);
            return Ok();
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Test dataset generation cancelled");
            return Ok();
        }
    }

    /// <summary>
    /// Generates a byte array filled with random data for blob testing.
    /// Used to simulate large binary fields in test datasets.
    /// </summary>
    /// <param name="size">Size of the blob in bytes</param>
    /// <returns>Byte array filled with random data</returns>
    private byte[] GenerateTestBlob(int size)
    {
        var blob = new byte[size];
        new Random().NextBytes(blob);
        return blob;
    }

    /// <summary>
    /// Generates a string of specified length filled with random alphanumeric characters.
    /// Used to simulate large text fields in test datasets.
    /// </summary>
    /// <param name="length">Length of the string to generate</param>
    /// <returns>Random string of the specified length</returns>
    private string GenerateLargeString(int length)
    {
        var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ";
        var random = new Random();
        return new string(Enumerable.Repeat(chars, length)
            .Select(s => s[random.Next(s.Length)]).ToArray());
    }
}