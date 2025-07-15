using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using ReportBuilder.Configuration;
using ReportBuilder.Request;
using ReportBuilder.Service;
using System.Text.Json;

namespace ReportBuilder.Controllers;

/// <summary>
/// Controller for demonstrating backpressure handling in streaming scenarios.
/// Provides endpoints to test streaming behavior with different throttling and backpressure configurations.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public class BackpressureStreamingController : ControllerBase
{
    private readonly IBackpressureStreamingService _streamingService;
    private readonly ILogger<BackpressureStreamingController> _logger;
    private readonly BackpressureSettings _settings;

    /// <summary>
    /// Initializes a new instance of the BackpressureStreamingController.
    /// </summary>
    /// <param name="streamingService">Service for handling backpressure-aware streaming operations</param>
    /// <param name="logger">Logger for recording streaming performance and events</param>
    /// <param name="settings">Configuration settings for backpressure and throttling behavior</param>
    public BackpressureStreamingController(
        IBackpressureStreamingService streamingService,
        ILogger<BackpressureStreamingController> logger,
        IOptions<BackpressureSettings> settings)
    {
        _streamingService = streamingService;
        _logger = logger;
        _settings = settings.Value;
    }

    /// <summary>
    /// Streams query results as JSON with configurable backpressure handling.
    /// Demonstrates how to handle streaming with potential slow consumers.
    /// </summary>
    /// <param name="request">Query request containing SQL and dataset information</param>
    /// <param name="cancellationToken">Token for cancelling the streaming operation</param>
    [HttpPost("stream")]
    public async Task StreamQueryResults([FromBody] QueryRequest request, CancellationToken cancellationToken)
    {
        Response.ContentType = "application/json";
        Response.Headers.Add("Cache-Control", "no-cache");
        Response.Headers.Add("Connection", "keep-alive");

        _logger.LogInformation("Starting streaming query for dataset: {Dataset}", request.DatasetPath);
        
        var rowCount = 0;
        var startTime = DateTime.UtcNow;

        try
        {
            await Response.WriteAsync("[", cancellationToken);
            
            var isFirstRow = true;
            await foreach (var row in _streamingService.StreamQueryResultsAsync(request, cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (!isFirstRow)
                {
                    await Response.WriteAsync(",", cancellationToken);
                }
                
                var json = JsonSerializer.Serialize(row);
                await Response.WriteAsync(json, cancellationToken);
                
                // Manual flush after each row if configured
                if (_settings.FlushAfterEachRow)
                {
                    await Response.Body.FlushAsync(cancellationToken);
                }

                isFirstRow = false;
                rowCount++;

                // Log progress periodically
                if (rowCount % 1000 == 0)
                {
                    var elapsed = DateTime.UtcNow - startTime;
                    _logger.LogInformation("Streamed {RowCount} rows in {ElapsedMs}ms", rowCount, elapsed.TotalMilliseconds);
                }
            }

            await Response.WriteAsync("]", cancellationToken);
            await Response.Body.FlushAsync(cancellationToken);

            var totalElapsed = DateTime.UtcNow - startTime;
            _logger.LogInformation("Streaming completed. Total rows: {RowCount}, Total time: {ElapsedMs}ms", 
                rowCount, totalElapsed.TotalMilliseconds);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Streaming cancelled by client or timeout after {RowCount} rows", rowCount);
            await Response.WriteAsync("]", cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during streaming after {RowCount} rows", rowCount);
            await Response.WriteAsync($",{{\"error\":\"{ex.Message}\"}}]", cancellationToken);
        }
    }

    /// <summary>
    /// Streams query results with additional throttling to simulate slow consumer scenarios.
    /// Always flushes after each row and applies configurable delays for testing backpressure.
    /// </summary>
    /// <param name="request">Query request containing SQL and dataset information</param>
    /// <param name="cancellationToken">Token for cancelling the streaming operation</param>
    [HttpPost("stream-with-throttle")]
    public async Task StreamQueryResultsWithThrottle([FromBody] QueryRequest request, CancellationToken cancellationToken)
    {
        Response.ContentType = "application/json";
        Response.Headers.Add("Cache-Control", "no-cache");
        Response.Headers.Add("Connection", "keep-alive");

        _logger.LogInformation("Starting throttled streaming query for dataset: {Dataset}", request.DatasetPath);
        
        var rowCount = 0;
        var startTime = DateTime.UtcNow;

        try
        {
            await Response.WriteAsync("[", cancellationToken);
            
            var isFirstRow = true;
            await foreach (var row in _streamingService.StreamQueryResultsAsync(request, cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (!isFirstRow)
                {
                    await Response.WriteAsync(",", cancellationToken);
                }
                
                var json = JsonSerializer.Serialize(row);
                await Response.WriteAsync(json, cancellationToken);
                
                // Always flush after each row for throttled endpoint
                await Response.Body.FlushAsync(cancellationToken);

                // Add extra throttling for slow consumer simulation
                if (_settings.EnableThrottling)
                {
                    await Task.Delay(_settings.ThrottleDelayMs * 2, cancellationToken);
                    
                    if (rowCount % 100 == 0)
                    {
                        _logger.LogInformation("Throttling active - processed {RowCount} rows", rowCount);
                    }
                }

                isFirstRow = false;
                rowCount++;
            }

            await Response.WriteAsync("]", cancellationToken);
            await Response.Body.FlushAsync(cancellationToken);

            var totalElapsed = DateTime.UtcNow - startTime;
            _logger.LogInformation("Throttled streaming completed. Total rows: {RowCount}, Total time: {ElapsedMs}ms", 
                rowCount, totalElapsed.TotalMilliseconds);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Throttled streaming cancelled by client or timeout after {RowCount} rows", rowCount);
            await Response.WriteAsync("]", cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during throttled streaming after {RowCount} rows", rowCount);
            await Response.WriteAsync($",{{\"error\":\"{ex.Message}\"}}]", cancellationToken);
        }
    }

    /// <summary>
    /// Test endpoint that simulates a slow consumer by sending data with deliberate delays.
    /// Useful for testing client-side backpressure handling and streaming behavior.
    /// </summary>
    /// <param name="cancellationToken">Token for cancelling the test operation</param>
    /// <returns>Success result when test completes or is cancelled</returns>
    [HttpGet("test-slow-consumer")]
    public async Task<IActionResult> TestSlowConsumer(CancellationToken cancellationToken)
    {
        Response.ContentType = "text/plain";
        Response.Headers.Add("Cache-Control", "no-cache");

        try
        {
            _logger.LogInformation("Starting slow consumer test");

            for (int i = 0; i < 100; i++)
            {
                await Response.WriteAsync($"Row {i + 1}: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff}\n", cancellationToken);
                await Response.Body.FlushAsync(cancellationToken);
                
                // Simulate slow consumer
                await Task.Delay(100, cancellationToken);
                
                if (i % 10 == 0)
                {
                    _logger.LogInformation("Slow consumer test - sent {Count} rows", i + 1);
                }
            }

            _logger.LogInformation("Slow consumer test completed");
            return Ok();
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Slow consumer test cancelled");
            return Ok();
        }
    }
}