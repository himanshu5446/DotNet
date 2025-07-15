using Microsoft.AspNetCore.Mvc;
using ReportBuilder.Request;
using ReportBuilder.Response;
using ReportBuilder.Service;
using System.Runtime.CompilerServices;

namespace ReportBuilder.Controllers;

[ApiController]
[Route("query")]
public class QueryController : ControllerBase
{
    private readonly IDuckDbQueryService _queryService;
    private readonly ILogger<QueryController> _logger;

    /// <summary>
    /// Initializes a new instance of the QueryController with required dependencies
    /// </summary>
    /// <param name="queryService">Service for executing DuckDB queries</param>
    /// <param name="logger">Logger for recording controller activities</param>
    public QueryController(IDuckDbQueryService queryService, ILogger<QueryController> logger)
    {
        _queryService = queryService;
        _logger = logger;
    }

    /// <summary>
    /// Executes a query against a dataset (CSV or Parquet) with streaming response
    /// </summary>
    /// <param name="request">Query request containing dataset path, query, and optional filters</param>
    /// <param name="cancellationToken">Token to cancel the operation</param>
    /// <returns>Streamed query results or error response</returns>
    [HttpPost("run")]
    public async Task<IActionResult> RunQuery([FromBody] QueryRequest request, CancellationToken cancellationToken = default)
    {
        if (!ModelState.IsValid)
        {
            return BadRequest(ModelState);
        }

        try
        {
            // Validate file exists
            if (!System.IO.File.Exists(request.DatasetPath))
            {
                return BadRequest(new QueryErrorResponse
                {
                    ErrorMessage = $"Dataset file not found: {request.DatasetPath}",
                    MemoryUsage = GC.GetTotalMemory(false)
                });
            }

            // Validate file extension
            var extension = Path.GetExtension(request.DatasetPath).ToLower();
            if (extension != ".csv" && extension != ".parquet")
            {
                return BadRequest(new QueryErrorResponse
                {
                    ErrorMessage = $"Unsupported file format: {extension}. Only CSV and Parquet files are supported.",
                    MemoryUsage = GC.GetTotalMemory(false)
                });
            }

            _logger.LogInformation("Executing query for dataset: {DatasetPath}", request.DatasetPath);
            
            var result = await _queryService.ExecuteQueryAsync(request, cancellationToken);
            
            if (!result.Success)
            {
                return BadRequest(new QueryErrorResponse
                {
                    ErrorMessage = result.ErrorMessage ?? "Unknown error occurred",
                    MemoryUsage = result.FinalMemoryUsage
                });
            }

            return Ok(StreamResults(result.Data!, cancellationToken));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing query request");
            return StatusCode(500, new QueryErrorResponse
            {
                ErrorMessage = "Internal server error occurred while processing the query",
                MemoryUsage = GC.GetTotalMemory(false)
            });
        }
    }

    /// <summary>
    /// Streams query results asynchronously to minimize memory usage
    /// </summary>
    /// <param name="data">Async enumerable of query result rows</param>
    /// <param name="cancellationToken">Token to cancel the streaming operation</param>
    /// <returns>Async enumerable of result dictionaries</returns>
    private async IAsyncEnumerable<Dictionary<string, object?>> StreamResults(
        IAsyncEnumerable<Dictionary<string, object?>> data,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in data.WithCancellation(cancellationToken))
        {
            yield return item;
        }
    }
}