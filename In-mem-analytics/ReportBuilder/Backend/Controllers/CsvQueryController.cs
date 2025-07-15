using Microsoft.AspNetCore.Mvc;
using ReportBuilder.Request;
using ReportBuilder.Response;
using ReportBuilder.Service;
using System.Runtime.CompilerServices;

namespace ReportBuilder.Controllers;

[ApiController]
[Route("query")]
public class CsvQueryController : ControllerBase
{
    private readonly ICsvDuckDbService _csvQueryService;
    private readonly ILogger<CsvQueryController> _logger;

    /// <summary>
    /// Initializes a new instance of the CsvQueryController with required dependencies
    /// </summary>
    /// <param name="csvQueryService">Service for executing CSV queries via DuckDB</param>
    /// <param name="logger">Logger for recording controller activities</param>
    public CsvQueryController(ICsvDuckDbService csvQueryService, ILogger<CsvQueryController> logger)
    {
        _csvQueryService = csvQueryService;
        _logger = logger;
    }

    /// <summary>
    /// Executes a streaming query against an uploaded CSV file with column projection and filtering
    /// </summary>
    /// <param name="request">CSV query request containing file, columns, and optional filters</param>
    /// <returns>Streamed JSON response with query results and metadata</returns>
    [HttpPost("csv-stream")]
    [RequestSizeLimit(500_000_000)] // 500MB file size limit
    public async Task<IActionResult> CsvStream([FromForm] CsvQueryRequest request)
    {
        var cancellationToken = HttpContext.RequestAborted;
        
        if (!ModelState.IsValid)
        {
            return BadRequest(ModelState);
        }

        try
        {
            // Validate CSV file
            if (request.CsvFile == null || request.CsvFile.Length == 0)
            {
                return BadRequest(new CsvQueryErrorResponse
                {
                    ErrorMessage = "CSV file is required and cannot be empty",
                    MemoryUsage = GC.GetTotalMemory(false)
                });
            }

            // Validate file extension
            var fileName = request.CsvFile.FileName;
            if (!fileName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
            {
                return BadRequest(new CsvQueryErrorResponse
                {
                    ErrorMessage = "Only CSV files are supported",
                    MemoryUsage = GC.GetTotalMemory(false)
                });
            }

            // Validate columns list
            if (!request.Columns.Any())
            {
                return BadRequest(new CsvQueryErrorResponse
                {
                    ErrorMessage = "At least one column must be specified for projection",
                    MemoryUsage = GC.GetTotalMemory(false)
                });
            }

            // Validate page size
            if (request.PageSize <= 0 || request.PageSize > 10000)
            {
                request.PageSize = 1000; // Default to 1000 if invalid
            }

            _logger.LogInformation("Processing CSV query for file: {FileName}, Size: {SizeKB} KB, Columns: {Columns}", 
                fileName, request.CsvFile.Length / 1024, string.Join(", ", request.Columns));

            var result = await _csvQueryService.ExecuteCsvQueryAsync(request, cancellationToken);
            
            if (!result.Success)
            {
                // Check if it's a column validation error
                if (result.ErrorMessage?.Contains("Column(s) not found") == true)
                {
                    return BadRequest(new CsvQueryErrorResponse
                    {
                        ErrorMessage = result.ErrorMessage,
                        MemoryUsage = result.FinalMemoryUsage
                    });
                }
                
                return StatusCode(500, new CsvQueryErrorResponse
                {
                    ErrorMessage = result.ErrorMessage ?? "Query execution failed",
                    MemoryUsage = result.FinalMemoryUsage
                });
            }

            // Stream the results
            Response.ContentType = "application/json";
            await Response.WriteAsync("{\"success\":true,\"data\":[", cancellationToken);
            
            var isFirst = true;
            var rowCount = 0;
            
            await foreach (var row in result.Data!.WithCancellation(cancellationToken))
            {
                if (!isFirst)
                {
                    await Response.WriteAsync(",", cancellationToken);
                }
                
                var json = System.Text.Json.JsonSerializer.Serialize(row);
                await Response.WriteAsync(json, cancellationToken);
                
                isFirst = false;
                rowCount++;
                
                // Flush every 100 rows to improve streaming performance
                if (rowCount % 100 == 0)
                {
                    await Response.Body.FlushAsync(cancellationToken);
                }
            }
            
            await Response.WriteAsync($"],\"metadata\":{{\"totalRows\":{rowCount},\"columns\":{System.Text.Json.JsonSerializer.Serialize(result.Columns)},\"initialMemoryMB\":{result.InitialMemoryUsage / (1024 * 1024)},\"finalMemoryMB\":{result.FinalMemoryUsage / (1024 * 1024)},\"maxMemoryMB\":{result.MaxMemoryUsage / (1024 * 1024)}}}}}", cancellationToken);
            
            _logger.LogInformation("CSV query completed successfully. Rows: {RowCount}, Memory: {MemoryMB} MB", 
                rowCount, result.FinalMemoryUsage / (1024 * 1024));
            
            return new EmptyResult();
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("CSV query was cancelled by client");
            return StatusCode(499, new CsvQueryErrorResponse
            {
                ErrorMessage = "Request was cancelled",
                MemoryUsage = GC.GetTotalMemory(false)
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing CSV query request");
            return StatusCode(500, new CsvQueryErrorResponse
            {
                ErrorMessage = "Internal server error occurred while processing the CSV query",
                MemoryUsage = GC.GetTotalMemory(false)
            });
        }
    }

    /// <summary>
    /// Inspects a CSV file to extract column names for query building
    /// </summary>
    /// <param name="csvFile">CSV file to inspect</param>
    /// <returns>List of column names available in the CSV file</returns>
    [HttpPost("csv-columns")]
    [RequestSizeLimit(100_000_000)] // 100MB file size limit for column inspection
    public async Task<IActionResult> GetCsvColumns([FromForm] IFormFile csvFile)
    {
        var cancellationToken = HttpContext.RequestAborted;
        
        if (csvFile == null || csvFile.Length == 0)
        {
            return BadRequest(new { error = "CSV file is required" });
        }

        if (!csvFile.FileName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
        {
            return BadRequest(new { error = "Only CSV files are supported" });
        }

        try
        {
            _logger.LogInformation("Inspecting CSV columns for file: {FileName}", csvFile.FileName);
            
            // Read first few lines to determine columns
            using var reader = new StreamReader(csvFile.OpenReadStream());
            var firstLine = await reader.ReadLineAsync();
            
            if (string.IsNullOrEmpty(firstLine))
            {
                return BadRequest(new { error = "CSV file appears to be empty" });
            }

            // Simple CSV parsing - assumes first line is headers
            var columns = firstLine.Split(',')
                .Select(col => col.Trim().Trim('"'))
                .Where(col => !string.IsNullOrEmpty(col))
                .ToList();

            return Ok(new
            {
                success = true,
                columns = columns,
                fileName = csvFile.FileName,
                fileSizeKB = csvFile.Length / 1024
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error inspecting CSV columns");
            return StatusCode(500, new { error = "Failed to inspect CSV columns" });
        }
    }
}