using Microsoft.AspNetCore.Mvc;
using ReportBuilder.Request;
using ReportBuilder.Response;
using ReportBuilder.Service;
using System.Runtime.CompilerServices;

namespace ReportBuilder.Controllers;

/// <summary>
/// Controller for executing SQL JOIN operations on uploaded CSV files.
/// Provides endpoints for joining datasets with memory usage monitoring and stream-based result delivery.
/// </summary>
[ApiController]
[Route("query")]
public class JoinQueryController : ControllerBase
{
    private readonly IJoinQueryService _joinQueryService;
    private readonly ILogger<JoinQueryController> _logger;

    /// <summary>
    /// Initializes a new instance of the JoinQueryController.
    /// </summary>
    /// <param name="joinQueryService">Service for executing JOIN operations on CSV data</param>
    /// <param name="logger">Logger for recording JOIN operation performance and events</param>
    public JoinQueryController(IJoinQueryService joinQueryService, ILogger<JoinQueryController> logger)
    {
        _joinQueryService = joinQueryService;
        _logger = logger;
    }

    /// <summary>
    /// Executes a JOIN operation on two uploaded CSV files and streams the results.
    /// Monitors memory usage and provides Databricks recommendations for large datasets.
    /// </summary>
    /// <param name="request">JOIN request containing two CSV files and join configuration</param>
    /// <returns>Streamed JSON results with metadata about the join operation</returns>
    [HttpPost("join")]
    [RequestSizeLimit(1_000_000_000)] // 1GB total request size limit
    public async Task<IActionResult> ExecuteJoin([FromForm] JoinQueryRequest request)
    {
        var cancellationToken = HttpContext.RequestAborted;
        
        if (!ModelState.IsValid)
        {
            return BadRequest(ModelState);
        }

        try
        {
            // Validate files
            var fileValidation = ValidateFiles(request);
            if (!fileValidation.isValid)
            {
                return BadRequest(new JoinQueryErrorResponse
                {
                    ErrorMessage = fileValidation.errorMessage,
                    MemoryUsage = GC.GetTotalMemory(false),
                    LeftFileSize = request.LeftFile?.Length ?? 0,
                    RightFileSize = request.RightFile?.Length ?? 0
                });
            }

            // Validate request parameters
            var requestValidation = ValidateRequest(request);
            if (!requestValidation.isValid)
            {
                return BadRequest(new JoinQueryErrorResponse
                {
                    ErrorMessage = requestValidation.errorMessage,
                    MemoryUsage = GC.GetTotalMemory(false),
                    LeftFileSize = request.LeftFile.Length,
                    RightFileSize = request.RightFile.Length
                });
            }

            _logger.LogInformation("Processing JOIN query for files: {LeftFile} ({LeftSizeKB} KB) and {RightFile} ({RightSizeKB} KB)", 
                request.LeftFile.FileName, request.LeftFile.Length / 1024,
                request.RightFile.FileName, request.RightFile.Length / 1024);

            var result = await _joinQueryService.ExecuteJoinQueryAsync(request, cancellationToken);
            
            if (!result.Success)
            {
                var statusCode = result.DatabricksSuggestion?.ShouldUseDatabricks == true ? 413 : 500;
                
                return StatusCode(statusCode, new JoinQueryErrorResponse
                {
                    ErrorMessage = result.ErrorMessage ?? "JOIN query execution failed",
                    MemoryUsage = result.FinalMemoryUsage,
                    LeftFileSize = result.LeftFileSize,
                    RightFileSize = result.RightFileSize,
                    DatabricksSuggestion = result.DatabricksSuggestion
                });
            }

            // Stream the JOIN results
            Response.ContentType = "application/json";
            await Response.WriteAsync("{\"success\":true,\"joinResults\":[", cancellationToken);
            
            var isFirst = true;
            var rowCount = 0;
            var lastMemoryCheck = GC.GetTotalMemory(false);
            
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
                
                // Flush and check memory every 50 rows
                if (rowCount % 50 == 0)
                {
                    await Response.Body.FlushAsync(cancellationToken);
                    
                    var currentMemory = GC.GetTotalMemory(false);
                    if (rowCount % 500 == 0)
                    {
                        _logger.LogDebug("Streamed {RowCount} JOIN rows, Memory: {MemoryMB} MB", 
                            rowCount, currentMemory / (1024 * 1024));
                    }
                }
            }
            
            var finalMemoryUsage = GC.GetTotalMemory(false);
            var metadata = new
            {
                totalRows = rowCount,
                joinType = request.JoinType.ToString(),
                columns = result.Columns,
                queryExecuted = result.QueryExecuted,
                memoryStats = new
                {
                    initialMemoryMB = result.InitialMemoryUsage / (1024 * 1024),
                    finalMemoryMB = result.FinalMemoryUsage / (1024 * 1024),
                    maxMemoryMB = result.MaxMemoryUsage / (1024 * 1024),
                    peakMemoryMB = result.PeakMemoryUsage / (1024 * 1024)
                },
                fileStats = new
                {
                    leftFileSizeKB = result.LeftFileSize / 1024,
                    rightFileSizeKB = result.RightFileSize / 1024,
                    totalFileSizeKB = (result.LeftFileSize + result.RightFileSize) / 1024
                },
                databricksSuggestion = result.DatabricksSuggestion
            };
            
            await Response.WriteAsync($"],\"metadata\":{System.Text.Json.JsonSerializer.Serialize(metadata)}}}", cancellationToken);
            
            _logger.LogInformation("JOIN query completed successfully. Rows: {RowCount}, Peak Memory: {PeakMemoryMB} MB", 
                rowCount, result.PeakMemoryUsage / (1024 * 1024));
            
            return new EmptyResult();
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("JOIN query was cancelled by client");
            return StatusCode(499, new JoinQueryErrorResponse
            {
                ErrorMessage = "Request was cancelled",
                MemoryUsage = GC.GetTotalMemory(false),
                LeftFileSize = request.LeftFile?.Length ?? 0,
                RightFileSize = request.RightFile?.Length ?? 0
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing JOIN query request");
            return StatusCode(500, new JoinQueryErrorResponse
            {
                ErrorMessage = "Internal server error occurred while processing the JOIN query",
                MemoryUsage = GC.GetTotalMemory(false),
                LeftFileSize = request.LeftFile?.Length ?? 0,
                RightFileSize = request.RightFile?.Length ?? 0
            });
        }
    }

    /// <summary>
    /// Previews a JOIN operation by analyzing CSV file columns and providing join suggestions.
    /// Returns column information and memory usage estimates without executing the full join.
    /// </summary>
    /// <param name="request">Preview request containing two CSV files to analyze</param>
    /// <returns>Column information, common columns, and join recommendations</returns>
    [HttpPost("join-preview")]
    [RequestSizeLimit(50_000_000)] // 50MB limit for preview
    public async Task<IActionResult> PreviewJoin([FromForm] JoinPreviewRequest request)
    {
        var cancellationToken = HttpContext.RequestAborted;
        
        if (request.LeftFile == null || request.RightFile == null)
        {
            return BadRequest(new { error = "Both CSV files are required" });
        }

        try
        {
            _logger.LogInformation("Previewing JOIN for files: {LeftFile} and {RightFile}", 
                request.LeftFile.FileName, request.RightFile.FileName);

            var leftColumns = await GetCsvColumnsPreview(request.LeftFile);
            var rightColumns = await GetCsvColumnsPreview(request.RightFile);

            var commonColumns = leftColumns.Intersect(rightColumns, StringComparer.OrdinalIgnoreCase).ToList();
            
            var estimatedMemoryMB = (request.LeftFile.Length + request.RightFile.Length) * 3 / (1024 * 1024);
            var recommendDatabricks = estimatedMemoryMB > 300;

            return Ok(new
            {
                success = true,
                leftFile = new
                {
                    name = request.LeftFile.FileName,
                    sizeKB = request.LeftFile.Length / 1024,
                    columns = leftColumns
                },
                rightFile = new
                {
                    name = request.RightFile.FileName,
                    sizeKB = request.RightFile.Length / 1024,
                    columns = rightColumns
                },
                joinSuggestions = new
                {
                    commonColumns = commonColumns,
                    estimatedMemoryMB = estimatedMemoryMB,
                    recommendDatabricks = recommendDatabricks,
                    reason = recommendDatabricks ? "Large files may exceed memory limits" : "Files are suitable for in-memory processing"
                }
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error previewing JOIN");
            return StatusCode(500, new { error = "Failed to preview JOIN operation" });
        }
    }

    /// <summary>
    /// Validates that the uploaded files meet the requirements for JOIN operations.
    /// Checks file existence, size, and format (CSV).
    /// </summary>
    /// <param name="request">JOIN request containing files to validate</param>
    /// <returns>Validation result with success status and error message if validation fails</returns>
    private (bool isValid, string errorMessage) ValidateFiles(JoinQueryRequest request)
    {
        if (request.LeftFile == null || request.LeftFile.Length == 0)
        {
            return (false, "Left file is required and cannot be empty");
        }

        if (request.RightFile == null || request.RightFile.Length == 0)
        {
            return (false, "Right file is required and cannot be empty");
        }

        if (!request.LeftFile.FileName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
        {
            return (false, "Left file must be a CSV file");
        }

        if (!request.RightFile.FileName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
        {
            return (false, "Right file must be a CSV file");
        }

        return (true, string.Empty);
    }

    /// <summary>
    /// Validates the JOIN request parameters for completeness and correctness.
    /// Checks join column, select columns, aliases, and page size constraints.
    /// </summary>
    /// <param name="request">JOIN request to validate</param>
    /// <returns>Validation result with success status and error message if validation fails</returns>
    private (bool isValid, string errorMessage) ValidateRequest(JoinQueryRequest request)
    {
        if (string.IsNullOrEmpty(request.JoinColumn))
        {
            return (false, "Join column is required");
        }

        if (!request.SelectColumns.Any())
        {
            return (false, "At least one select column must be specified");
        }

        if (request.PageSize <= 0 || request.PageSize > 10000)
        {
            request.PageSize = 1000; // Set default
        }

        if (string.IsNullOrEmpty(request.LeftAlias) || string.IsNullOrEmpty(request.RightAlias))
        {
            return (false, "Both left and right aliases are required");
        }

        if (request.LeftAlias == request.RightAlias)
        {
            return (false, "Left and right aliases must be different");
        }

        return (true, string.Empty);
    }

    /// <summary>
    /// Extracts column names from the first line of a CSV file for preview purposes.
    /// Parses CSV header row and returns cleaned column names.
    /// </summary>
    /// <param name="file">CSV file to analyze</param>
    /// <returns>List of column names found in the CSV header</returns>
    private async Task<List<string>> GetCsvColumnsPreview(IFormFile file)
    {
        using var reader = new StreamReader(file.OpenReadStream());
        var firstLine = await reader.ReadLineAsync();
        
        if (string.IsNullOrEmpty(firstLine))
        {
            return new List<string>();
        }

        return firstLine.Split(',')
            .Select(col => col.Trim().Trim('"'))
            .Where(col => !string.IsNullOrEmpty(col))
            .ToList();
    }
}

/// <summary>
/// Request model for previewing JOIN operations on two CSV files.
/// Contains the uploaded files to analyze for join compatibility.
/// </summary>
public class JoinPreviewRequest
{
    /// <summary>
    /// Gets or sets the left CSV file for the join preview.
    /// </summary>
    public IFormFile? LeftFile { get; set; }
    
    /// <summary>
    /// Gets or sets the right CSV file for the join preview.
    /// </summary>
    public IFormFile? RightFile { get; set; }
}