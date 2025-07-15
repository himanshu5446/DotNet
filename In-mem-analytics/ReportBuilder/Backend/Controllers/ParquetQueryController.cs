using Microsoft.AspNetCore.Mvc;
using ReportBuilder.Request;
using ReportBuilder.Response;
using ReportBuilder.Service;
using System.Runtime.CompilerServices;

namespace ReportBuilder.Controllers;

/// <summary>
/// Controller for querying Apache Parquet files with advanced filtering and schema inspection.
/// Provides endpoints for efficient columnar data processing with memory optimization.
/// </summary>
[ApiController]
[Route("query")]
public class ParquetQueryController : ControllerBase
{
    private readonly IParquetQueryService _parquetQueryService;
    private readonly ILogger<ParquetQueryController> _logger;

    /// <summary>
    /// Initializes a new instance of the ParquetQueryController.
    /// </summary>
    /// <param name="parquetQueryService">Service for handling Parquet file operations and queries</param>
    /// <param name="logger">Logger for recording Parquet query performance and events</param>
    public ParquetQueryController(IParquetQueryService parquetQueryService, ILogger<ParquetQueryController> logger)
    {
        _parquetQueryService = parquetQueryService;
        _logger = logger;
    }

    /// <summary>
    /// Executes a query on a Parquet file with column projection and filtering capabilities.
    /// Supports both uploaded files and server-side file paths with streaming results.
    /// </summary>
    /// <param name="request">Parquet query request containing file, columns, and filter criteria</param>
    /// <returns>Streamed query results with metadata and performance statistics</returns>
    [HttpPost("parquet-scan")]
    [RequestSizeLimit(1_000_000_000)] // 1GB file size limit
    public async Task<IActionResult> ParquetScan([FromForm] ParquetQueryRequest request)
    {
        var cancellationToken = HttpContext.RequestAborted;
        
        if (!ModelState.IsValid)
        {
            return BadRequest(ModelState);
        }

        try
        {
            // Validate input
            var validation = ValidateRequest(request);
            if (!validation.isValid)
            {
                return BadRequest(new ParquetQueryErrorResponse
                {
                    ErrorMessage = validation.errorMessage,
                    MemoryUsage = GC.GetTotalMemory(false),
                    FileSize = request.ParquetFile?.Length ?? 0
                });
            }

            // Log request details
            var fileInfo = request.ParquetFile != null 
                ? $"uploaded file: {request.ParquetFile.FileName} ({request.ParquetFile.Length / 1024} KB)"
                : $"file path: {request.ParquetFilePath}";
            
            _logger.LogInformation("Processing Parquet scan for {FileInfo}, Columns: {Columns}, Filter: {Filter}", 
                fileInfo, 
                request.SelectColumns.Any() ? string.Join(", ", request.SelectColumns) : "ALL",
                request.FilterExpression ?? "NONE");

            var result = await _parquetQueryService.ExecuteParquetQueryAsync(request, cancellationToken);
            
            if (!result.Success)
            {
                var errorResponse = new ParquetQueryErrorResponse
                {
                    ErrorMessage = result.ErrorMessage ?? "Parquet query execution failed",
                    MemoryUsage = result.FinalMemoryUsage,
                    FileSize = result.FileSize
                };

                // Add suggestions for common errors
                if (result.ErrorMessage?.Contains("Invalid columns") == true)
                {
                    errorResponse.Suggestions.Add("Use POST /query/parquet-schema to see available columns");
                }
                
                if (result.ErrorMessage?.Contains("Filter") == true)
                {
                    errorResponse.Suggestions.Add("Check filter syntax - use column names, operators (>, <, =, AND, OR), and quoted string values");
                }

                return BadRequest(errorResponse);
            }

            // Handle warnings
            if (result.Warnings.Any())
            {
                foreach (var warning in result.Warnings)
                {
                    _logger.LogWarning("Parquet query warning: {Warning}", warning);
                }
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
                
                // Flush every 100 rows for better streaming
                if (rowCount % 100 == 0)
                {
                    await Response.Body.FlushAsync(cancellationToken);
                }
            }
            
            // Write metadata
            var metadata = new
            {
                totalRows = rowCount,
                columns = result.Columns,
                queryExecuted = result.QueryExecuted,
                optimizations = result.Optimizations,
                memoryStats = new
                {
                    initialMemoryMB = result.InitialMemoryUsage / (1024 * 1024),
                    finalMemoryMB = result.FinalMemoryUsage / (1024 * 1024),
                    maxMemoryMB = result.MaxMemoryUsage / (1024 * 1024),
                    peakMemoryMB = result.PeakMemoryUsage / (1024 * 1024)
                },
                fileStats = new
                {
                    filePath = result.FilePath,
                    fileSizeKB = result.FileSize / 1024,
                    fileSizeMB = result.FileSize / (1024 * 1024)
                },
                warnings = result.Warnings
            };
            
            await Response.WriteAsync($"],\"metadata\":{System.Text.Json.JsonSerializer.Serialize(metadata)}}}", cancellationToken);
            
            _logger.LogInformation("Parquet scan completed successfully. Rows: {RowCount}, Peak Memory: {PeakMemoryMB} MB", 
                rowCount, result.PeakMemoryUsage / (1024 * 1024));
            
            return new EmptyResult();
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Parquet scan was cancelled by client");
            return StatusCode(499, new ParquetQueryErrorResponse
            {
                ErrorMessage = "Request was cancelled",
                MemoryUsage = GC.GetTotalMemory(false),
                FileSize = request.ParquetFile?.Length ?? 0
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing Parquet scan request");
            return StatusCode(500, new ParquetQueryErrorResponse
            {
                ErrorMessage = "Internal server error occurred while processing the Parquet query",
                MemoryUsage = GC.GetTotalMemory(false),
                FileSize = request.ParquetFile?.Length ?? 0,
                Suggestions = new List<string> 
                { 
                    "Check if the file is a valid Parquet format",
                    "Verify the file path is accessible",
                    "Try with a smaller file or add filters to reduce memory usage"
                }
            });
        }
    }

    /// <summary>
    /// Retrieves schema information from a Parquet file including column names, types, and sample data.
    /// Provides metadata needed for building efficient queries without processing the entire file.
    /// </summary>
    /// <param name="request">Schema request containing Parquet file or file path</param>
    /// <returns>Parquet schema information with column details and optional sample rows</returns>
    [HttpPost("parquet-schema")]
    [RequestSizeLimit(500_000_000)] // 500MB limit for schema inspection
    public async Task<IActionResult> GetParquetSchema([FromForm] ParquetSchemaRequest request)
    {
        var cancellationToken = HttpContext.RequestAborted;
        
        try
        {
            // Validate input
            if (request.ParquetFile == null && string.IsNullOrEmpty(request.ParquetFilePath))
            {
                return BadRequest(new { error = "Either ParquetFile or ParquetFilePath must be provided" });
            }

            if (request.ParquetFile != null && !request.ParquetFile.FileName.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
            {
                return BadRequest(new { error = "File must be a Parquet file (.parquet extension)" });
            }

            _logger.LogInformation("Getting Parquet schema for {FileInfo}", 
                request.ParquetFile != null ? $"uploaded file: {request.ParquetFile.FileName}" : $"file path: {request.ParquetFilePath}");

            var result = await _parquetQueryService.GetParquetSchemaAsync(request, cancellationToken);
            
            if (!result.Success)
            {
                return BadRequest(new { error = result.ErrorMessage });
            }

            return Ok(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting Parquet schema");
            return StatusCode(500, new { error = "Failed to get Parquet schema" });
        }
    }

    /// <summary>
    /// Generates query suggestions and optimization tips based on Parquet file schema.
    /// Analyzes file structure and provides sample queries, filter examples, and performance recommendations.
    /// </summary>
    /// <param name="request">Query builder request containing Parquet file to analyze</param>
    /// <returns>Schema information with generated query suggestions and optimization tips</returns>
    [HttpPost("parquet-query-builder")]
    public async Task<IActionResult> BuildParquetQuery([FromForm] ParquetQueryBuilderRequest request)
    {
        var cancellationToken = HttpContext.RequestAborted;
        
        try
        {
            if (request.ParquetFile == null && string.IsNullOrEmpty(request.ParquetFilePath))
            {
                return BadRequest(new { error = "Either ParquetFile or ParquetFilePath must be provided" });
            }

            // Get schema first
            var schemaRequest = new ParquetSchemaRequest
            {
                ParquetFile = request.ParquetFile,
                ParquetFilePath = request.ParquetFilePath,
                IncludeDataTypes = true,
                SampleRows = 3
            };

            var schema = await _parquetQueryService.GetParquetSchemaAsync(schemaRequest, cancellationToken);
            
            if (!schema.Success)
            {
                return BadRequest(new { error = schema.ErrorMessage });
            }

            // Build query suggestions
            var suggestions = new
            {
                availableColumns = schema.Columns.Select(c => new { name = c.Name, type = c.DataType }).ToList(),
                sampleQueries = new[]
                {
                    new { 
                        description = "Select all columns", 
                        selectColumns = new string[0],
                        filterExpression = (string?)null
                    },
                    new { 
                        description = "Select first 3 columns", 
                        selectColumns = schema.Columns.Take(3).Select(c => c.Name).ToArray(),
                        filterExpression = (string?)null
                    },
                    new { 
                        description = "Select with basic filter", 
                        selectColumns = schema.Columns.Take(2).Select(c => c.Name).ToArray(),
                        filterExpression = schema.Columns.Any(c => c.DataType.Contains("INT")) 
                            ? $"{schema.Columns.First(c => c.DataType.Contains("INT")).Name} > 0"
                            : null
                    }
                },
                optimizationTips = new[]
                {
                    "Use column projection (selectColumns) to reduce memory usage",
                    "Add filters to reduce the number of rows processed",
                    "Consider using LIMIT for large datasets",
                    "Use ORDER BY only when necessary as it requires full scan"
                },
                filterExamples = new[]
                {
                    "column_name = 'value'",
                    "numeric_column > 100",
                    "date_column >= '2023-01-01'",
                    "column1 = 'value' AND column2 > 50",
                    "column IN ('value1', 'value2', 'value3')"
                }
            };

            return Ok(new
            {
                success = true,
                schema = schema,
                queryBuilder = suggestions
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error building Parquet query");
            return StatusCode(500, new { error = "Failed to build Parquet query" });
        }
    }

    /// <summary>
    /// Validates Parquet query request parameters for file format, size limits, and query constraints.
    /// Ensures file has .parquet extension and validates pagination and sampling parameters.
    /// </summary>
    /// <param name="request">Parquet query request to validate</param>
    /// <returns>Validation result with success status and error message if validation fails</returns>
    private (bool isValid, string errorMessage) ValidateRequest(ParquetQueryRequest request)
    {
        // Validate file input
        if (request.ParquetFile == null && string.IsNullOrEmpty(request.ParquetFilePath))
        {
            return (false, "Either ParquetFile or ParquetFilePath must be provided");
        }

        // Validate file extension
        if (request.ParquetFile != null)
        {
            if (request.ParquetFile.Length == 0)
            {
                return (false, "Parquet file cannot be empty");
            }

            if (!request.ParquetFile.FileName.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
            {
                return (false, "File must be a Parquet file (.parquet extension)");
            }
        }

        // Validate file path
        if (!string.IsNullOrEmpty(request.ParquetFilePath))
        {
            if (!request.ParquetFilePath.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
            {
                return (false, "File path must point to a Parquet file (.parquet extension)");
            }
        }

        // Validate page size
        if (request.PageSize <= 0 || request.PageSize > 10000)
        {
            request.PageSize = 1000; // Set default
        }

        // Validate limit
        if (request.Limit.HasValue && request.Limit.Value <= 0)
        {
            return (false, "Limit must be greater than 0");
        }

        // Validate sample size
        if (request.SampleSize.HasValue && request.SampleSize.Value <= 0)
        {
            return (false, "Sample size must be greater than 0");
        }

        return (true, string.Empty);
    }
}

/// <summary>
/// Request model for building optimized Parquet queries based on file schema analysis.
/// Contains the Parquet file to analyze for generating query suggestions.
/// </summary>
public class ParquetQueryBuilderRequest
{
    /// <summary>
    /// Gets or sets the server-side path to the Parquet file to analyze.
    /// </summary>
    public string? ParquetFilePath { get; set; }
    
    /// <summary>
    /// Gets or sets the uploaded Parquet file to analyze.
    /// </summary>
    public IFormFile? ParquetFile { get; set; }
}