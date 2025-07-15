using Microsoft.AspNetCore.Mvc;
using ReportBuilder.Request;
using ReportBuilder.Response;
using ReportBuilder.Service;
using System.Text.Json;

namespace ReportBuilder.Controllers;

/// <summary>
/// Controller for executing SQL queries with User-Defined Functions (UDFs) for data processing and transformation.
/// Provides endpoints for text normalization, custom functions, and advanced data cleaning operations.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public class UdfQueryController : ControllerBase
{
    private readonly IDuckDbUdfService _udfService;
    private readonly ILogger<UdfQueryController> _logger;

    /// <summary>
    /// Initializes a new instance of the UdfQueryController.
    /// </summary>
    /// <param name="udfService">Service for executing queries with custom User-Defined Functions</param>
    /// <param name="logger">Logger for recording UDF query performance and events</param>
    public UdfQueryController(
        IDuckDbUdfService udfService,
        ILogger<UdfQueryController> logger)
    {
        _udfService = udfService;
        _logger = logger;
    }

    /// <summary>
    /// Executes a SQL query with User-Defined Functions in multiple response formats.
    /// Supports streaming, buffered, and count-only responses with comprehensive error handling.
    /// </summary>
    /// <param name="request">UDF query request containing SQL, file path, and execution options</param>
    /// <returns>Query results in the specified format (streaming, buffered, or count)</returns>
    [HttpPost("execute")]
    public async Task<IActionResult> ExecuteUdfQuery([FromBody] UdfQueryRequest request)
    {
        var cancellationToken = HttpContext.RequestAborted;
        var queryStartTime = DateTime.UtcNow;

        try
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(new UdfErrorResponse
                {
                    ErrorMessage = "Invalid request model",
                    ValidationErrors = ModelState.Values
                        .SelectMany(v => v.Errors)
                        .Select(e => e.ErrorMessage)
                        .ToList()
                });
            }

            _logger.LogInformation("Starting UDF query execution for file: {FilePath}", request.CsvFilePath);

            // Validate file exists
            if (!System.IO.File.Exists(request.CsvFilePath))
            {
                return BadRequest(new UdfErrorResponse
                {
                    ErrorMessage = $"CSV file not found: {request.CsvFilePath}",
                    ErrorType = "FileNotFound",
                    Suggestions = new List<string>
                    {
                        "Verify the file path is correct",
                        "Check file permissions",
                        "Ensure the file exists on the server"
                    }
                });
            }

            // Set up timeout
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(request.TimeoutSeconds));

            // Handle different response formats
            switch (request.ResponseFormat)
            {
                case UdfResponseFormat.Streaming:
                    return await HandleStreamingResponse(request, timeoutCts.Token);

                case UdfResponseFormat.Buffered:
                    return await HandleBufferedResponse(request, timeoutCts.Token);

                case UdfResponseFormat.Count:
                    return await HandleCountResponse(request, timeoutCts.Token);

                default:
                    return BadRequest(new UdfErrorResponse
                    {
                        ErrorMessage = $"Unsupported response format: {request.ResponseFormat}"
                    });
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("[CANCELLED] UDF query was cancelled by client");
            return StatusCode(499, new UdfErrorResponse
            {
                ErrorMessage = "[CANCELLED] Query aborted by client",
                ErrorType = "ClientClosedRequest"
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing UDF query");
            return StatusCode(500, new UdfErrorResponse
            {
                ErrorMessage = "Internal server error occurred while executing the UDF query",
                ErrorType = "InternalError",
                Suggestions = new List<string>
                {
                    "Check SQL syntax and file path",
                    "Verify file format is supported",
                    "Try reducing batch size",
                    "Check server logs for details"
                }
            });
        }
    }

    /// <summary>
    /// Performs specialized text normalization using built-in UDFs with configurable transformation types.
    /// Supports full normalization, case conversion, trimming, special character removal, and format-specific cleaning.
    /// </summary>
    /// <param name="request">Text normalization request with column specification and transformation options</param>
    /// <returns>Streamed results with original and normalized text columns</returns>
    [HttpPost("normalize-text")]
    public async Task<IActionResult> NormalizeText([FromBody] UdfTextNormalizationRequest request)
    {
        var cancellationToken = HttpContext.RequestAborted;

        try
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(new UdfErrorResponse
                {
                    ErrorMessage = "Invalid request model",
                    ValidationErrors = ModelState.Values
                        .SelectMany(v => v.Errors)
                        .Select(e => e.ErrorMessage)
                        .ToList()
                });
            }

            _logger.LogInformation("Starting text normalization for column: {ColumnName} in file: {FilePath}", 
                request.TextColumnName, request.CsvFilePath);

            // Build the normalization query
            var query = BuildNormalizationQuery(request);
            _logger.LogDebug("Generated normalization query: {Query}", query);

            // Set up timeout
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(request.TimeoutSeconds));

            // Set up response for streaming
            Response.ContentType = "application/json";
            Response.Headers["X-Content-Format"] = "udf-normalization-stream";
            Response.Headers["X-UDF-Type"] = request.NormalizationType.ToString();
            Response.Headers["X-Text-Column"] = request.TextColumnName;

            // Stream the normalization results
            await Response.WriteAsync("{", cancellationToken);
            await Response.WriteAsync($"\"success\":true,", cancellationToken);
            await Response.WriteAsync($"\"normalizationType\":\"{request.NormalizationType}\",", cancellationToken);
            await Response.WriteAsync($"\"textColumn\":\"{request.TextColumnName}\",", cancellationToken);
            await Response.WriteAsync($"\"query\":\"{query.Replace("\"", "\\\"")}\",", cancellationToken);
            await Response.WriteAsync("\"data\":[", cancellationToken);

            var isFirst = true;
            var rowCount = 0;

            var streamData = await _udfService.StreamQueryWithUdfAsync(
                request.CsvFilePath, 
                query, 
                request.BatchSize, 
                timeoutCts.Token);

            await foreach (var row in streamData.WithCancellation(timeoutCts.Token))
            {
                if (!isFirst)
                {
                    await Response.WriteAsync(",", cancellationToken);
                }

                var json = JsonSerializer.Serialize(row);
                await Response.WriteAsync(json, cancellationToken);

                isFirst = false;
                rowCount++;

                // Flush every 100 rows for better streaming
                if (rowCount % 100 == 0)
                {
                    await Response.Body.FlushAsync(cancellationToken);
                }
            }

            await Response.WriteAsync("]", cancellationToken);
            await Response.WriteAsync($",\"totalRows\":{rowCount}", cancellationToken);
            await Response.WriteAsync($",\"timestamp\":\"{DateTime.UtcNow:yyyy-MM-ddTHH:mm:ss.fffZ}\"", cancellationToken);
            await Response.WriteAsync("}", cancellationToken);

            _logger.LogInformation("Text normalization completed. Processed {RowCount} rows", rowCount);
            return new EmptyResult();
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("[CANCELLED] Text normalization was cancelled by client");
            return StatusCode(499, new UdfErrorResponse
            {
                ErrorMessage = "[CANCELLED] Text normalization aborted by client",
                ErrorType = "ClientClosedRequest"
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing text normalization");
            return StatusCode(500, new UdfErrorResponse
            {
                ErrorMessage = "Internal server error occurred during text normalization",
                ErrorType = "InternalError"
            });
        }
    }

    /// <summary>
    /// Returns information about all available User-Defined Functions with parameters and usage examples.
    /// Provides comprehensive documentation for text processing, data cleaning, and utility functions.
    /// </summary>
    /// <returns>Available UDFs with descriptions, parameters, return types, and usage examples</returns>
    [HttpGet("udfs")]
    public IActionResult GetAvailableUdfs()
    {
        var udfs = new
        {
            textFunctions = new[]
            {
                new { name = "NormalizeText", description = "Comprehensive text normalization (lowercase, trim, remove special chars)", parameters = new[] { "input_text VARCHAR" }, returns = "VARCHAR" },
                new { name = "CleanPhone", description = "Phone number normalization (digits only)", parameters = new[] { "phone_text VARCHAR" }, returns = "VARCHAR" },
                new { name = "ExtractDomain", description = "Extract domain from email address", parameters = new[] { "email VARCHAR" }, returns = "VARCHAR" },
                new { name = "SafeSubstring", description = "Safe substring with bounds checking", parameters = new[] { "input_text VARCHAR", "start_pos INTEGER", "length INTEGER" }, returns = "VARCHAR" },
                new { name = "WordCount", description = "Count words in text", parameters = new[] { "input_text VARCHAR" }, returns = "INTEGER" }
            },
            examples = new
            {
                normalizeText = "SELECT NormalizeText(text_column) FROM input_data",
                cleanPhone = "SELECT CleanPhone(phone_column) FROM input_data",
                extractDomain = "SELECT ExtractDomain(email_column) FROM input_data",
                safeSubstring = "SELECT SafeSubstring(text_column, 1, 10) FROM input_data",
                wordCount = "SELECT WordCount(description_column) FROM input_data",
                combined = "SELECT customer_id, NormalizeText(name), CleanPhone(phone), ExtractDomain(email) FROM input_data WHERE WordCount(description) > 5"
            },
            tips = new[]
            {
                "Use NormalizeText() for general text cleanup and standardization",
                "CleanPhone() removes all non-digit characters from phone numbers",
                "ExtractDomain() is useful for email analytics and grouping",
                "SafeSubstring() prevents index out of bounds errors",
                "WordCount() can be used for content analysis and filtering",
                "Combine multiple UDFs for comprehensive data cleaning",
                "Always test UDFs on a small subset before processing large files"
            }
        };

        return Ok(udfs);
    }

    /// <summary>
    /// Validates a UDF query for safety, file accessibility, and provides performance recommendations.
    /// Checks for dangerous SQL operations and estimates resource usage without executing the query.
    /// </summary>
    /// <param name="request">UDF query request to validate</param>
    /// <returns>Validation result with file info, query analysis, and optimization recommendations</returns>
    [HttpPost("validate")]
    public async Task<IActionResult> ValidateUdfQuery([FromBody] UdfQueryRequest request)
    {
        try
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(new UdfErrorResponse
                {
                    ErrorMessage = "Invalid request model",
                    ValidationErrors = ModelState.Values
                        .SelectMany(v => v.Errors)
                        .Select(e => e.ErrorMessage)
                        .ToList()
                });
            }

            // Basic validations
            if (!System.IO.File.Exists(request.CsvFilePath))
            {
                return BadRequest(new UdfErrorResponse
                {
                    ErrorMessage = $"CSV file not found: {request.CsvFilePath}",
                    ErrorType = "FileNotFound"
                });
            }

            var fileInfo = new FileInfo(request.CsvFilePath);
            var fileSizeMB = fileInfo.Length / (1024.0 * 1024.0);

            // Validate SQL query for dangerous operations
            var sqlValidation = ValidateSqlSafety(request.SqlQuery);
            if (!sqlValidation.isValid)
            {
                return BadRequest(new UdfErrorResponse
                {
                    ErrorMessage = sqlValidation.errorMessage,
                    ErrorType = "InvalidSQL"
                });
            }

            var response = new
            {
                success = true,
                fileInfo = new
                {
                    path = request.CsvFilePath,
                    sizeMB = Math.Round(fileSizeMB, 2),
                    exists = true
                },
                queryInfo = new
                {
                    sqlQuery = request.SqlQuery,
                    batchSize = request.BatchSize,
                    estimatedMemoryUsageMB = Math.Round(fileSizeMB * 2, 1), // Rough estimate
                    timeoutSeconds = request.TimeoutSeconds
                },
                recommendations = GetQueryRecommendations(fileSizeMB, request.BatchSize),
                supportedUdfs = new[] { "NormalizeText", "CleanPhone", "ExtractDomain", "SafeSubstring", "WordCount" }
            };

            return Ok(response);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error validating UDF query");
            return StatusCode(500, new UdfErrorResponse
            {
                ErrorMessage = "Internal server error occurred while validating the query",
                ErrorType = "InternalError"
            });
        }
    }

    /// <summary>
    /// Handles streaming response format for UDF queries with real-time result delivery.
    /// Streams query results as JSON with configurable row limits and performance monitoring.
    /// </summary>
    /// <param name="request">UDF query request to execute</param>
    /// <param name="cancellationToken">Token for cancelling the streaming operation</param>
    /// <returns>Streaming JSON response with query results</returns>
    private async Task<IActionResult> HandleStreamingResponse(UdfQueryRequest request, CancellationToken cancellationToken)
    {
        Response.ContentType = "application/json";
        Response.Headers["X-Content-Format"] = "udf-stream";

        await Response.WriteAsync("{", cancellationToken);
        await Response.WriteAsync("\"success\":true,", cancellationToken);
        await Response.WriteAsync("\"format\":\"streaming\",", cancellationToken);
        await Response.WriteAsync("\"data\":[", cancellationToken);

        var isFirst = true;
        var rowCount = 0;

        var streamData = await _udfService.StreamQueryWithUdfAsync(
            request.CsvFilePath, 
            request.SqlQuery, 
            request.BatchSize, 
            cancellationToken);

        await foreach (var row in streamData.WithCancellation(cancellationToken))
        {
            if (request.MaxRows > 0 && rowCount >= request.MaxRows)
                break;

            if (!isFirst)
            {
                await Response.WriteAsync(",", cancellationToken);
            }

            var json = JsonSerializer.Serialize(row);
            await Response.WriteAsync(json, cancellationToken);

            isFirst = false;
            rowCount++;

            if (rowCount % 100 == 0)
            {
                await Response.Body.FlushAsync(cancellationToken);
            }
        }

        await Response.WriteAsync("]", cancellationToken);
        await Response.WriteAsync($",\"totalRows\":{rowCount}", cancellationToken);
        await Response.WriteAsync("}", cancellationToken);

        return new EmptyResult();
    }

    /// <summary>
    /// Handles buffered response format for UDF queries by collecting all results in memory.
    /// Returns complete result set as a single JSON response with optional row limiting.
    /// </summary>
    /// <param name="request">UDF query request to execute</param>
    /// <param name="cancellationToken">Token for cancelling the operation</param>
    /// <returns>Complete query results as structured JSON response</returns>
    private async Task<IActionResult> HandleBufferedResponse(UdfQueryRequest request, CancellationToken cancellationToken)
    {
        var results = await _udfService.ExecuteUdfToListAsync(
            request.CsvFilePath, 
            request.SqlQuery, 
            cancellationToken);

        if (request.MaxRows > 0 && results.Count > request.MaxRows)
        {
            results = results.Take(request.MaxRows).ToList();
        }

        var response = new UdfQueryResponse
        {
            Success = true,
            Data = results,
            Query = new UdfQueryMetadata
            {
                OriginalQuery = request.SqlQuery,
                CsvFilePath = request.CsvFilePath
            }
        };

        return Ok(response);
    }

    /// <summary>
    /// Handles count-only response format for UDF queries to get result set size without data.
    /// Executes the query wrapped in COUNT(*) to return only the number of matching rows.
    /// </summary>
    /// <param name="request">UDF query request to count</param>
    /// <param name="cancellationToken">Token for cancelling the operation</param>
    /// <returns>Count result with total number of rows that would be returned</returns>
    private async Task<IActionResult> HandleCountResponse(UdfQueryRequest request, CancellationToken cancellationToken)
    {
        var countQuery = $"SELECT COUNT(*) FROM ({request.SqlQuery}) AS count_subquery";
        
        var count = await _udfService.ExecuteUdfScalarAsync(
            request.CsvFilePath, 
            countQuery, 
            cancellationToken);

        var response = new
        {
            success = true,
            format = "count",
            totalRows = Convert.ToInt64(count ?? 0),
            query = request.SqlQuery,
            filePath = request.CsvFilePath
        };

        return Ok(response);
    }

    /// <summary>
    /// Builds a SQL query for text normalization based on the specified transformation type.
    /// Constructs appropriate UDF calls and column selections for different normalization operations.
    /// </summary>
    /// <param name="request">Text normalization request with transformation specifications</param>
    /// <returns>SQL query string with appropriate UDF calls for the requested normalization</returns>
    private string BuildNormalizationQuery(UdfTextNormalizationRequest request)
    {
        var columns = new List<string>();
        
        // Add additional columns first
        columns.AddRange(request.AdditionalColumns);

        // Add normalized text column
        var normalizedColumn = request.NormalizationType switch
        {
            UdfNormalizationType.Full => $"NormalizeText({request.TextColumnName}) AS normalized_{request.TextColumnName}",
            UdfNormalizationType.LowerOnly => $"LOWER({request.TextColumnName}) AS normalized_{request.TextColumnName}",
            UdfNormalizationType.TrimOnly => $"TRIM({request.TextColumnName}) AS normalized_{request.TextColumnName}",
            UdfNormalizationType.RemoveSpecial => $"REGEXP_REPLACE({request.TextColumnName}, '[^a-zA-Z0-9\\s]', '', 'g') AS normalized_{request.TextColumnName}",
            UdfNormalizationType.Phone => $"CleanPhone({request.TextColumnName}) AS normalized_{request.TextColumnName}",
            UdfNormalizationType.Email => $"ExtractDomain({request.TextColumnName}) AS normalized_{request.TextColumnName}",
            _ => $"NormalizeText({request.TextColumnName}) AS normalized_{request.TextColumnName}"
        };
        
        columns.Add(normalizedColumn);

        var selectClause = string.Join(", ", columns);
        var query = $"SELECT {selectClause} FROM input_data";

        if (!string.IsNullOrEmpty(request.WhereClause))
        {
            query += $" WHERE {request.WhereClause}";
        }

        if (!string.IsNullOrEmpty(request.OrderBy))
        {
            query += $" ORDER BY {request.OrderBy}";
        }

        return query;
    }

    /// <summary>
    /// Validates SQL query for safety by checking for prohibited operations and injection patterns.
    /// Prevents execution of destructive operations like DELETE, UPDATE, DROP, etc.
    /// </summary>
    /// <param name="sql">SQL query to validate</param>
    /// <returns>Validation result with success status and error message if dangerous patterns are found</returns>
    private (bool isValid, string errorMessage) ValidateSqlSafety(string sql)
    {
        var dangerousPatterns = new[]
        {
            @"\bDELETE\b", @"\bUPDATE\b", @"\bINSERT\b", @"\bDROP\b",
            @"\bCREATE\b", @"\bALTER\b", @"\bTRUNCATE\b", @"\bEXEC\b"
        };

        foreach (var pattern in dangerousPatterns)
        {
            if (System.Text.RegularExpressions.Regex.IsMatch(sql, pattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase))
            {
                return (false, $"SQL contains prohibited operation: {pattern}");
            }
        }

        return (true, string.Empty);
    }

    /// <summary>
    /// Generates performance recommendations based on file size and batch configuration.
    /// Provides suggestions for memory optimization, batch sizing, and processing strategies.
    /// </summary>
    /// <param name="fileSizeMB">Size of the file being processed in megabytes</param>
    /// <param name="batchSize">Configured batch size for processing</param>
    /// <returns>List of performance optimization recommendations</returns>
    private List<string> GetQueryRecommendations(double fileSizeMB, int batchSize)
    {
        var recommendations = new List<string>();

        if (fileSizeMB > 100)
        {
            recommendations.Add("Large file detected - consider using WHERE clauses to filter data");
            recommendations.Add("Monitor memory usage during processing");
        }

        if (batchSize > 50000)
        {
            recommendations.Add("Large batch size may cause memory issues - consider reducing to 10,000-25,000");
        }

        if (fileSizeMB > 500)
        {
            recommendations.Add("Very large file - consider processing in chunks or using streaming format");
        }

        return recommendations;
    }
}