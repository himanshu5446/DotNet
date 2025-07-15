using DuckDB.NET.Data;
using ReportBuilder.Request;
using ReportBuilder.Response;
using System.Data;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

namespace ReportBuilder.Service;

public interface IParquetQueryService
{
    Task<ParquetQueryResponse> ExecuteParquetQueryAsync(ParquetQueryRequest request, CancellationToken cancellationToken = default);
    Task<ParquetSchemaResponse> GetParquetSchemaAsync(ParquetSchemaRequest request, CancellationToken cancellationToken = default);
}

public class ParquetQueryService : IParquetQueryService
{
    private readonly ILogger<ParquetQueryService> _logger;
    private const long MaxMemoryBytes = 500 * 1024 * 1024; // 500MB
    private const long WarnMemoryBytes = 400 * 1024 * 1024; // 400MB warning
    private const int MaxPageSize = 10000;
    private const int DefaultPageSize = 1000;

    public ParquetQueryService(ILogger<ParquetQueryService> logger)
    {
        _logger = logger;
    }

    public async Task<ParquetQueryResponse> ExecuteParquetQueryAsync(ParquetQueryRequest request, CancellationToken cancellationToken = default)
    {
        var initialMemory = GC.GetTotalMemory(true);
        var maxMemory = initialMemory;
        var peakMemory = initialMemory;
        string? tempFilePath = null;
        var warnings = new List<string>();

        _logger.LogInformation("Starting Parquet query execution. Initial memory: {MemoryMB} MB", initialMemory / (1024 * 1024));

        try
        {
            // Determine file path
            var filePath = await GetFilePathAsync(request, cancellationToken);
            tempFilePath = filePath.tempPath;
            var actualFilePath = filePath.actualPath;

            if (!System.IO.File.Exists(actualFilePath))
            {
                return new ParquetQueryResponse
                {
                    Success = false,
                    ErrorMessage = $"Parquet file not found: {actualFilePath}",
                    InitialMemoryUsage = initialMemory,
                    FinalMemoryUsage = GC.GetTotalMemory(false)
                };
            }

            var fileSize = new FileInfo(actualFilePath).Length;
            _logger.LogInformation("Processing Parquet file: {FilePath}, Size: {SizeKB} KB", actualFilePath, fileSize / 1024);

            using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync(cancellationToken);

            // Get available columns
            var availableColumns = await GetParquetColumnsAsync(connection, actualFilePath, cancellationToken);
            
            // Validate and optimize request
            var validation = ValidateAndOptimizeRequest(request, availableColumns, warnings);
            if (!validation.isValid)
            {
                return new ParquetQueryResponse
                {
                    Success = false,
                    ErrorMessage = validation.errorMessage,
                    InitialMemoryUsage = initialMemory,
                    FinalMemoryUsage = GC.GetTotalMemory(false),
                    FileSize = fileSize,
                    FilePath = actualFilePath,
                    Warnings = warnings
                };
            }

            // Build optimized query
            var queryBuilder = BuildOptimizedQuery(request, actualFilePath, availableColumns);
            var query = queryBuilder.query;
            var optimizations = queryBuilder.optimizations;
            
            _logger.LogInformation("Executing optimized Parquet query: {Query}", query);

            // Check memory before execution
            var memoryBeforeQuery = GC.GetTotalMemory(false);
            maxMemory = Math.Max(maxMemory, memoryBeforeQuery);
            peakMemory = Math.Max(peakMemory, memoryBeforeQuery);

            if (memoryBeforeQuery > WarnMemoryBytes)
            {
                warnings.Add($"High memory usage before query execution: {memoryBeforeQuery / (1024 * 1024)} MB");
                _logger.LogWarning("High memory usage before query: {MemoryMB} MB", memoryBeforeQuery / (1024 * 1024));
            }

            // Execute streaming query
            var data = StreamParquetResults(connection, query, request.PageSize, cancellationToken);
            var finalMemory = GC.GetTotalMemory(false);
            maxMemory = Math.Max(maxMemory, finalMemory);
            peakMemory = Math.Max(peakMemory, finalMemory);

            _logger.LogInformation("Parquet query completed. Final memory: {MemoryMB} MB, Peak: {PeakMemoryMB} MB", 
                finalMemory / (1024 * 1024), peakMemory / (1024 * 1024));

            return new ParquetQueryResponse
            {
                Success = true,
                InitialMemoryUsage = initialMemory,
                FinalMemoryUsage = finalMemory,
                MaxMemoryUsage = maxMemory,
                PeakMemoryUsage = peakMemory,
                Columns = request.SelectColumns.Any() ? request.SelectColumns : availableColumns,
                QueryExecuted = query,
                FileSize = fileSize,
                FilePath = actualFilePath,
                Optimizations = optimizations,
                Data = data,
                Warnings = warnings
            };
        }
        catch (Exception ex)
        {
            var errorMemory = GC.GetTotalMemory(false);
            maxMemory = Math.Max(maxMemory, errorMemory);
            peakMemory = Math.Max(peakMemory, errorMemory);
            _logger.LogError(ex, "Error executing Parquet query");
            
            return new ParquetQueryResponse
            {
                Success = false,
                ErrorMessage = ex.Message,
                InitialMemoryUsage = initialMemory,
                FinalMemoryUsage = errorMemory,
                MaxMemoryUsage = maxMemory,
                PeakMemoryUsage = peakMemory,
                FileSize = request.ParquetFile?.Length ?? 0,
                FilePath = request.ParquetFilePath,
                Warnings = warnings
            };
        }
        finally
        {
            // Clean up temp file
            if (tempFilePath != null && System.IO.File.Exists(tempFilePath))
            {
                try
                {
                    System.IO.File.Delete(tempFilePath);
                    _logger.LogDebug("Deleted temporary Parquet file: {FilePath}", tempFilePath);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to delete temporary file: {FilePath}", tempFilePath);
                }
            }
        }
    }

    public async Task<ParquetSchemaResponse> GetParquetSchemaAsync(ParquetSchemaRequest request, CancellationToken cancellationToken = default)
    {
        string? tempFilePath = null;
        
        try
        {
            var filePath = await GetSchemaFilePathAsync(request, cancellationToken);
            tempFilePath = filePath.tempPath;
            var actualFilePath = filePath.actualPath;

            if (!System.IO.File.Exists(actualFilePath))
            {
                return new ParquetSchemaResponse
                {
                    Success = false,
                    ErrorMessage = $"Parquet file not found: {actualFilePath}"
                };
            }

            var fileSize = new FileInfo(actualFilePath).Length;
            
            using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync(cancellationToken);

            // Get schema information
            var columns = await GetParquetSchemaAsync(connection, actualFilePath, cancellationToken);
            
            // Get sample data if requested
            var sampleData = new List<Dictionary<string, object?>>();
            if (request.SampleRows.HasValue && request.SampleRows.Value > 0)
            {
                sampleData = await GetSampleDataAsync(connection, actualFilePath, request.SampleRows.Value, cancellationToken);
            }

            return new ParquetSchemaResponse
            {
                Success = true,
                FileSize = fileSize,
                FilePath = actualFilePath,
                Columns = columns,
                SampleData = sampleData
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting Parquet schema");
            return new ParquetSchemaResponse
            {
                Success = false,
                ErrorMessage = ex.Message
            };
        }
        finally
        {
            if (tempFilePath != null && System.IO.File.Exists(tempFilePath))
            {
                try
                {
                    System.IO.File.Delete(tempFilePath);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to delete temporary file: {FilePath}", tempFilePath);
                }
            }
        }
    }

    private async Task<(string? tempPath, string actualPath)> GetFilePathAsync(ParquetQueryRequest request, CancellationToken cancellationToken)
    {
        if (request.ParquetFile != null)
        {
            var tempPath = await SaveTempParquetFileAsync(request.ParquetFile, cancellationToken);
            return (tempPath, tempPath);
        }
        
        if (!string.IsNullOrEmpty(request.ParquetFilePath))
        {
            return (null, request.ParquetFilePath);
        }
        
        throw new ArgumentException("Either ParquetFile or ParquetFilePath must be provided");
    }

    private async Task<(string? tempPath, string actualPath)> GetSchemaFilePathAsync(ParquetSchemaRequest request, CancellationToken cancellationToken)
    {
        if (request.ParquetFile != null)
        {
            var tempPath = await SaveTempParquetFileAsync(request.ParquetFile, cancellationToken);
            return (tempPath, tempPath);
        }
        
        if (!string.IsNullOrEmpty(request.ParquetFilePath))
        {
            return (null, request.ParquetFilePath);
        }
        
        throw new ArgumentException("Either ParquetFile or ParquetFilePath must be provided");
    }

    private async Task<string> SaveTempParquetFileAsync(IFormFile file, CancellationToken cancellationToken)
    {
        var tempPath = Path.GetTempFileName();
        var tempParquetPath = Path.ChangeExtension(tempPath, ".parquet");
        
        System.IO.File.Delete(tempPath);
        
        using var fileStream = new FileStream(tempParquetPath, FileMode.Create, FileAccess.Write);
        await file.CopyToAsync(fileStream, cancellationToken);
        
        _logger.LogDebug("Saved Parquet file to: {FilePath}, Size: {SizeKB} KB", 
            tempParquetPath, file.Length / 1024);
        
        return tempParquetPath;
    }

    private async Task<List<string>> GetParquetColumnsAsync(DuckDBConnection connection, string filePath, CancellationToken cancellationToken)
    {
        var query = $"DESCRIBE SELECT * FROM parquet_scan('{filePath}') LIMIT 1";
        using var command = new DuckDBCommand(query, connection);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        
        var columns = new List<string>();
        while (await reader.ReadAsync(cancellationToken))
        {
            columns.Add(reader.GetString("column_name"));
        }
        
        _logger.LogDebug("Parquet columns: {Columns}", string.Join(", ", columns));
        return columns;
    }

    private async Task<List<ParquetColumn>> GetParquetSchemaAsync(DuckDBConnection connection, string filePath, CancellationToken cancellationToken)
    {
        var query = $"DESCRIBE SELECT * FROM parquet_scan('{filePath}') LIMIT 1";
        using var command = new DuckDBCommand(query, connection);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        
        var columns = new List<ParquetColumn>();
        while (await reader.ReadAsync(cancellationToken))
        {
            columns.Add(new ParquetColumn
            {
                Name = reader.GetString("column_name"),
                DataType = reader.GetString("column_type"),
                IsNullable = reader.GetString("null").Equals("YES", StringComparison.OrdinalIgnoreCase)
            });
        }
        
        return columns;
    }

    private async Task<List<Dictionary<string, object?>>> GetSampleDataAsync(DuckDBConnection connection, string filePath, int sampleRows, CancellationToken cancellationToken)
    {
        var query = $"SELECT * FROM parquet_scan('{filePath}') LIMIT {sampleRows}";
        using var command = new DuckDBCommand(query, connection);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        
        var columnNames = new string[reader.FieldCount];
        for (int i = 0; i < reader.FieldCount; i++)
        {
            columnNames[i] = reader.GetName(i);
        }

        var sampleData = new List<Dictionary<string, object?>>();
        while (await reader.ReadAsync(cancellationToken))
        {
            var row = new Dictionary<string, object?>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row[columnNames[i]] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }
            sampleData.Add(row);
        }

        return sampleData;
    }

    private (bool isValid, string errorMessage) ValidateAndOptimizeRequest(ParquetQueryRequest request, List<string> availableColumns, List<string> warnings)
    {
        // Validate page size
        if (request.PageSize <= 0 || request.PageSize > MaxPageSize)
        {
            request.PageSize = DefaultPageSize;
            warnings.Add($"Invalid page size, using default: {DefaultPageSize}");
        }

        // Validate columns if provided
        if (request.SelectColumns.Any())
        {
            var invalidColumns = request.SelectColumns.Except(availableColumns, StringComparer.OrdinalIgnoreCase).ToList();
            if (invalidColumns.Any())
            {
                return (false, $"Invalid columns: {string.Join(", ", invalidColumns)}. Available columns: {string.Join(", ", availableColumns)}");
            }
        }
        else
        {
            warnings.Add("No columns specified - selecting all columns. Consider using projection for better performance.");
        }

        // Validate filter expression
        if (!string.IsNullOrEmpty(request.FilterExpression))
        {
            var filterValidation = ValidateFilterExpression(request.FilterExpression, availableColumns);
            if (!filterValidation.isValid)
            {
                return (false, $"Invalid filter expression: {filterValidation.errorMessage}");
            }
        }
        else
        {
            warnings.Add("No filter specified - scanning entire file. Consider adding filters for better performance.");
        }

        return (true, string.Empty);
    }

    private (bool isValid, string errorMessage) ValidateFilterExpression(string filterExpression, List<string> availableColumns)
    {
        try
        {
            // Basic validation - check if referenced columns exist
            var columnPattern = @"\b([a-zA-Z_][a-zA-Z0-9_]*)\b";
            var matches = Regex.Matches(filterExpression, columnPattern);
            
            var referencedColumns = matches.Cast<Match>()
                .Select(m => m.Value)
                .Where(col => !IsReservedKeyword(col))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            var invalidColumns = referencedColumns.Except(availableColumns, StringComparer.OrdinalIgnoreCase).ToList();
            if (invalidColumns.Any())
            {
                return (false, $"Filter references invalid columns: {string.Join(", ", invalidColumns)}");
            }

            // Check for SQL injection patterns
            var dangerousPatterns = new[] { ";", "--", "/*", "*/", "xp_", "sp_", "exec", "execute", "union", "insert", "update", "delete", "drop", "create", "alter" };
            var lowerFilter = filterExpression.ToLower();
            
            foreach (var pattern in dangerousPatterns)
            {
                if (lowerFilter.Contains(pattern))
                {
                    return (false, $"Filter contains potentially dangerous pattern: {pattern}");
                }
            }

            return (true, string.Empty);
        }
        catch (Exception ex)
        {
            return (false, $"Filter validation error: {ex.Message}");
        }
    }

    private bool IsReservedKeyword(string word)
    {
        var keywords = new[] { "and", "or", "not", "in", "like", "between", "is", "null", "true", "false", "case", "when", "then", "else", "end" };
        return keywords.Contains(word.ToLower());
    }

    private (string query, QueryOptimizations optimizations) BuildOptimizedQuery(ParquetQueryRequest request, string filePath, List<string> availableColumns)
    {
        var optimizations = new QueryOptimizations();
        
        // Build SELECT clause with projection pushdown
        string selectClause;
        if (request.SelectColumns.Any())
        {
            selectClause = string.Join(", ", request.SelectColumns.Select(col => $"'{col}'"));
            optimizations.ProjectionPushdown = true;
            optimizations.ColumnPruning = true;
            optimizations.ProjectedColumns = request.SelectColumns.Count;
        }
        else
        {
            selectClause = "*";
            optimizations.ProjectedColumns = availableColumns.Count;
        }
        
        optimizations.TotalColumns = availableColumns.Count;
        
        // Build base query
        var query = $"SELECT {selectClause} FROM parquet_scan('{filePath}')";
        
        // Add WHERE clause with filter pushdown
        if (!string.IsNullOrEmpty(request.FilterExpression))
        {
            query += $" WHERE {request.FilterExpression}";
            optimizations.FilterPushdown = true;
        }
        
        // Add ORDER BY
        if (!string.IsNullOrEmpty(request.OrderBy))
        {
            query += $" ORDER BY {request.OrderBy}";
        }
        
        // Add LIMIT
        if (request.Limit.HasValue)
        {
            query += $" LIMIT {request.Limit.Value}";
        }
        
        // Generate optimization summary
        var optimizationFeatures = new List<string>();
        if (optimizations.ProjectionPushdown) optimizationFeatures.Add("projection pushdown");
        if (optimizations.FilterPushdown) optimizationFeatures.Add("filter pushdown");
        if (optimizations.ColumnPruning) optimizationFeatures.Add("column pruning");
        
        optimizations.OptimizationSummary = optimizationFeatures.Any() 
            ? $"Applied: {string.Join(", ", optimizationFeatures)}" 
            : "No optimizations applied";
        
        return (query, optimizations);
    }

    private async IAsyncEnumerable<Dictionary<string, object?>> StreamParquetResults(
        DuckDBConnection connection,
        string query,
        int pageSize,
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
        var batch = new List<Dictionary<string, object?>>();

        while (await reader.ReadAsync(cancellationToken))
        {
            // Check memory usage every 200 rows
            if (rowCount % 200 == 0)
            {
                var currentMemory = GC.GetTotalMemory(false);
                if (currentMemory > MaxMemoryBytes)
                {
                    _logger.LogWarning("Memory limit exceeded during Parquet streaming: {MemoryMB} MB", currentMemory / (1024 * 1024));
                    throw new InvalidOperationException($"Memory limit exceeded during Parquet streaming: {currentMemory / (1024 * 1024)} MB");
                }
            }

            var row = new Dictionary<string, object?>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row[columnNames[i]] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            batch.Add(row);
            rowCount++;

            // Yield batch when page size is reached
            if (batch.Count >= pageSize)
            {
                foreach (var item in batch)
                {
                    yield return item;
                }
                batch.Clear();
                
                // Force GC every 4 batches
                if (rowCount % (pageSize * 4) == 0)
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    _logger.LogDebug("Forced GC after {RowCount} Parquet rows", rowCount);
                }
            }
        }

        // Yield remaining items
        foreach (var item in batch)
        {
            yield return item;
        }
        
        _logger.LogInformation("Streamed {RowCount} Parquet rows", rowCount);
    }
}