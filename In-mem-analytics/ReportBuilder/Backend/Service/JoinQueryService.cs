using DuckDB.NET.Data;
using ReportBuilder.Request;
using ReportBuilder.Response;
using System.Data;
using System.Runtime.CompilerServices;

namespace ReportBuilder.Service;

public interface IJoinQueryService
{
    Task<JoinQueryResponse> ExecuteJoinQueryAsync(JoinQueryRequest request, CancellationToken cancellationToken = default);
}

public class JoinQueryService : IJoinQueryService
{
    private readonly ILogger<JoinQueryService> _logger;
    private const long MaxMemoryBytes = 400 * 1024 * 1024; // 400MB
    private const long WarnMemoryBytes = 300 * 1024 * 1024; // 300MB warning threshold
    private const long MaxFileSize = 100 * 1024 * 1024; // 100MB per file
    private const int PageSize = 1000;

    public JoinQueryService(ILogger<JoinQueryService> logger)
    {
        _logger = logger;
    }

    public async Task<JoinQueryResponse> ExecuteJoinQueryAsync(JoinQueryRequest request, CancellationToken cancellationToken = default)
    {
        var initialMemory = GC.GetTotalMemory(true);
        var maxMemory = initialMemory;
        var peakMemory = initialMemory;
        string? leftTempPath = null;
        string? rightTempPath = null;

        _logger.LogInformation("Starting JOIN query execution. Initial memory: {MemoryMB} MB", initialMemory / (1024 * 1024));

        try
        {
            // Check file sizes before processing
            var leftFileSize = request.LeftFile.Length;
            var rightFileSize = request.RightFile.Length;
            
            _logger.LogInformation("File sizes - Left: {LeftSizeKB} KB, Right: {RightSizeKB} KB", 
                leftFileSize / 1024, rightFileSize / 1024);

            // Check for Databricks recommendation early
            var databricksSuggestion = CheckDatabricksRecommendation(leftFileSize, rightFileSize, initialMemory);
            if (databricksSuggestion.ShouldUseDatabricks)
            {
                return new JoinQueryResponse
                {
                    Success = false,
                    ErrorMessage = "Files too large for in-memory processing. Consider using Databricks.",
                    InitialMemoryUsage = initialMemory,
                    FinalMemoryUsage = GC.GetTotalMemory(false),
                    MaxMemoryUsage = maxMemory,
                    LeftFileSize = leftFileSize,
                    RightFileSize = rightFileSize,
                    DatabricksSuggestion = databricksSuggestion
                };
            }

            // Save uploaded files temporarily
            leftTempPath = await SaveTempFileAsync(request.LeftFile, "left", cancellationToken);
            rightTempPath = await SaveTempFileAsync(request.RightFile, "right", cancellationToken);

            // Check memory after file operations
            var memoryAfterFiles = GC.GetTotalMemory(false);
            maxMemory = Math.Max(maxMemory, memoryAfterFiles);
            peakMemory = Math.Max(peakMemory, memoryAfterFiles);

            using var connection = new DuckDBConnection("Data Source=:memory:");
            await connection.OpenAsync(cancellationToken);

            // Get available columns from both files
            var leftColumns = await GetCsvColumnsAsync(connection, leftTempPath, "left_table", cancellationToken);
            var rightColumns = await GetCsvColumnsAsync(connection, rightTempPath, "right_table", cancellationToken);

            // Validate join column exists in both files
            var joinColumnExists = ValidateJoinColumn(request.JoinColumn, leftColumns, rightColumns);
            if (!joinColumnExists.isValid)
            {
                return new JoinQueryResponse
                {
                    Success = false,
                    ErrorMessage = joinColumnExists.errorMessage,
                    InitialMemoryUsage = initialMemory,
                    FinalMemoryUsage = GC.GetTotalMemory(false),
                    MaxMemoryUsage = maxMemory,
                    LeftFileSize = leftFileSize,
                    RightFileSize = rightFileSize
                };
            }

            // Validate selected columns exist
            var columnValidation = ValidateSelectColumns(request.SelectColumns, request.LeftAlias, request.RightAlias, leftColumns, rightColumns);
            if (!columnValidation.isValid)
            {
                return new JoinQueryResponse
                {
                    Success = false,
                    ErrorMessage = columnValidation.errorMessage,
                    InitialMemoryUsage = initialMemory,
                    FinalMemoryUsage = GC.GetTotalMemory(false),
                    MaxMemoryUsage = maxMemory,
                    LeftFileSize = leftFileSize,
                    RightFileSize = rightFileSize
                };
            }

            // Build JOIN query
            var query = BuildJoinQuery(request, leftTempPath, rightTempPath);
            _logger.LogInformation("Executing JOIN query: {Query}", query);

            // Monitor memory before executing JOIN
            var memoryBeforeJoin = GC.GetTotalMemory(false);
            maxMemory = Math.Max(maxMemory, memoryBeforeJoin);
            peakMemory = Math.Max(peakMemory, memoryBeforeJoin);

            if (memoryBeforeJoin > WarnMemoryBytes)
            {
                _logger.LogWarning("Memory usage before JOIN: {MemoryMB} MB - approaching limits", memoryBeforeJoin / (1024 * 1024));
            }

            // Execute streaming JOIN query
            var data = StreamJoinResults(connection, query, request.PageSize, cancellationToken);
            var finalMemory = GC.GetTotalMemory(false);
            maxMemory = Math.Max(maxMemory, finalMemory);
            peakMemory = Math.Max(peakMemory, finalMemory);

            _logger.LogInformation("JOIN query completed. Final memory: {MemoryMB} MB, Peak: {PeakMemoryMB} MB", 
                finalMemory / (1024 * 1024), peakMemory / (1024 * 1024));

            return new JoinQueryResponse
            {
                Success = true,
                InitialMemoryUsage = initialMemory,
                FinalMemoryUsage = finalMemory,
                MaxMemoryUsage = maxMemory,
                PeakMemoryUsage = peakMemory,
                Columns = request.SelectColumns,
                QueryExecuted = query,
                LeftFileSize = leftFileSize,
                RightFileSize = rightFileSize,
                Data = data,
                DatabricksSuggestion = peakMemory > WarnMemoryBytes ? 
                    CheckDatabricksRecommendation(leftFileSize, rightFileSize, peakMemory) : null
            };
        }
        catch (Exception ex)
        {
            var errorMemory = GC.GetTotalMemory(false);
            maxMemory = Math.Max(maxMemory, errorMemory);
            peakMemory = Math.Max(peakMemory, errorMemory);
            _logger.LogError(ex, "Error executing JOIN query");
            
            return new JoinQueryResponse
            {
                Success = false,
                ErrorMessage = ex.Message,
                InitialMemoryUsage = initialMemory,
                FinalMemoryUsage = errorMemory,
                MaxMemoryUsage = maxMemory,
                PeakMemoryUsage = peakMemory,
                LeftFileSize = request.LeftFile.Length,
                RightFileSize = request.RightFile.Length,
                DatabricksSuggestion = CheckDatabricksRecommendation(request.LeftFile.Length, request.RightFile.Length, errorMemory)
            };
        }
        finally
        {
            // Clean up temp files
            _ = CleanupTempFiles(leftTempPath, rightTempPath);
        }
    }

    private async Task<string> SaveTempFileAsync(IFormFile file, string prefix, CancellationToken cancellationToken)
    {
        var tempPath = Path.GetTempFileName();
        var tempCsvPath = Path.ChangeExtension(tempPath, $".{prefix}.csv");
        
        System.IO.File.Delete(tempPath);
        
        using var fileStream = new FileStream(tempCsvPath, FileMode.Create, FileAccess.Write);
        await file.CopyToAsync(fileStream, cancellationToken);
        
        _logger.LogDebug("Saved {Prefix} file to: {FilePath}, Size: {SizeKB} KB", 
            prefix, tempCsvPath, file.Length / 1024);
        
        return tempCsvPath;
    }

    private async Task<List<string>> GetCsvColumnsAsync(DuckDBConnection connection, string filePath, string tableName, CancellationToken cancellationToken)
    {
        var query = $"DESCRIBE SELECT * FROM read_csv_auto('{filePath}') LIMIT 1";
        using var command = new DuckDBCommand(query, connection);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        
        var columns = new List<string>();
        while (await reader.ReadAsync(cancellationToken))
        {
            columns.Add(reader.GetString("column_name"));
        }
        
        _logger.LogDebug("{TableName} columns: {Columns}", tableName, string.Join(", ", columns));
        return columns;
    }

    private (bool isValid, string errorMessage) ValidateJoinColumn(string joinColumn, List<string> leftColumns, List<string> rightColumns)
    {
        var leftHasColumn = leftColumns.Contains(joinColumn, StringComparer.OrdinalIgnoreCase);
        var rightHasColumn = rightColumns.Contains(joinColumn, StringComparer.OrdinalIgnoreCase);
        
        if (!leftHasColumn && !rightHasColumn)
        {
            return (false, $"Join column '{joinColumn}' not found in either file");
        }
        if (!leftHasColumn)
        {
            return (false, $"Join column '{joinColumn}' not found in left file");
        }
        if (!rightHasColumn)
        {
            return (false, $"Join column '{joinColumn}' not found in right file");
        }
        
        return (true, string.Empty);
    }

    private (bool isValid, string errorMessage) ValidateSelectColumns(List<string> selectColumns, string leftAlias, string rightAlias, List<string> leftColumns, List<string> rightColumns)
    {
        var missingColumns = new List<string>();
        
        foreach (var column in selectColumns)
        {
            if (column.StartsWith($"{leftAlias}."))
            {
                var columnName = column.Substring(2);
                if (!leftColumns.Contains(columnName, StringComparer.OrdinalIgnoreCase))
                {
                    missingColumns.Add(column);
                }
            }
            else if (column.StartsWith($"{rightAlias}."))
            {
                var columnName = column.Substring(2);
                if (!rightColumns.Contains(columnName, StringComparer.OrdinalIgnoreCase))
                {
                    missingColumns.Add(column);
                }
            }
        }
        
        if (missingColumns.Any())
        {
            return (false, $"Columns not found: {string.Join(", ", missingColumns)}");
        }
        
        return (true, string.Empty);
    }

    private string BuildJoinQuery(JoinQueryRequest request, string leftPath, string rightPath)
    {
        var joinTypeStr = request.JoinType switch
        {
            JoinType.Left => "LEFT JOIN",
            JoinType.Right => "RIGHT JOIN",
            JoinType.Full => "FULL OUTER JOIN",
            _ => "JOIN"
        };
        
        var selectClause = string.Join(", ", request.SelectColumns);
        var query = $@"
SELECT {selectClause}
FROM read_csv_auto('{leftPath}') AS {request.LeftAlias}
{joinTypeStr} read_csv_auto('{rightPath}') AS {request.RightAlias}
ON {request.LeftAlias}.{request.JoinColumn} = {request.RightAlias}.{request.JoinColumn}";
        
        if (!string.IsNullOrEmpty(request.WhereClause))
        {
            query += $" WHERE {request.WhereClause}";
        }
        
        if (!string.IsNullOrEmpty(request.OrderBy))
        {
            query += $" ORDER BY {request.OrderBy}";
        }
        
        if (request.Limit.HasValue)
        {
            query += $" LIMIT {request.Limit.Value}";
        }
        
        return query.Trim();
    }

    private async IAsyncEnumerable<Dictionary<string, object?>> StreamJoinResults(
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
            // Check memory usage every 250 rows
            if (rowCount % 250 == 0)
            {
                var currentMemory = GC.GetTotalMemory(false);
                if (currentMemory > MaxMemoryBytes)
                {
                    _logger.LogWarning("Memory limit exceeded during JOIN streaming: {MemoryMB} MB", currentMemory / (1024 * 1024));
                    throw new InvalidOperationException($"Memory limit exceeded during JOIN streaming: {currentMemory / (1024 * 1024)} MB. Consider using Databricks for large datasets.");
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
                
                // Force garbage collection every 3 batches
                if (rowCount % (pageSize * 3) == 0)
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    _logger.LogDebug("Forced GC after {RowCount} JOIN rows", rowCount);
                }
            }
        }

        // Yield remaining items
        foreach (var item in batch)
        {
            yield return item;
        }
        
        _logger.LogInformation("Streamed {RowCount} JOIN result rows", rowCount);
    }

    private DatabricksSuggestion CheckDatabricksRecommendation(long leftFileSize, long rightFileSize, long currentMemory)
    {
        var totalFileSize = leftFileSize + rightFileSize;
        var estimatedMemoryRequired = totalFileSize * 3; // Rough estimate for JOIN operations
        
        var shouldUseDatabricks = 
            totalFileSize > MaxFileSize * 2 || 
            currentMemory > WarnMemoryBytes ||
            estimatedMemoryRequired > MaxMemoryBytes;
        
        if (!shouldUseDatabricks)
            return new DatabricksSuggestion { ShouldUseDatabricks = false };
        
        var reason = totalFileSize > MaxFileSize * 2 
            ? "Combined file size exceeds recommended limits"
            : "Memory usage is approaching system limits";
        
        return new DatabricksSuggestion
        {
            ShouldUseDatabricks = true,
            Reason = reason,
            EstimatedMemoryRequired = estimatedMemoryRequired,
            SuggestedSqlCode = @"
-- Databricks SQL for large JOIN operations
CREATE OR REPLACE TEMPORARY VIEW sales AS
SELECT * FROM read_files('dbfs:/path/to/sales.csv');

CREATE OR REPLACE TEMPORARY VIEW products AS  
SELECT * FROM read_files('dbfs:/path/to/products.csv');

SELECT s.order_id, p.product_name, s.amount
FROM sales s
JOIN products p ON s.product_id = p.product_id
WHERE s.amount > 100;",
            OptimizationTips = new List<string>
            {
                "Use Delta Lake format for better performance",
                "Consider partitioning large tables",
                "Apply filters early in the query",
                "Use broadcast joins for smaller lookup tables",
                "Enable adaptive query execution"
            }
        };
    }

    private Task CleanupTempFiles(string? leftPath, string? rightPath)
    {
        var filesToDelete = new[] { leftPath, rightPath }.Where(path => path != null).ToList();
        
        foreach (var filePath in filesToDelete)
        {
            try
            {
                if (System.IO.File.Exists(filePath))
                {
                    System.IO.File.Delete(filePath);
                    _logger.LogDebug("Deleted temporary file: {FilePath}", filePath);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to delete temporary file: {FilePath}", filePath);
            }
        }
        
        return Task.CompletedTask;
    }
}