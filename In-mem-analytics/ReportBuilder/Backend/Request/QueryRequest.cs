using System.ComponentModel.DataAnnotations;

namespace ReportBuilder.Request;

/// <summary>
/// Basic query request model for executing SQL queries on local datasets.
/// Provides fundamental query execution capabilities with projection and filtering options.
/// </summary>
public class QueryRequest
{
    /// <summary>
    /// Gets or sets the SQL query to execute against the dataset.
    /// Must be a valid SQL query compatible with DuckDB syntax.
    /// </summary>
    [Required]
    public string Query { get; set; } = string.Empty;
    
    /// <summary>
    /// Gets or sets the path to the dataset file to query.
    /// Supports local file paths for CSV, Parquet, and other supported formats.
    /// </summary>
    [Required]
    public string DatasetPath { get; set; } = string.Empty;
    
    /// <summary>
    /// Gets or sets optional column projection to limit returned columns.
    /// Comma-separated list of column names to include in results.
    /// </summary>
    public string? Projection { get; set; }
    
    /// <summary>
    /// Gets or sets optional filter expression to apply to the dataset.
    /// SQL WHERE clause conditions to filter data before processing.
    /// </summary>
    public string? Filter { get; set; }
    
    /// <summary>
    /// Gets or sets the page size for result pagination.
    /// Controls the number of rows returned per page. Default: 1000.
    /// </summary>
    public int PageSize { get; set; } = 1000;
}