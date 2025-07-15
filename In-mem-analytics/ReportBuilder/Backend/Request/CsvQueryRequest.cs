using System.ComponentModel.DataAnnotations;

namespace ReportBuilder.Request;

public class CsvQueryRequest
{
    [Required]
    public IFormFile CsvFile { get; set; } = null!;
    
    [Required]
    public List<string> Columns { get; set; } = new();
    
    public int PageSize { get; set; } = 1000;
    
    public string? WhereClause { get; set; }
    
    public string? OrderBy { get; set; }
    
    public int? Limit { get; set; }
}