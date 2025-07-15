using System.ComponentModel.DataAnnotations;

namespace ReportBuilder.Request;

public class ParquetQueryRequest
{
    public string? ParquetFilePath { get; set; }
    
    public IFormFile? ParquetFile { get; set; }
    
    public List<string> SelectColumns { get; set; } = new();
    
    public string? FilterExpression { get; set; }
    
    public string? OrderBy { get; set; }
    
    public int? Limit { get; set; }
    
    public int PageSize { get; set; } = 1000;
    
    public bool UseDefaultQuery { get; set; } = false;
    
    public int? SampleSize { get; set; }
}

public class ParquetSchemaRequest
{
    public string? ParquetFilePath { get; set; }
    
    public IFormFile? ParquetFile { get; set; }
    
    public bool IncludeDataTypes { get; set; } = true;
    
    public int? SampleRows { get; set; } = 5;
}