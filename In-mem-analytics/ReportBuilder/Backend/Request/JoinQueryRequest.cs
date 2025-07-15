using System.ComponentModel.DataAnnotations;

namespace ReportBuilder.Request;

public class JoinQueryRequest
{
    [Required]
    public IFormFile LeftFile { get; set; } = null!;
    
    [Required]
    public IFormFile RightFile { get; set; } = null!;
    
    [Required]
    public string LeftAlias { get; set; } = "s";
    
    [Required]
    public string RightAlias { get; set; } = "p";
    
    [Required]
    public string JoinColumn { get; set; } = string.Empty;
    
    [Required]
    public List<string> SelectColumns { get; set; } = new();
    
    public string? WhereClause { get; set; }
    
    public string? OrderBy { get; set; }
    
    public int? Limit { get; set; }
    
    public int PageSize { get; set; } = 1000;
    
    public JoinType JoinType { get; set; } = JoinType.Inner;
}

public enum JoinType
{
    Inner,
    Left,
    Right,
    Full
}