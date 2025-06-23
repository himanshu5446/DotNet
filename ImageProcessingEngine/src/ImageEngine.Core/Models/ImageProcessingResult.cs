namespace ImageProcessor.Core.Models;
public class ImageProcessingResult
{
    public string OriginalPath { get; set; }
    public string OutputPath { get; set; }
    public TimeSpan Duration { get; set; }
    public bool Success { get; set; }
    public string ErrorMessage { get; set; }
}
