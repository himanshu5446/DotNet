namespace ImageProcessor.Core.Models
{
    public class ImageMetadata
    {
        public int Width { get; set; }
        public int Height { get; set; }
        public float DpiX { get; set; }
        public float DpiY { get; set; }
        public string Format { get; set; }
        public long FileSizeBytes { get; set; }
        public int? Orientation { get; set; }
        public IDictionary<string, string> ExifData { get; set; }
    }
}
