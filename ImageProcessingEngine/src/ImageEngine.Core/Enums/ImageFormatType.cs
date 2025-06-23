namespace ImageProcessor.Core.Enums
{
    public enum ImageFormatType
    {
        Jpeg,
        Png,
        Bmp,
        Gif,
        Tiff,
        Icon,
        Webp,     // Optional: via external libs (e.g., ImageSharp)
        Heif,     // Optional
        Avif,     // Optional
        Emf,      // Vector (Windows)
        Wmf       // Vector (Windows)
    }
}
