using System.Collections.Generic;
using System.Drawing;

namespace ImageProcessor.Core.Interfaces
{
    public interface IMetadataReader
    {
        /// <summary>
        /// Extracts all available EXIF metadata from the image.
        /// </summary>
        IDictionary<string, string> GetExifMetadata(Image image);

        /// <summary>
        /// Returns image dimensions (Width, Height).
        /// </summary>
        (int Width, int Height) GetDimensions(Image image);

        /// <summary>
        /// Returns the DPI of the image (Horizontal, Vertical).
        /// </summary>
        (float DpiX, float DpiY) GetDpi(Image image);

        /// <summary>
        /// Returns the raw image format (e.g. Jpeg, Png).
        /// </summary>
        string GetFormat(Image image);

        /// <summary>
        /// Returns file size in bytes from file path.
        /// </summary>
        long GetFileSize(string filePath);

        /// <summary>
        /// Returns image orientation if available (e.g. 1 = normal, 3 = 180°, 6 = 90°).
        /// </summary>
        int? GetOrientation(Image image);

        /// <summary>
        /// Read all metadata including dimensions, dpi, format, etc.
        /// </summary>
        ImageMetadata ReadMetadata(Image image, string filePath = null);
    }
}
