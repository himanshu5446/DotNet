using System.Drawing;
using System.Drawing.Imaging;

namespace ImageProcessor.Core.Interfaces
{
    public interface IFormatConverter
    {
        /// <summary>
        /// Converts the given image to the specified image format (e.g., JPEG, PNG, BMP).
        /// </summary>
        Image Convert(Image sourceImage, ImageFormat targetFormat);

        /// <summary>
        /// Converts and saves the image to the target format directly to disk.
        /// </summary>
        void ConvertAndSave(Image sourceImage, string outputPath, ImageFormat targetFormat, long quality = 100L);

        /// <summary>
        /// Gets ImageFormat object from format string (e.g., "jpeg", "png", "webp").
        /// </summary>
        ImageFormat GetImageFormat(string formatString);

        /// <summary>
        /// Gets default extension (e.g., ".jpg") from ImageFormat.
        /// </summary>
        string GetExtensionFromFormat(ImageFormat format);
    }
}
