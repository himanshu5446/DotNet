using System.Drawing;

namespace ImageProcessor.Core.Interfaces
{
    public interface IWatermarkService
    {
        /// <summary>
        /// Adds a text watermark to the image at the specified position.
        /// </summary>
        Image AddTextWatermark(Image image, string text, Font font, Color color, Point position, float opacity = 1.0f);

        /// <summary>
        /// Adds a logo/image watermark to the image at the specified position and scale.
        /// </summary>
        Image AddImageWatermark(Image baseImage, Image watermarkImage, Point position, float opacity = 1.0f, float scale = 1.0f);

        /// <summary>
        /// Adds a dynamic text watermark (e.g., filename, timestamp, metadata).
        /// </summary>
        Image AddDynamicTextWatermark(Image image, string template, Font font, Color color, Point position, object context, float opacity = 1.0f);
    }
}
