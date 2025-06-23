using System.Drawing;
using System.Drawing.Imaging;

namespace ImageProcessor.Core.Interfaces
{
    public interface IImageProcessor
    {
        // --- Basic Transformations ---
        Image Resize(Image image, int width, int height);
        Image Crop(Image image, Rectangle area);
        Image Rotate(Image image, float angle);
        Image FlipHorizontal(Image image);
        Image FlipVertical(Image image);

        // --- Color & Tone Adjustments ---
        Image AdjustBrightness(Image image, float brightnessFactor);   // 1.0 = no change
        Image AdjustContrast(Image image, float contrastFactor);       // 1.0 = no change
        Image AdjustSaturation(Image image, float saturationFactor);   // 1.0 = no change
        Image InvertColors(Image image);
        Image ApplyGrayscale(Image image);
        Image ApplySepia(Image image);
        Image ApplyBlackAndWhite(Image image, byte threshold = 128);

        // --- Filters & Effects ---
        Image ApplyBlur(Image image, int intensity = 3);         // Gaussian blur
        Image ApplySharpen(Image image);
        Image ApplyEdgeDetection(Image image);                   // Sobel, Laplacian
        Image ApplyEmboss(Image image);
        Image ApplyVignette(Image image, float radius = 0.5f);
        Image AddBorder(Image image, int thickness, Color color);
        Image AddShadow(Image image, int offset, Color shadowColor);

        // --- Watermarking ---
        Image AddTextWatermark(Image image, string text, Font font, Color color, Point position, float opacity = 1.0f);
        Image AddImageWatermark(Image image, Image watermarkImage, Point position, float opacity = 1.0f, float scale = 1.0f);

        // --- Format Conversion ---
        Image ConvertFormat(Image image, ImageFormat format);

        // --- Advanced / Composite ---
        Image GenerateThumbnail(Image image, int maxWidth, int maxHeight, bool preserveAspectRatio = true);
        Image ResizeAndConvert(Image image, int width, int height, ImageFormat format);
    }
}
