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

    public static class ImageProcessorExtensions
    {
        /// <summary>
        /// Resizes and crops the image to the specified dimensions.
        /// </summary>
        /// <param name="processor">The image processor instance.</param>
        /// <param name="image">The image to process.</param>
        /// <param name="width">The target width.</param>
        /// <param name="height">The target height.</param>
        /// <returns>The resized and cropped image.</returns>
        public static Image ResizeAndCrop(this IImageProcessor processor, Image image, int width, int height)
        {
            var resizedImage = processor.Resize(image, width, height);
            return processor.Crop(resizedImage, new Rectangle(0, 0, width, height));
        }
        /// <summary>
        /// Applies a series of color adjustments to the image. 
        /// </summary>
        /// <param name="processor">The image processor instance.</param>
        /// <param name="image">The image to process.</param>
        /// <param name="brightness">The brightness adjustment factor (1.0 = no change).</param>
        /// <param name="contrast">The contrast adjustment factor (1.0 = no change).</param>
        /// <param name="saturation">The saturation adjustment factor (1.0 = no change).</param>
        /// <returns>The image after applying brightness, contrast, and saturation adjustments.</returns>
        /// <remarks>
        /// This method allows for dynamic application of color adjustments based on the provided parameters.
        /// It first adjusts the brightness, then contrast, and finally saturation.
        /// </remarks>
        /// <exception cref="ArgumentException">Thrown if any of the adjustment factors are invalid.</exception>
        /// <exception cref="NotSupportedException">Thrown if the processor does not support the required operations.</exception>
        
        public static Image ApplyColorFilter(this IImageProcessor processor, Image image, float brightness, float contrast, float saturation)
        {
            var adjustedImage = processor.AdjustBrightness(image, brightness);
            adjustedImage = processor.AdjustContrast(adjustedImage, contrast);
            return processor.AdjustSaturation(adjustedImage, saturation);
        }
        /// <summary>
        /// Applies a watermark to the image using either text or an image.
        /// </summary>
        /// <param name="processor">The image processor instance.</param>
        /// <param name="image">The image to process.</param>
        /// <param name="text">The text to use as a watermark.</param>
        /// <param name="font">The font to use for the text watermark.</param>
        /// <param name="color">The color of the text watermark.</param>    
        /// <param name="position">The position where the watermark will be placed.</param>
        /// <param name="opacity">The opacity of the watermark (default is 1.0f, fully opaque).</param>
        /// <returns>The image with the text watermark applied.</returns>
        /// <remarks>
        /// This method allows for dynamic application of text watermarks based on the provided parameters.
        /// It supports specifying the text, font, color, position, and opacity of the watermark.
        /// </remarks>
        /// <exception cref="ArgumentException">Thrown if the text is null or empty.</exception>
        /// <exception cref="NotSupportedException">Thrown if the processor does not support text watermarking.</exception>
        public static Image ApplyWatermark(this IImageProcessor processor, Image image, string text, Font font, Color color, Point position, float opacity = 1.0f)
        {
            var watermarkedImage = processor.AddTextWatermark(image, text, font, color, position, opacity);
            return watermarkedImage;
        }   

        public static Image ApplyWatermark(this IImageProcessor processor, Image image, Image watermarkImage, Point position, float opacity = 1.0f, float scale = 1.0f)
        {
            var scaledWatermark = new Bitmap(watermarkImage, new Size((int)(watermarkImage.Width * scale), (int)(watermarkImage.Height * scale)));
            return processor.AddImageWatermark(image, scaledWatermark, position, opacity);
        }
        public static Image ApplyFilter(this IImageProcessor processor, Image image, string filterName, params object[] parameters)
        {
            return filterName switch
            {
                "Blur" => processor.ApplyBlur(image, (int)parameters[0]),
                "Sharpen" => processor.ApplySharpen(image),
                "EdgeDetection" => processor.ApplyEdgeDetection(image),
                "Emboss" => processor.ApplyEmboss(image),
                "Vignette" => processor.ApplyVignette(image, (float)parameters[0]),
                _ => throw new NotSupportedException($"Filter '{filterName}' is not supported.")
            };
        }
        public static Image ApplyComposite(this IImageProcessor processor, Image image, string compositeName, params object[] parameters)
        {
            return compositeName switch
            {
                "Thumbnail" => processor.GenerateThumbnail(image, (int)parameters[0], (int)parameters[1], (bool)parameters[2]),
                "ResizeAndConvert" => processor.ResizeAndConvert(image, (int)parameters[0], (int)parameters[1], (ImageFormat)parameters[2]),
                _ => throw new NotSupportedException($"Composite operation '{compositeName}' is not supported.")
            };
        }
        public static Image ApplyFormatConversion(this IImageProcessor processor, Image image, string formatName)
        {
            return formatName switch
            {
                "JPEG" => processor.ConvertFormat(image, ImageFormat.Jpeg),     

                "PNG" => processor.ConvertFormat(image, ImageFormat.Png),
                "BMP" => processor.ConvertFormat(image, ImageFormat.Bmp),
                "GIF" => processor.ConvertFormat(image, ImageFormat.Gif),
                "TIFF" => processor.ConvertFormat(image, ImageFormat.Tiff),
                _ => throw new NotSupportedException($"Format '{formatName}' is not supported.")
            };  
        }
        public static Image ApplyBasicTransformation(this IImageProcessor processor, Image image, string transformationName, params object[] parameters)
        {
            return transformationName switch
            {
                "Resize" => processor.Resize(image, (int)parameters[0], (int)parameters[1]),
                "Crop" => processor.Crop(image, (Rectangle)parameters[0]),
                "Rotate" => processor.Rotate(image, (float)parameters[0]),
                "FlipHorizontal" => processor.FlipHorizontal(image),
                "FlipVertical" => processor.FlipVertical(image),
                _ => throw new NotSupportedException($"Transformation '{transformationName}' is not supported.")
            };
        }
        public static Image ApplyColorAdjustment(this IImageProcessor processor, Image image, string adjustmentName, params object[] parameters)
        {
            return adjustmentName switch
            {
                "Brightness" => processor.AdjustBrightness(image, (float)parameters[0]),
                "Contrast" => processor.AdjustContrast(image, (float)parameters[0]),
                "Saturation" => processor.AdjustSaturation(image, (float)parameters[0]),
                "InvertColors" => processor.InvertColors(image),
                "Grayscale" => processor.ApplyGrayscale(image),
                "Sepia" => processor.ApplySepia(image),
                "BlackAndWhite" => processor.ApplyBlackAndWhite(image, (byte)parameters[0]),
                _ => throw new NotSupportedException($"Color adjustment '{adjustmentName}' is not supported.")
            };
        }
        public static Image ApplyFilterEffect(this IImageProcessor processor, Image image, string effectName
        , params object[] parameters)
        {
            return effectName switch
            {
                "Blur" => processor.ApplyBlur(image, (int)parameters[0]),
                "Sharpen" => processor.ApplySharpen(image),
                "EdgeDetection" => processor.ApplyEdgeDetection(image),
                "Emboss" => processor.ApplyEmboss(image),
                "Vignette" => processor.ApplyVignette(image, (float)parameters[0]),
                _ => throw new NotSupportedException($"Effect '{effectName}' is not supported.")
            };
        }
        public static Image ApplyBorder(this IImageProcessor processor, Image image, int thickness, Color color)
        {
            return processor.AddBorder(image, thickness, color);
        }
        public static Image ApplyShadow(this IImageProcessor processor, Image image, int offset, Color shadowColor)
        {
            return processor.AddShadow(image, offset, shadowColor);
        }
        public static Image ApplyWatermark(this IImageProcessor processor, Image image, string text, Font font, Color color, Point position, float opacity = 1.0f)
        {
            return processor.AddTextWatermark(image, text, font, color, position, opacity);
        }
        public static Image ApplyWatermark(this IImageProcessor processor, Image image, Image watermarkImage, Point position, float opacity = 1.0f, float scale = 1.0f)
        {
            var scaledWatermark = new Bitmap(watermarkImage, new Size((int)(watermarkImage.Width * scale), (int)(watermarkImage.Height * scale)));
            return processor.AddImageWatermark(image, scaledWatermark, position, opacity);
        }
        public static Image ConvertImageFormat(this IImageProcessor processor, Image image, string formatName       
        )
        {
            return formatName switch
            {
                "JPEG" => processor.ConvertFormat(image, ImageFormat.Jpeg),
                "PNG" => processor.ConvertFormat(image, ImageFormat.Png),
                "BMP" => processor.ConvertFormat(image, ImageFormat.Bmp),
                "GIF" => processor.ConvertFormat(image, ImageFormat.Gif),
                "TIFF" => processor.ConvertFormat(image, ImageFormat.Tiff),
                _ => throw new NotSupportedException($"Format '{formatName}' is not supported.")
            };
        }
        public static Image GenerateThumbnail(this IImageProcessor processor, Image image, int maxWidth, int maxHeight, bool preserveAspectRatio = true)
        {
            if (preserveAspectRatio)
            {
                float aspectRatio = (float)image.Width / image.Height;
                if (image.Width > image.Height)
                {
                    maxHeight = (int)(maxWidth / aspectRatio);  
                }
                else
                {
                    maxWidth = (int)(maxHeight * aspectRatio);  
                }
            }
            return processor.GenerateThumbnail(image, maxWidth, maxHeight, preserveAspectRatio);        
        }
        public static Image ResizeAndConvert(this IImageProcessor processor, Image image, int width, int height, string formatName)
        {
            var format = formatName switch
            {
                "JPEG" => ImageFormat.Jpeg,
                "PNG" => ImageFormat.Png,
                "BMP" => ImageFormat.Bmp,
                "GIF" => ImageFormat.Gif,
                "TIFF" => ImageFormat.Tiff,
                _ => throw new NotSupportedException($"Format '{formatName}' is not supported.")
            };
            return processor.ResizeAndConvert(image, width, height, format);
        }
        /// <summary>
        /// Applies a composite operation to the image based on the provided composite name and parameters.
        /// </summary>
        /// <param name="processor">The image processor instance.</param>
        /// <param name="image">The image to process.</param>
        /// <param name="compositeName">The name of the composite operation to apply.</param>
        /// <param name="parameters">The parameters required for the composite operation.</param>
        /// <returns>The processed image after applying the composite operation.</returns>
        /// <exception cref="NotSupportedException">Thrown if the composite operation is not supported.</exception>
        /// <remarks>
        /// This method allows for dynamic application of composite operations based on the operation name.
        /// Supported operations include "Thumbnail" and "ResizeAndConvert".
        /// </remarks>
        public static Image ApplyCompositeOperation(this IImageProcessor processor, Image image, string compositeName
        , params object[] parameters)
        {
            return compositeName switch
            {
                "Thumbnail" => processor.GenerateThumbnail(image, (int)parameters[0], (int)parameters[1], (bool)parameters[2]),
                "ResizeAndConvert" => processor.ResizeAndConvert(image, (int)parameters[0], (int)parameters[1], (ImageFormat)parameters[2]),
                _ => throw new NotSupportedException($"Composite operation '{compositeName}' is not supported.")
            };  
        }
    }
}
