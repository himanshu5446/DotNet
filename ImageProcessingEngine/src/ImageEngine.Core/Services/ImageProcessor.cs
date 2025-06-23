namespace ImageEngine.Core.Services{

    using System;
    using System.Drawing;
    using System.Threading.Tasks;
    using ImageProcessor.Core.Interfaces;

    public class ImageProcessor : IImageProcessor
    {
        private readonly IImageLoader _imageLoader;

        public ImageProcessor(IImageLoader imageLoader)
        {
            _imageLoader = imageLoader ?? throw new ArgumentNullException(nameof(imageLoader));
        }

        public async Task<Image> ProcessImageAsync(string path, Func<Image, Image> processingFunction)
        {
            if (string.IsNullOrEmpty(path)) throw new ArgumentException("Path cannot be null or empty.", nameof(path));
            if (processingFunction == null) throw new ArgumentNullException(nameof(processingFunction));

            var image = await _imageLoader.LoadImageAsync(path);
            return processingFunction(image);
        }
        
        public async Task SaveProcessedImageAsync(Image image, string path, ImageFormatType format)
        {
            if (image == null) throw new ArgumentNullException(nameof(image));
            if (string.IsNullOrEmpty(path)) throw new ArgumentException("Path cannot be null or empty.", nameof(path));

            await _imageLoader.SaveImageAsync(image, path, format);
        }
    }
}