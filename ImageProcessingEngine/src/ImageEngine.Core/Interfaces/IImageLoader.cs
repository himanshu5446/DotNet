namespace ImageProcessor.Core.Interfaces
{
    using System.Drawing;
    using System.Threading.Tasks;
    using ImageProcessor.Core.Enums;
    using ImageProcessor.Core.Models;

    public interface IImageLoader
    {
        /// <summary>
        /// Loads an image from the specified path.
        /// </summary>
        Task<Image> LoadImageAsync(string path);

        /// <summary>
        /// Saves an image to the specified path with the given format.
        /// </summary>
        Task SaveImageAsync(Image image, string path, ImageFormatType format);

        /// <summary>
        /// Gets metadata for the specified image.
        /// </summary>
        Task<ImageMetadata> GetImageMetadataAsync(string path);
    }
}