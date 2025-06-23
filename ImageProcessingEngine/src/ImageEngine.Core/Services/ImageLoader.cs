namespace ImageEngine.Core.Services
{
    using System.Drawing;
    using System.IO;
    using System.Threading.Tasks;
    using ImageProcessor.Core.Enums;
    using ImageProcessor.Core.Interfaces;
    using ImageProcessor.Core.Models;

    public class ImageLoader : IImageLoader
    {
        public async Task<Image> LoadImageAsync(string path)
        {
            return await Task.Run(() => Image.FromFile(path));
        }

        public async Task SaveImageAsync(Image image, string path, ImageFormatType format)
        {
            await Task.Run(() =>
            {
                var imageFormat = format switch
                {
                    ImageFormatType.Jpeg => System.Drawing.Imaging.ImageFormat.Jpeg,
                    ImageFormatType.Png => System.Drawing.Imaging.ImageFormat.Png,
                    _ => System.Drawing.Imaging.ImageFormat.Bmp
                };
                image.Save(path, imageFormat);
            });
        }

        public async Task<ImageMetadata> GetImageMetadataAsync(string path)
        {
            return await Task.Run(() =>
            {
                var fileInfo = new FileInfo(path);
                return new ImageMetadata
                {
                    FileName = fileInfo.Name,
                    FileSize = fileInfo.Length,
                    CreationDate = fileInfo.CreationTime,
                    LastModifiedDate = fileInfo.LastWriteTime
                };
            });
        }
    }
}