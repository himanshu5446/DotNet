namespace ImageProcessor.Utils;
{
    public interface IImageLoader
    {
        /// <summary>
        /// Loads images from the specified directory.
        /// </summary>
        /// <param name="filePath">The directory path containing the images.</param>
        /// <returns>A list of tuples containing the file path and the loaded image.</returns>
        public static List<(string filepath, Image image)> LoadImage(string filePath);
        /// <summary>
        /// Gets the file paths of images in the specified folder with optional extensions.
        /// </summary>
        /// <param name="folderPath">The folder path to search for images.</param>
        /// <param name="extensions">Optional list of file extensions to filter by. If null
        /// is provided, all image files will be returned.</param>
        /// <returns>An enumerable of file paths that match the specified criteria.</returns>
        /// <exception cref="DirectoryNotFoundException">Thrown when the specified directory does not exist
        /// <exception cref="ArgumentException">Thrown when the folder path is null or empty.</exception>
        /// <exception cref="Exception">Thrown when an error occurs while retrieving file paths.</exception>
        /// <remarks>
        /// This method searches for image files in the specified folder and returns their paths.
        /// If no extensions are provided, it defaults to common image formats like .jpg, .jpeg, .png, .bmp, and .gif.
        /// </remarks>
        public static IEnumerable<string> GetImageFilePaths(string folderPath, IEnumerable<string> extensions = null);

    }
    publi class ImageLoader : IImageLoader{
        /// Loads an image from the specified file path.
        private static readonly string[] SupportedFormats = { ".jpg", ".jpeg", ".png", ".bmp", ".gif" };
       
       /// <summary>
       /// Loads images from the specified directory.
       /// </summary>
       /// <param name="filePath">The directory path containing the images.</param>             
       /// <returns>A list of tuples containing the file path and the loaded image.</returns>
       /// <exception cref="DirectoryNotFoundException">Thrown when the specified directory does not exist
         /// <exception cref="Exception">Thrown when an error occurs while loading an image.</exception>     
        public static List<(string filepath, Image image)> LoadImage(string filePath){
            if(!Directory.Exists(filePath)){
                throw new DirectoryNotFoundException($"The file '{filePath}' does not exist.");
            }

            var image = new List<(string,Image)>();

            foreach (var item in Directory.GetFiles(filePath))
            {
                var fileExtension = Path.GetExtension(filePath).ToLowerInvariant();
                if (Array.Exists(SupportedFormats, ext => ext.Equals(fileExtension)))
                {
                    try
                    {
                        var img = Image.FromFile(item);
                        image.Add((item, img));
                    }
                    catch (Exception ex)
                    {
                        throw new Exception($"An error occurred while loading the image '{item}': {ex.Message}", ex);
                    }
                }

            }
            return image;
    }
}