namespace ImageEngine.Core.Services
{
    using System.Drawing;
    using System.IO;
    using System.Threading.Tasks;
    using ImageEngine.Core.Enums;
    using ImageEngine.Core.Interfaces;
    using ImageEngine.Core.Models;

    public class ParallelImageProcessor : IParallelImageProcessor
    {                                    
        /// <summary>
        /// Processes a collection of image paths in parallel.
        /// </summary>
        /// <param name="imagePaths">The collection of image paths to process.</param>
        /// <param name="loader">A function to load an image from a given path.</param>
        /// <param name="processor">A function to process the loaded image.</param>
        /// <param name="logger">An optional function to log messages during processing.</param>
        /// <exception cref="ArgumentNullException">Thrown when any of the parameters are null.</exception>
        /// <exception cref="ArgumentException">Thrown when the imagePaths collection is empty.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the loader or processor functions are null.</exception>
        /// <remarks>
        /// This method processes each image path in parallel, loading the image and then applying the processor function.
        /// If a logger function is provided, it will log messages for each processed image or any errors encountered.
        /// </remarks>
        public void ProcessWithParallelForEach(
            IEnumerable<string> imagePaths,
            Func<string, Image> loader,
            Action<string, Image> processor,
            Action<string> logger = null)
            {
                Parallel.ForEach(imagePaths, path =>
                {
                    try
                    {
                        var image = loader(path);
                        processor(path, image);
                        logger?.Invoke($"Processed {path}");
                    }
                    catch (Exception ex)
                    {
                        logger?.Invoke($"Error processing {path}: {ex.Message}");
                    }
                });
            } 
        /// <summary>
        /// Processes a collection of image paths asynchronously using parallel execution.
        /// </summary>
        /// <param name="imagePaths">The collection of image paths to process.</param>
        /// <param name="loader">A function to load an image from a given path.</param>
        /// <param name="processor">A function to process the loaded image.</param>
        /// <param name="logger">An optional function to log messages during processing.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when any of the parameters are null.</exception>
        /// <exception cref="ArgumentException">Thrown when the imagePaths collection is empty.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the loader or processor functions are null.</exception>
        /// <remarks>
        /// This method processes each image path in parallel, loading the image and then applying the processor function.
        /// If a logger function is provided, it will log messages for each processed image or any errors encountered.
        /// </remarks>
        public async Task ProcessWithParallelForEachAsync(
            IEnumerable<string> imagePaths,
            Func<string, Task<Image>> loader,
            Func<string, Image, Task> processor,
            Func<string, Task> logger = null)
        {
            var tasks = imagePaths.Select(async path =>
            {
                try
                {
                    var image = await loader(path);
                    await processor(path, image);
                    if (logger != null)
                    {
                        await logger($"Processed {path}");
                    }
                }
                catch (Exception ex)
                {
                    if (logger != null)
                    {
                        await logger($"Error processing {path}: {ex.Message}");
                    }
                }
            });

            await Task.WhenAll(tasks);
        }  
    }
}