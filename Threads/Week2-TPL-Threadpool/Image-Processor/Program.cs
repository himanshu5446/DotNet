using ImageProcessor.Utils;

public class Program
{
    static void Main()
    {
        string inputFolder = @"./input-images"; // or load from config

        var images = ImageLoader.LoadImages(inputFolder);

        Console.WriteLine($"Loaded {images.Count} images.");

        foreach (var (filePath, image) in images)
        {
            Console.WriteLine($"✔️ {Path.GetFileName(filePath)} | Size: {image.Width}x{image.Height}");
            // image.Dispose() when done
        }
    }
}
