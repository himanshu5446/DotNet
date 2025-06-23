namespace ImageEngine.Core.Utils
{
    using System;
    using System.IO;

    public static class FileHelper
    {
        /// <summary>
        /// Checks if a file exists at the specified path.
        /// </summary>
        /// <param name="path">The file path to check.</param>
        /// <returns>True if the file exists, otherwise false.</returns>
        /// <exception cref="ArgumentException">Thrown when the path is null or empty.</exception>
        /// <exception cref="Exception">Thrown when an unexpected error occurs while checking the file existence.</exception>
        /// <remarks>                                       
        /// This method is a utility function that checks for the existence of a file at the specified path.
        /// It is designed to handle various exceptions that may occur during the file existence check, providing
        /// a robust way to verify file presence in a file system.
        /// </remarks>
        public static bool FileExists(string path)
        {
            if (string.IsNullOrEmpty(path)) throw new ArgumentException("Path cannot be null or empty.", nameof(path));
            return File.Exists(path);
        }
        /// <summary>  
        /// Ensures that the directory for the specified file path exists.
        /// </summary>
        /// <param name="path">The file path for which the directory should be ensured.</param>
        /// <exception cref="ArgumentException">Thrown when the path is null or empty.</exception>
        /// <remarks>
        /// This method checks if the directory for the specified file path exists, and if not,
        /// it creates the directory. This is useful for ensuring that a directory is available before
        /// attempting to write a file to that location.
        /// It is designed to handle various exceptions that may occur during the directory creation process,
        /// providing a robust way to ensure the directory structure is in place.
        /// </remarks>   
        public static void EnsureDirectoryExists(string path)
        {
            if (string.IsNullOrEmpty(path)) throw new ArgumentException("Path cannot be null or empty.", nameof(path));
            var directory = Path.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }
        }
        /// <summary>
        /// Gets the file name from a specified path.
        /// </summary>  
        ///     /// <param name="path">The file path from which to extract the file name.</param>
        /// <returns>The file name including the extension.</returns>
        /// <exception cref="ArgumentException">Thrown when the path is null or empty.</exception>
        /// <remarks>
        ///     This method extracts the file name from a given file path.
        ///    It is useful for obtaining the name of a file without needing to manipulate the path
        ///    directly. The method returns the file name including its extension.
        /// </remarks>
        
        public static string GetFileNameWithoutExtension(string path)
        {
            if (string.IsNullOrEmpty(path)) throw new ArgumentException("Path cannot be null or empty.", nameof(path));
            return Path.GetFileNameWithoutExtension(path);
        }

        /// <summary>
        /// Gets the file extension from a specified path.
        /// </summary>
        /// <param name="path">The file path from which to extract the file extension.</param>
        /// <returns>The file extension including the leading dot (e.g., ".jpg").
        /// <exception cref="ArgumentException">Thrown when the path is null or empty.</exception>
        /// <remarks>
        /// This method extracts the file extension from a given file path.
        /// It is useful for determining the type of file based on its extension.
        /// The method returns the file extension including the leading dot.
        /// </remarks>
        public static string GetFileExtension(string path)
        {
            if (string.IsNullOrEmpty(path)) throw new ArgumentException("Path cannot be null or empty.", nameof(path));
            return Path.GetExtension(path);
        }
    }
}