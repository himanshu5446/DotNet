namespace ImageEngine.Core.Utils{


    using System;
    using System.IO;

    public static class Logger
    {
        /// <summary>
        /// Logs a message to a specified log file.
        /// </summary>
        /// <param name="message">The message to log.</param>
        /// <param name="logFilePath">The path to the log file.</param>
        /// <exception cref="ArgumentException">Thrown when the log file path is null or empty.</exception>
        public static void Log(string message, string logFilePath)
        {
            if (string.IsNullOrEmpty(logFilePath)) throw new ArgumentException("Log file path cannot be null or empty.", nameof(logFilePath));
            File.AppendAllText(logFilePath, $"{DateTime.Now}: {message}{Environment.NewLine}");
        }
    }
}