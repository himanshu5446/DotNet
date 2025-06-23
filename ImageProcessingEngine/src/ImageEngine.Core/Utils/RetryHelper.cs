namespace ImageEngine.Core.Utils
{
    using System;
    using System.Threading.Tasks;

    public static class RetryHelper
    {
        /// <summary>
        /// Executes a function with retry logic.
        /// </summary>
        /// <param name="action">The action to execute.</param>
        /// <param name="maxRetries">The maximum number of retries.</param>
        /// <param name="delay">The delay between retries in milliseconds.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public static async Task ExecuteWithRetryAsync(Func<Task> action, int maxRetries = 3, int delay = 1000)
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            if (maxRetries <= 0) throw new ArgumentOutOfRangeException(nameof(maxRetries), "Max retries must be greater than zero.");
            if (delay < 0) throw new ArgumentOutOfRangeException(nameof(delay), "Delay cannot be negative.");

            for (int i = 0; i < maxRetries; i++)
            {
                try
                {
                    await action();
                    return; // Exit if successful
                }
                catch
                {
                    if (i == maxRetries - 1) throw; // Rethrow on last attempt
                    await Task.Delay(delay); // Wait before retrying
                }
            }
        }
    }
}