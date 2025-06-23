using System;
using System.Collections.Generic;
using System.Drawing;
using System.Threading;
using System.Threading.Tasks;

namespace ImageProcessor.Core.Interfaces
{
    public interface IParallelImageProcessor
    {
        /// <summary>
        /// Processes a batch of image files using Parallel.ForEach for CPU-bound scenarios.
        /// </summary>
        void ProcessWithParallelForEach(
            IEnumerable<string> imagePaths,
            Func<string, Image> loader,
            Action<string, Image> processor,
            Action<string> logger = null);

        /// <summary>
        /// Processes a batch of image files asynchronously using Task.Run (ideal for I/O-bound or hybrid workloads).
        /// </summary>
        Task ProcessWithTaskRunAsync(
            IEnumerable<string> imagePaths,
            Func<string, Task<Image>> loader,
            Func<string, Image, Task> processor,
            CancellationToken token = default,
            Action<string> logger = null);

        /// <summary>
        /// Processes a batch of image files using ThreadPool.QueueUserWorkItem (legacy compatibility).
        /// </summary>
        void ProcessWithThreadPool(
            IEnumerable<string> imagePaths,
            Func<string, Image> loader,
            Action<string, Image> processor,
            Action<string> logger = null);

        /// <summary>
        /// Enables custom logging (console, file, UI callback).
        /// </summary>
        void SetLogger(Action<string> logAction);
    }
}
