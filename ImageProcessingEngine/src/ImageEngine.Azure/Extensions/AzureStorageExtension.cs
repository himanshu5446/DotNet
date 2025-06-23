namespace ImageEngine.Azure.extensions
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using ImageEngine.Azure.Interfaces;

    /// <summary>
    /// Provides extension methods for Azure services.
    /// </summary>
    public static class AzureStorageExtension
    {
        /// <summary>
        /// Uploads a file to Azure Blob Storage and tracks the event.
        /// </summary>
        /// <param name="blobStorageService">The blob storage service.</param>
        /// <param name="filePath">The local path of the file to upload.</param>
        /// <param name="containerName">The name of the container in Azure Blob Storage.</param>
        /// <param name="telemetryService">The telemetry service to track the event.</param>
        /// <returns>The URL of the uploaded blob.</returns>
        public static async Task<string> UploadFileWithTelemetryAsync(
            this IBlobStorageService blobStorageService,
            string filePath,
            string containerName,
            ITelemetryService telemetryService)
        {
            try
            {
                var blobUrl = await blobStorageService.UploadFileAsync(filePath, containerName);
                telemetryService.TrackEvent("FileUploaded", new Dictionary<string, string>
                {
                    { "FilePath", filePath },
                    { "ContainerName", containerName },
                    { "BlobUrl", blobUrl }
                });
                return blobUrl;
            }
            catch (Exception ex)
            {
                telemetryService.TrackException(ex, new Dictionary<string, string>
                {
                    { "FilePath", filePath },
                    { "ContainerName", containerName }
                });
                throw;
            }
        }
        /// <summary>
        /// Downloads a file from Azure Blob Storage and tracks the event.
        /// </summary>          
        /// <param name="blobStorageService">The blob storage service.</param>
        /// <param name="blobUrl">The URL of the blob to download.</param>
        /// <param name="destinationPath">The local path where the file will be saved.</
        ///     
        /// 
        /// param>
        /// <param name="telemetryService">The telemetry service to track the event.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public static async Task DownloadFileWithTelemetryAsync(
            this IBlobStorageService blobStorageService,
            string blobUrl,     
            string destinationPath,
            ITelemetryService telemetryService)
        {
            try
            {
                await blobStorageService.DownloadFileAsync(blobUrl, destinationPath);
                telemetryService.TrackEvent("FileDownloaded", new Dictionary<string, string>
                {
                    { "BlobUrl", blobUrl },
                    { "DestinationPath", destinationPath }          
                });
            
            }
            catch (Exception ex)
            {
                telemetryService.TrackException(ex, new Dictionary<string, string>
                {
                    { "BlobUrl", blobUrl },
                    { "DestinationPath", destinationPath }
                });
                throw;
            }
    }
}