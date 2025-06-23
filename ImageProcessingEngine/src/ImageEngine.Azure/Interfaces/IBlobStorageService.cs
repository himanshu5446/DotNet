namespace ImageEngine.Azure.Interfaces
{
    public interface IBlobStorageService
    {
        /// <summary>
        /// Uploads a file to Azure Blob Storage.
        /// </summary>
        /// <param name="filePath">The local path of the file to upload.</param>
        /// <param name="containerName">The name of the container in Azure Blob Storage.</param>
        /// <returns>The URL of the uploaded blob.</returns>
        Task<string> UploadFileAsync(string filePath, string containerName);

        /// <summary>
        /// Downloads a file from Azure Blob Storage.
        /// </summary>
        /// <param name="blobUrl">The URL of the blob to download.</param>
        /// <param name="destinationPath">The local path where the file will be saved.</param>
        Task DownloadFileAsync(string blobUrl, string destinationPath);
    }
}