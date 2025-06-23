namespace ImageEngine.Azure.DependencyInjection
{
    /// <summary>
    /// Represents the settings for Azure Storage.
    /// </summary>
    public class AzureStorageSetting
    {
        /// <summary>
        /// Gets or sets the connection string for Azure Storage.
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the name of the container in Azure Blob Storage.
        /// </summary>
        public string BlobContainerName { get; set; }

        /// <summary>
        /// Gets or sets the name of the queue in Azure Queue Storage.
        /// </summary>
        public string QueueName { get; set; }
    }
    
}