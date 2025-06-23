namespace ImageEngine.Azure.Interfaces
{
    public interface IQueueStorageService
    {
        /// <summary>
        /// Sends a message to the Azure Queue Storage.
        /// </summary>
        /// <param name="queueName">The name of the queue.</param>
        /// <param name="message">The message to send.</param>
        Task SendMessageAsync(string queueName, string message);

        /// <summary>
        /// Receives a message from the Azure Queue Storage.
        /// </summary>
        /// <param name="queueName">The name of the queue.</param>
        /// <returns>The received message.</returns>
        Task<string> ReceiveMessageAsync(string queueName);

        /// <summary>
        /// Deletes a message from the Azure Queue Storage.
        /// </summary>
        /// <param name="queueName">The name of the queue.</param>
        /// <param name="messageId">The ID of the message to delete.</param>
        Task DeleteMessageAsync(string queueName, string messageId);
    }
}