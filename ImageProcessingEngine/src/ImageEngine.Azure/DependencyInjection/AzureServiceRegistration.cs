namespace ImageEngine.Azure.DependencyInjection;
public static class AzureServiceRegistration
{
    /// <summary>
    /// Registers Azure services in the dependency injection container.
    /// </summary>
    /// <param name="services">The service collection to register the services with.</param>
    /// <returns>The updated service collection.</returns>
    public static IServiceCollection AddAzureServices(this IServiceCollection services)
    {
        // Register Azure Blob Storage service
        services.AddScoped<IBlobStorageService, BlobStorageService>();

        // Register Azure Queue Storage service
        services.AddScoped<IQueueStorageService, QueueStorageService>();

        // Register telemetry service
        services.AddScoped<ITelemetryService, TelemetryService>();

        return services;
    }
}