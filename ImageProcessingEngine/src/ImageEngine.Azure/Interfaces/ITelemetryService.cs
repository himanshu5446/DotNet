namespace ImageEngine.Azure.Interfaces
{
    /// <summary>
    /// Interface for telemetry services to track application performance and usage.
    /// </summary>
    public interface ITelemetryService
    {
        /// <summary>
        /// Tracks an event with optional properties.
        /// </summary>
        /// <param name="eventName">The name of the event to track.</param>
        /// <param name="properties">Optional properties to include with the event.</param>
        void TrackEvent(string eventName, IDictionary<string, string> properties = null);

        /// <summary>
        /// Tracks an exception with optional properties.
        /// </summary>
        /// <param name="exception">The exception to track.</param>
        /// <param name="properties">Optional properties to include with the exception.</param>
        void TrackException(Exception exception, IDictionary<string, string> properties = null);
    }
}