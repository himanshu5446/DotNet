namespace ImageEngine.AI.Models
{
    public class InferenceLog
    {
        public Guid Id { get; set; }
        public Guid JobId { get; set; }
        public string Prompt { get; set; } = string.Empty;
        public string Response { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

        // Navigation properties can be added if needed
        // public ImageJob Job { get; set; } = null!;
    }
}