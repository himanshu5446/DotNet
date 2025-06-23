namespace ImageEngine.AI.Models
{
    public class ImageEmbedding
    {
        public Guid Id { get; set; }
        public Guid ImageId { get; set; }
        public string EmbeddingData { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

        // Navigation properties can be added if needed
        // public Image Image { get; set; } = null!;
    }
}