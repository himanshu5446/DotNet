namespace Namespace ImageEngine.Identity.Models
{
    public class ImageJob
    {
        public Guid Id { get; set; }
        public Guid ImageId { get; set; }
        public ImageAsset Image { get; set; } = null!;
        public Guid UserId { get; set; }
        public User User { get; set; } = null!;

        public string Type { get; set; } = string.Empty;
        public string Status { get; set; } = "Pending";
        public DateTime StartedAt { get; set; } = DateTime.UtcNow;
        public DateTime? CompletedAt { get; set; }

        public GenAIPipeline? Pipeline { get; set; }
    }
}