namespace ImageEngine.Identity.Models
{
    public class ImageWorkspace
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public Guid UserId { get; set; }
        public User User { get; set; } = null!;
        public Guid OrganizationId { get; set; }
        public Organization Organization { get; set; } = null!;
        public bool IsShared { get; set; } = false;
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

        public ICollection<ImageAsset> Images { get; set; } = new List<ImageAsset>();
    }
}