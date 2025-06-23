namespace ImageEngine.Identity.Models
{
    public class Organization
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public Guid OwnerUserId { get; set; }
        public string Tier { get; set; } = "Free";
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

        public ICollection<User> Users { get; set; } = new List<User>();
        public ICollection<ImageWorkspace> Workspaces { get; set; } = new List<ImageWorkspace>();
    }
}