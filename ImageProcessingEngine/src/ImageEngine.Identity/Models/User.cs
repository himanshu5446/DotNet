namespace Namespace ImageEngine.Identity.Models
{
    public class User
    {
        public Guid Id { get; set; }
        public string Email { get; set; } = string.Empty;
        public string PasswordHash { get; set; } = string.Empty;
        public string FullName { get; set; } = string.Empty;
        public string Role { get; set; } = "Member";
        public Guid OrganizationId { get; set; }
        public Organization Organization { get; set; } = null!;
        public bool IsActive { get; set; } = true;
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

        public ICollection<ImageWorkspace> Workspaces { get; set; } = new List<ImageWorkspace>();
        public ICollection<ImageJob> ImageJobs { get; set; } = new List<ImageJob>();
    }
}