namespace  ImageEngine.Identity.Models
{
    using System;
    using System.Collections.Generic;

    public class ImageAsset
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string FilePath { get; set; } = string.Empty;
        public long SizeInBytes { get; set; }
        public string MimeType { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

        public Guid WorkspaceId { get; set; }
        public ImageWorkspace Workspace { get; set; } = null!;

        public ICollection<ImageJob> ImageJobs { get; set; } = new List<ImageJob>();
    }
}