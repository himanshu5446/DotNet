namespace ImageEngine.AI.Models
{
    using System;

    public class UserFeedback
    {
        public Guid Id { get; set; }
        public Guid UserId { get; set; }
        public string FeedbackText { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

        // Navigation properties can be added if needed
        // public User User { get; set; } = null!;
    }
}