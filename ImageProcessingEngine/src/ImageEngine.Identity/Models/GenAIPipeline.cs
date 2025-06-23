namespace Namespace ImageEngine.Identity.Models
{
    public class GenAIPipeline
    {
        public Guid Id { get; set; }
        public Guid JobId { get; set; }
        public ImageJob Job { get; set; } = null!;

        public string Prompt { get; set; } = string.Empty;
        public string ConfigJson { get; set; } = string.Empty;
        public string ModelUsed { get; set; } = "GPT-4";
    }
}