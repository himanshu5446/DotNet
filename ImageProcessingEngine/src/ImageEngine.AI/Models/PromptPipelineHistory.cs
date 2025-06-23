namespace ImageEngine.AI.Models
{
    using System;
    using System.Collections.Generic;

    public class PipelineVersionHistory
    {
        public Guid Id { get; set; }
        public Guid PipelineId { get; set; }
        public string Version { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

        public ICollection<PipelineStep> Steps { get; set; } = new List<PipelineStep>();
    }

    public class PipelineStep
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
        public int Order { get; set; }
    }
}