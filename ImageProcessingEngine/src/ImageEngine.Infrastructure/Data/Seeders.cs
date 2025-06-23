namespace  ImageEngine.Infrastructure.Data
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using ImageEngine.Domain.Entities;
    using Microsoft.EntityFrameworkCore;

    public static class Seeders
    {
        public static async Task SeedAsync(DbContext context)
        {
            if (context == null) throw new ArgumentNullException(nameof(context));

            if (!await context.Set<ImageEntity>().AnyAsync())
            {
                var images = new List<ImageEntity>
                {
                    new ImageEntity { Id = Guid.NewGuid(), Name = "SampleImage1", Url = "http://example.com/image1.jpg" },
                    new ImageEntity { Id = Guid.NewGuid(), Name = "SampleImage2", Url = "http://example.com/image2.jpg" }
                };

                await context.Set<ImageEntity>().AddRangeAsync(images);
                await context.SaveChangesAsync();
            }
        }
    }
    
}