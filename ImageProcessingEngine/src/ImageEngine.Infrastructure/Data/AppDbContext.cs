namespace ImageEngine.Infrastructure.Data
{
    using System;
    using System.Threading.Tasks;
    using ImageEngine.Domain.Entities;
    using ImageEngine.Domain.Repositories;
    using Microsoft.EntityFrameworkCore;

    public class AppDbContext : DbContext, IUnitOfWork
    {
        public AppDbContext(DbContextOptions<AppDbContext> options) : base(options)
        {
        }

        public DbSet<ImageEntity> Images { get; set; }

        public async Task SaveChangesAsync()
        {
            await base.SaveChangesAsync();
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ImageEntity>().ToTable("Images");
            base.OnModelCreating(modelBuilder);
        }
    }
    
}