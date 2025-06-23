namespace  ImageEngine.Infrastructure.Repositories
{
    using System;
    using System.Threading.Tasks;
    using ImageEngine.Domain.Repositories;
    using ImageEngine.Infrastructure.Contexts;

    public class UnitOfWork : IUnitOfWork
    {
        private readonly ImageEngineDbContext _context;

        public UnitOfWork(ImageEngineDbContext context)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
        }

        public async Task SaveChangesAsync()
        {
            await _context.SaveChangesAsync();
        }

        public void Dispose()
        {
            _context.Dispose();
        }
    }
}