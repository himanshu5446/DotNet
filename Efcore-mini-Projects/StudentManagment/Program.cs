



    using Microsoft.EntityFrameworkCore;
    using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = Host.CreateDefaultBuilder()
        .ConfigureServices((context, services) =>
        {
            services.AddDbContext<SchoolContext>(options =>
                options.UseNpgsql("Host=localhost;Port=5432;Database=StudentDb;Username=shradhagautam;Password=him544"));
        })
        .Build();

using var scope = host.Services.CreateScope();
var db = scope.ServiceProvider.GetRequiredService<SchoolContext>();
    db.Database.Migrate(); // applies migrations

// Add new student
var student = new Student
{
    Name = "John Doe",
    EnrollmentDate = DateTime.UtcNow
};

    db.Students.Add(student);
db.SaveChanges();

// Read all students
var students = db.Students.ToList();
foreach (var s in students)
    Console.WriteLine($"{s.StudentId}: {s.Name}");
