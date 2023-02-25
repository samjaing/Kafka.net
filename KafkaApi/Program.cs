using Microsoft.Extensions.Options;
using Microsoft.OpenApi.Models;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
//builder.Services.AddSingleton<IHostedService, KafkaApi.Models.Consumer>();

builder.Services.AddControllers();
builder.Services.AddSwaggerGen(options =>
{
    options.SwaggerDoc("v1", new OpenApiInfo
    {
        Version = "v1",
        Title = "PubSub.API",
        Description = "An ASP.NET Core Web API for managing ToDo items",
        Contact = new OpenApiContact
        {
            Name = "Shantanu",
        },

    });
    options.IncludeXmlComments("obj\\Debug\\net6.0\\KafkaApi.xml");
});

var app = builder.Build();

// Configure the HTTP request pipeline.

app.UseAuthorization();
app.UseSwagger();
app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "PubSub.API v1"));

//app.UseSwaggerUI(options =>
//{
//    options.SwaggerEndpoint("/swagger/v1/swagger.json", "v1");
//    options.RoutePrefix = string.Empty;
//});

app.MapControllers();

app.Run();
