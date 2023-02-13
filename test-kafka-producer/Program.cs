using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Net;

public class Program
{
    private static void Main(string[] args)
    {
        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .AddFilter("Microsoft", LogLevel.Warning)
                .AddFilter("System", LogLevel.Warning)
                .AddFilter("LoggingConsoleApp.Program", LogLevel.Debug)
                .AddConsole();
        });
        ILogger logger = loggerFactory.CreateLogger<Program>();
        logger.LogInformation("Program is starting...");

        var appConfig = new ConfigurationBuilder().AddJsonFile($"appsettings.json").Build();

        string topic = appConfig["kafka:topic"];
        string bootstrapServers = appConfig["kafka:bootstrap-server"];

        ProducerConfig producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers
        };

        try
        {
            using (var producer = new ProducerBuilder<Null, string>(producerConfig).Build())
            {
                var message = new Message<Null, string> { Value = $"Hello-Kafka-{DateTime.Now.ToString("yyyy-MM-dd-HH-mm-ss")}" };
                producer.Produce(topic, message);
                logger.LogInformation($"Producing: {JsonConvert.SerializeObject(message)}");

                producer.Flush();
            }
        }
        catch (Exception e)
        {
            logger.LogError(e.Message);
        }

        logger.LogInformation("Program has finished.");
    }
}