using Microsoft.AspNetCore.Mvc;
using Confluent.Kafka;

namespace KafkaApi.Controllers
{
    [ApiController]
    [Route("Producer")]
    public class ProducerController : ControllerBase
    {
        private readonly ProducerConfig config = new ProducerConfig
        { BootstrapServers = "localhost:9092" };
        private readonly string topic = "simpletalk_topic";

        //./kafka-topics.sh --create --bootstrap-server kafka-broker-1:9092 --replication-factor 1 --partitions 1 --topic simpletalk_topic

        //./kafka-topics.sh --create --bootstrap-server kafka-broker-1:9092 --replication-factor 1 --partitions 1 --topic simpletalk_topic


        [HttpPost]
        public IActionResult Post([FromQuery] string message)
        {
            return Created(string.Empty, SendToKafkaAsync(topic, message));
        }

        [HttpGet]
        public IActionResult Get()
        {
            Console.WriteLine($"sambhav");
            return Created(string.Empty, SendToKafkaAsync(topic, "hoha").Result);
        }

        private async Task<object> SendToKafkaAsync(string topic, string message)
        {
            Action<DeliveryReport<Null, string>> handler = r =>
           Console.WriteLine(!r.Error.IsError
               ? $"Delivered message to {r.TopicPartitionOffset}"
               : $"Delivery Error: {r.Error.Reason}");

            using (var producer =
                 new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    //producer.Produce(topic, new Message<Null, string> { Value = message }, handler);
                    var dr = await producer.ProduceAsync(topic, new Message<Null, string> { Value = message });
                    Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                    return "success";
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Oops, something went wrong: {e}");
                }
            }
            return null;
        }
    }
}
