using Microsoft.AspNetCore.Mvc;
using Confluent.Kafka;

namespace KafkaApi.Controllers
{
    [ApiController]
    [Route("Producer")]
    public class ProducerController : ControllerBase
    {
        private readonly ProducerConfig config = new ProducerConfig
        { BootstrapServers = "localhost:19392" };
        private readonly string topic = "simpletalk_topic";

        //./kafka-topics.sh --create --bootstrap-server kafka-broker-1:19392 --replication-factor 1 --partitions 1 --topic simpletalk_topic
        //./kafka-topics.sh --create --bootstrap-server kafka-broker-1:19392 --replication-factor 1 --partitions 1 --topic simpletalk_topic

        /// <summary>
        /// This endpoint acts as a producer for kafka
        /// </summary>
        /// <param name="message">Message to publish</param>
        /// <returns></returns>
        [HttpPost]
        public async Task<IActionResult> Post([FromQuery] string message)
        {
            try
            {
                await SendToKafkaAsync(topic, message);
            }
            catch(Exception ex)
            {
                return StatusCode(500,ex.ToString());
            }
            return Ok("Message sent.");
        }

        /// <summary>
        /// Communicates with kafka
        /// </summary>
        /// <param name="topic">Topic to which message has to be published</param>
        /// <param name="message">Actual message</param>
        /// <returns></returns>
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
                    var dr = await producer.ProduceAsync(topic, new Message<Null, string> { Value = message });
                    Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                    
                }
                catch (Exception e)
                {
                    return $"Oops, something went wrong: {e}";
                }
            }
            return "success";
        }
    }
}
