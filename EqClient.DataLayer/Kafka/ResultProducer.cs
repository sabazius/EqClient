using Confluent.Kafka;
using EqClient.DataLayer.Common;
using EqClient.DataLayer.Models;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace EqClient.DataLayer.Kafka.Interfaces
{
    public class ResultProducer : IResultProducer
    {
        private readonly ILogger<ResultProducer> _logger;
        private readonly IProducer<int, CalculationPack> _producer;

        public ResultProducer(ILogger<ResultProducer> logger)
        {
            _logger = logger;

            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9092"
            };

            _producer = new ProducerBuilder<int, CalculationPack>(config)
                .SetValueSerializer(new MsgPackSerializer<CalculationPack>())
                .Build();


        }

        public async Task ProduceResultAsync(CalculationPack data)
        {

            var msg = new Message<int, CalculationPack>()
            {
                Key = data.Id,
                Value = data
            };

            await _producer.ProduceAsync("results", msg);

        }
    }
}
