using Confluent.Kafka;
using EqClient.DataLayer.DataFlow;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace EqClient.DataLayer.Kafka
{
    public class DataConsumer : BackgroundService
    {
        private readonly ILogger<DataConsumer> _logger;
        private readonly ConsumerConfig _kafkaConfig;
        private readonly ICalculationDataFlow _calculationDataFlow;

        public DataConsumer(ILogger<DataConsumer> logger, ICalculationDataFlow calculationDataFlow)
        {
            _logger = logger;
            _calculationDataFlow = calculationDataFlow;

            _kafkaConfig = new ConsumerConfig
            {
                EnableAutoCommit = true,
                AutoCommitIntervalMs = 5000,
                FetchWaitMaxMs = 50,
                BootstrapServers = "localhost:9092",
                GroupId = $"EqClient",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
        }

        private Message<long, object> ExecuteQuery()
        {
            using var consumer = new ConsumerBuilder<int, byte[]>(_kafkaConfig)
                //.SetValueDeserializer(new MsgPackDeserializer<CalculationPack>())
                .Build();

            consumer.Subscribe("data");

            while (true)
            {
                consumer.Position(new TopicPartition("data", 2));
            }
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Run(() =>
           {
               using var consumer = new ConsumerBuilder<int, byte[]>(_kafkaConfig)
                   //.SetValueDeserializer(new MsgPackDeserializer<CalculationPack>())
                   .Build();

               try
               {
                   //consumer.Subscribe("data");

                   //consumer.Position(new TopicPartition("data",0));// Assign(new TopicPartition("data", 0));
                   consumer.Assign(new TopicPartitionOffset("data", 0, 19));
                   //consumer.Seek(new TopicPartitionOffset(new TopicPartition("data", 0),4));
                   //var consumeR = consumer.Consume(stoppingToken);

                   //var consumeResult = consumer.Consume(stoppingToken);

                   //_calculationDataFlow.ProcessMessage(consumeResult.Message.Value);

                   while (!stoppingToken.IsCancellationRequested)
                   {
                       try
                       {
                           var consumeResult = consumer.Consume(stoppingToken);

                           try
                           {
                               _calculationDataFlow.ProcessMessage(consumeResult.Message.Value);

                               _logger.LogInformation(consumeResult.Message.Key.ToString());
                           }
                           catch (Exception ex)
                           {
                               _logger.LogError(ex, "Kafka consume message error: {0}", ex.Message);
                           }

                           if (consumeResult.IsPartitionEOF)
                               break;

                       }
                       catch (ConsumeException e)
                       {
                           _logger.LogError(e, $"Consumer for topic '{e.ConsumerRecord.Topic}'. ConsumeException, Key: {Encoding.UTF8.GetString(e.ConsumerRecord.Message.Key)}, Error: {JsonConvert.SerializeObject(e.Error)}");
                       }
                       catch (Exception ex)
                       {
                           _logger.LogError(ex, "Kafka consume message error: {0}", ex.Message);
                       }
                   }
               }
               catch (OperationCanceledException e)
               {
                   _logger.LogError(e, $"Consumer for topics data: {e.Message}");
               }
               catch (Exception ex)
               {
                   _logger.LogError(ex, $"Consumer for topics '{string.Join(';', "data")}'. Exception.");
               }
           }, stoppingToken);
        }
    }
}
