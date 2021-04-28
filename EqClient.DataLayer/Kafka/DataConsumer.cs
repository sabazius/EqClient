using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;
using EqClient.DataLayer.Common;
using System.Text;
using EqClient.DataLayer.Models;
using Newtonsoft.Json;

namespace EqClient.DataLayer.Kafka
{
    public class DataConsumer : BackgroundService
    {
        private readonly ILogger<DataConsumer> _logger;
        private readonly ConsumerConfig _kafkaConfig;

        public DataConsumer(ILogger<DataConsumer> logger)
        {
            _logger = logger;

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

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Run( () =>
            {
                using var consumer = new ConsumerBuilder<int, byte[]>(_kafkaConfig)
                    //.SetValueDeserializer(new MsgPackDeserializer<CalculationPack>())
                    .Build();

                try
                {
                    consumer.Subscribe("data");

                    while (!stoppingToken.IsCancellationRequested)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(stoppingToken);

                            try
                            {
                                _logger.LogInformation(consumeResult.Message.Key.ToString());
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Kafka consume message error: {0}", ex.Message);
                            }
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
                    _logger.LogError(e, $"Consumer for topics data: {e.Message}" );
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Consumer for topics '{string.Join(';', "data")}'. Exception.");
                }
            }, stoppingToken);
        }
    }
}
