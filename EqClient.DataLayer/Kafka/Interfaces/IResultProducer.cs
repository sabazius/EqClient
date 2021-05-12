using System.Collections.Generic;
using System.Threading.Tasks;
using EqClient.DataLayer.Models;

namespace EqClient.DataLayer.Kafka
{
    public interface IResultProducer
    {
        Task ProduceResultAsync(CalculationPack data);
    }
}