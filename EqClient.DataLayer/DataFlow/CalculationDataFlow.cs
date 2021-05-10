using System.Threading.Tasks;
using EqClient.DataLayer.Helpers;
using EqClient.DataLayer.Models;
using MessagePack;
using System.Threading.Tasks.Dataflow;
using Microsoft.Extensions.Logging;

namespace EqClient.DataLayer.DataFlow
{
    public class CalculationDataFlow : ICalculationDataFlow
    {
        private readonly ILogger<CalculationDataFlow> _logger;

        private TransformBlock<byte[], CalculationPack> _deserializeBlock;
        private TransformBlock<CalculationPack, CalculationPack> _calculationBlock;
        private ActionBlock<CalculationPack> _publishBlock;

        public CalculationDataFlow(ILogger<CalculationDataFlow> logger)
        {
            _logger = logger;

            _deserializeBlock = new TransformBlock<byte[], CalculationPack>(msg => MessagePackSerializer.Deserialize<CalculationPack>(msg));

            _calculationBlock = new TransformBlock<CalculationPack, CalculationPack>(pack =>
            {
                Parallel.ForEach(pack.Data, unit =>
                {
                    unit.Equation.Result = CalculationHelper.Calculate(unit.Equation.EqMethod, unit.Equation.Values);
                });

                return pack;
            });

            _publishBlock = new ActionBlock<CalculationPack>(pack =>
            {
                //TODO publish in kafka topic

                //_logger.LogInformation(pack.Id.ToString());

                foreach (var eq in pack.Data)
                {
                    _logger.LogInformation($"PackId: {eq.CalcPackId} EqId: {eq.Equation.Id} Result:{eq.Equation.Result}");
                }

                var result = pack;
            });

            var linkOptions = new DataflowLinkOptions()
            {
                PropagateCompletion = true
            };

            _deserializeBlock.LinkTo(_calculationBlock, linkOptions);
            _calculationBlock.LinkTo(_publishBlock, linkOptions);
        }


        public async void ProcessMessage(byte[] msg)
        {
            await _deserializeBlock.SendAsync(msg);
        }
    }


}
