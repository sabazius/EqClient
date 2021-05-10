namespace EqClient.DataLayer.DataFlow
{
    public interface ICalculationDataFlow
    {
        void ProcessMessage(byte[] msg);
    }
}
