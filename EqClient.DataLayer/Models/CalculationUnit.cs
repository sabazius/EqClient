using MessagePack;

namespace EqClient.DataLayer.Models
{
    [MessagePackObject]
    public class CalculationUnit
    {
        [Key(0)]
        public int Number { get; set; }
        [Key(1)]
        public int CalcPackId { get; set; }
        [Key(2)]
        public Equation Equation { get; set; }
    }
}
