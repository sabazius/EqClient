using MessagePack;

namespace EqClient.DataLayer.Models
{
    [MessagePackObject]
    public class Equation
    {
        [Key(0)]
        public int Id { get; set; }

        [Key(1)]
        public string EqMethod { get; set; }

        [Key(2)]
        public int Result { get; set; }
    }
}
