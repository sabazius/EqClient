using System.Collections.Generic;
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
        public List<int> Values { get; set; }

        [Key(3)]
        public int Result { get; set; }
    }
}
