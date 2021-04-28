using System;
using System.Collections.Generic;
using System.Text;
using MessagePack;

namespace EqClient.DataLayer.Models
{
    [MessagePackObject]
    public class CalculationPack
    {
        [Key(0)]
        public int Id { get; set; }
        [Key(1)]
        public int Count { get; set; }
        [Key(2)]
        public int EqId { get; set; }
        [Key(3)]
        public List<CalculationUnit> Data { get; set; }
    }
}
