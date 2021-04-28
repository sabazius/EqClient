using Confluent.Kafka;
using MessagePack;

namespace EqClient.DataLayer.Common
{
    public class MsgPackSerializer<T> : ISerializer<T>
    {
        public byte[] Serialize(T data, SerializationContext context)
        {
            return MessagePackSerializer.Serialize<T>(data);
        }
    }
}
