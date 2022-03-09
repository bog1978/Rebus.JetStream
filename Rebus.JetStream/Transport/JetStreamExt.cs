using System.IO;
using System.Text;

using NATS.Client;

using Rebus.Messages;
using Rebus.Serialization;
using Rebus.Transport;

namespace Rebus.JetStream.Transport
{
    internal static class JetStreamExt
    {
        private static readonly HeaderSerializer HeaderSerializer = new HeaderSerializer();

        public static byte[] Serialize(this AbstractRebusTransport.OutgoingMessage msg)
        {
            var bodyBytes = msg.TransportMessage.Body;
            var headersBytes = HeaderSerializer.Serialize(msg.TransportMessage.Headers);

            using var ms = new MemoryStream();
            using var bs = new BinaryWriter(ms, Encoding.UTF8);

            bs.Write(headersBytes.Length);
            bs.Write(headersBytes);
            bs.Write(bodyBytes.Length);
            bs.Write(bodyBytes);

            bs.Flush();
            ms.Flush();

            return ms.ToArray();
        }

        public static TransportMessage Deserialize(this Msg stanMsg)
        {
            using var ms = new MemoryStream(stanMsg.Data);
            using var br = new BinaryReader(ms, Encoding.UTF8);

            var headersLength = br.ReadInt32();
            var headersBytes = br.ReadBytes(headersLength);
            var bodyLength = br.ReadInt32();
            var bodyBytes = br.ReadBytes(bodyLength);

            var headers = HeaderSerializer.Deserialize(headersBytes);
            var msg = new TransportMessage(headers, bodyBytes);
            return msg;
        }
    }
}