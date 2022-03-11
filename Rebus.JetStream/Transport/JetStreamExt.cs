using System.Collections.Generic;

using NATS.Client;

using Rebus.Messages;
using Rebus.Transport;

namespace Rebus.JetStream.Transport
{
    internal static class JetStreamExt
    {
        public static Msg Serialize(this AbstractRebusTransport.OutgoingMessage message)
        {
            var headers = new MsgHeader();
            foreach (var h in message.TransportMessage.Headers)
                headers.Add(h.Key, h.Value);
            return new Msg(message.DestinationAddress + "-subject", headers, message.TransportMessage.Body);
        }

        public static TransportMessage Deserialize(this Msg msg)
        {
            var body = msg.Data;
            var headers = new Dictionary<string, string>();
            foreach (string k in msg.Header.Keys)
                headers.Add(k, msg.Header[k]);
            return new TransportMessage(headers, body);
        }
    }
}