using System;

using Rebus.JetStream.Transport;
using Rebus.Logging;
using Rebus.Transport;

namespace Rebus.Config
{
    public static class JetStreamTransportConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use NATS Streaming as its transport.
        /// </summary>
        /// <param name="configurer">Static to extend</param>
        /// <param name="inputQueueName">Queue name to process messages from</param>
        public static void UseNatsStreaming(this StandardConfigurer<ITransport> configurer, string inputQueueName)
        {
            if (configurer == null)
                throw new ArgumentNullException(nameof(configurer));
            //if (network == null) throw new ArgumentNullException(nameof(network));
            if (inputQueueName == null)
                throw new ArgumentNullException(nameof(inputQueueName));

            configurer.OtherService<JetStreamTransport>()
                .Register(context => new JetStreamTransport(inputQueueName, context.Get<IRebusLoggerFactory>()));

            //configurer.OtherService<ITransportInspector>()
            //    .Register(context => context.Get<NatsStreamingTransport>());

            configurer.Register(context => context.Get<JetStreamTransport>());
        }
    }
}
