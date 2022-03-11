using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Rebus.Logging;
using Rebus.Messages;
using Rebus.Transport;

using NATS.Client;
using NATS.Client.JetStream;

namespace Rebus.JetStream.Transport
{
    internal class JetStreamTransport : AbstractRebusTransport, IDisposable
    {
        #region Константы и поля

        private readonly ILog _log;
        private readonly IConnection _connection;
        private readonly IJetStream _jetStream;
        private IJetStreamManagement _jsm;
        private IJetStreamPushSyncSubscription _sub;

        #endregion

        #region Конструкторы

        public JetStreamTransport(string inputQueueName, IRebusLoggerFactory rebusLoggerFactory) : base(inputQueueName)
        {
            try
            {
                _log = rebusLoggerFactory.GetLogger<JetStreamTransport>();
                var factory = new ConnectionFactory();
                _connection = factory.CreateConnection();
                _jetStream = _connection.CreateJetStreamContext();
                _jsm = _connection.CreateJetStreamManagementContext();
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        #endregion

        #region AbstractRebusTransport

        public override void CreateQueue(string address)
        {
            try
            {
                CreateQueueIfNotExists(address);
            }
            catch (Exception ex)
            {
                _log.Error(ex, "CreateQueue failed.");
                throw;
            }
        }

        private bool _subscribed;

        private void Subscribe()
        {
            if (_subscribed)
                return;
            try
            {
                CreateQueueIfNotExists(Address);
                var pso = PushSubscribeOptions.BindTo(Address + "-stream", Address + "-durable");
                _sub = _jetStream.PushSubscribeSync(Address + "-subject", Address + "-queue", pso);
                _subscribed = true;
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Subscribe failed.");
                throw;
            }
        }

        public override Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            Subscribe();
            TransportMessage Receive()
            {
                Msg msg = null;
                try
                {
                    msg = _sub.NextMessage(1000);
                }
                catch (NATSTimeoutException ex)
                {
                    return null;
                }

                var message = msg.Deserialize();
                context.OnCompleted(ctx =>
                {
                    msg.Ack();
                    return Task.FromResult(true);
                });

                return message;
            }

            return Task.Run(Receive);
        }

        protected override async Task SendOutgoingMessages(IEnumerable<OutgoingMessage> outgoingMessages, ITransactionContext context)
        {
            try
            {
                foreach (var message in outgoingMessages)
                {
                    CreateQueueIfNotExists(message.DestinationAddress);
                    var msg = message.Serialize();
                    var ack = await _jetStream.PublishAsync(msg);
                    ack.ThrowOnHasError();
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex, "SendOutgoingMessages failed.");
                throw;
            }
        }

        #endregion

        #region IDisposable

        public void Dispose()
        {
            if (_sub != null)
            {
                _sub.Drain();
                _sub.Dispose();
            }
            if (_connection != null)
            {
                _connection.Drain();
                _connection.Dispose();
            }
        }

        #endregion

        public void CreateQueueIfNotExists(string subject)
        {
            var streamName = subject + "-stream";

            try
            {
                var streams = _jsm.GetStreamNames();
                if (streams.Contains(streamName))
                    return;

                var sc = StreamConfiguration
                    .Builder()
                    .WithStorageType(StorageType.File)
                    .WithName(streamName)
                    .WithSubjects((string)(subject + "-subject"))
                    .Build();
                _jsm.AddStream(sc);

                var cc = ConsumerConfiguration.Builder()
                    .WithDurable(subject + "-durable")
                    .WithDeliverSubject(subject + "-deliver")
                    .WithDeliverGroup(subject + "-queue")
                    .Build();
                _jsm.AddOrUpdateConsumer(streamName, cc);
            }
            catch (Exception ex)
            {
                _log.Error(ex, "CreateQueueIfNotExists failed.");
                throw;
            }
        }
    }
}