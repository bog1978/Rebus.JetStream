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

        private const string CurrentJetStreamMessageKey = "jetstream-transport-message";

        private readonly string _inputQueueName;
        private readonly ILog _log;
        private readonly IConnection _connection;
        private readonly IJetStream _jetStream;
        private IJetStreamPullSubscription _sub;

        #endregion

        #region Конструкторы

        public JetStreamTransport(string inputQueueName, IRebusLoggerFactory rebusLoggerFactory) : base(inputQueueName)
        {
            _inputQueueName = inputQueueName;
            _log = rebusLoggerFactory.GetLogger<JetStreamTransport>();
            var factory = new ConnectionFactory();
            _connection = factory.CreateConnection();
            _jetStream = _connection.CreateJetStreamContext();
        }

        #endregion

        #region AbstractRebusTransport

        public override void CreateQueue(string address)
        {
            // Очереди создаются во время подписки или публикации.
            try
            {
                CreateStreamWhenDoesNotExist(_inputQueueName, _inputQueueName);

                _sub = _jetStream.PullSubscribe(
                    _inputQueueName,
                    PullSubscribeOptions
                        .Builder()
                        .WithStream(_inputQueueName)
                        .WithDurable(_inputQueueName)
                        .Build());

            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public override Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
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
                    CreateStreamWhenDoesNotExist(message.DestinationAddress, message.DestinationAddress);
                    var msg = message.Serialize();
                    var ack = await _jetStream.PublishAsync(msg);
                    ack.ThrowOnHasError();
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        #endregion

        #region IDisposable

        public void Dispose()
        {
            if (_sub != null)
            {
                _sub.Unsubscribe();
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

        public void CreateStreamWhenDoesNotExist(string stream, params string[] subjects)
        {
            var jsm = _connection.CreateJetStreamManagementContext();

            try
            {
                jsm.GetStreamInfo(stream); // this throws if the stream does not exist
                return;
            }
            catch (NATSJetStreamException)
            {
                /* stream does not exist */
            }

            StreamConfiguration sc = StreamConfiguration.Builder()
                .WithName(stream)
                .WithStorageType(StorageType.Memory)
                .WithSubjects(subjects)
                .Build();
            jsm.AddStream(sc);
        }
    }
}