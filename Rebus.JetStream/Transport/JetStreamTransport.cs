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

        private const string CurrentStanMessageKey = "nats-transport-message";

        private readonly string _inputQueueName;
        private readonly ILog _log;
        private readonly IConnection _connection;
        private readonly IJetStream _js;

        #endregion

        #region Конструкторы

        public JetStreamTransport(string inputQueueName, IRebusLoggerFactory rebusLoggerFactory) : base(inputQueueName)
        {
            _inputQueueName = inputQueueName;
            _log = rebusLoggerFactory.GetLogger<JetStreamTransport>();
            var factory = new ConnectionFactory();
            _connection = factory.CreateConnection();
            _js = _connection.CreateJetStreamContext();
        }

        #endregion

        #region AbstractRebusTransport

        public override void CreateQueue(string address)
        {
            // Очереди создаются во время подписки или публикации.
        }

        public override Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            //context.OnDisposed(DisposedAction);
            context.OnCompleted(CompletedAction);
            //context.OnAborted(AbortedAction);
            //context.OnCommitted(CommitAction);

            var tcs = new TaskCompletionSource<TransportMessage>();

            void Handler(object sender, MsgHandlerEventArgs e)
            {
                context.GetOrAdd(CurrentStanMessageKey, () => e.Message);
                try
                {
                    var msg = e.Message.Deserialize();
                    var isOk = tcs.TrySetResult(msg);
                    if (!isOk)
                    {
                    }
                }
                catch (Exception ex)
                {
                    tcs.SetException(ex);
                }
            }

            var subscription = _js.PushSubscribeAsync(_inputQueueName, Handler, false);

            tcs.Task.Wait(cancellationToken);
            return tcs.Task;
        }

        protected override Task SendOutgoingMessages(IEnumerable<OutgoingMessage> outgoingMessages, ITransactionContext context)
        {
            foreach (var message in outgoingMessages)
                _connection.Publish(message.DestinationAddress, message.Serialize());
            return Task.FromResult(true);
        }

        #endregion

        #region IDisposable

        public void Dispose()
        {
            _connection?.Dispose();
        }

        #endregion

        #region Другое

        private void AbortedAction(ITransactionContext ctx)
        {
            //_log.Info("OnAborted");
        }

        private Task CommitAction(ITransactionContext ctx)
        {
            //_log.Info("OnCommitted");
            return Task.FromResult(true);
        }

        private Task CompletedAction(ITransactionContext ctx)
        {
            try
            {
                var stanMsg = ctx.GetOrAdd<Msg>(CurrentStanMessageKey, null);

                if (stanMsg != null)
                {
                    stanMsg.Ack();
                    stanMsg.ArrivalSubscription.Unsubscribe();
                    //stanMsg.Subscription.Dispose();
                }
                else
                {
                }
            }
            catch (Exception ex)
            {
                // Ignore
            }

            //_log.Info("OnCompleted");
            return Task.FromResult(true);
        }

        private void DisposedAction(ITransactionContext ctx)
        {
            //_log.Info("OnDisposed");
        }

        #endregion
    }
}