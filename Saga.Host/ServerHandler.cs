using System;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Handlers;
using Saga.Messages;

namespace Saga.Host
{
    public class ServerHandler : IHandleMessages<ExampleRequest>
    {
        private readonly IBus _bus;
        private static int _cnt;

        public ServerHandler(IBus bus)
        {
            _bus = bus;
        }

        public async Task Handle(ExampleRequest message)
        {
            _cnt++;
            if (_cnt % 100 == 0)
            {
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine($"{DateTime.Now:T} Запросов получено: {_cnt}");
            }
            //создание экземпляра результата операции.
            var result = new ExampleResult
            {
                RequestId = message.RequestId,
                Sum = message.A + message.B,
            };
            await _bus.Reply(result);
        }
    }
}