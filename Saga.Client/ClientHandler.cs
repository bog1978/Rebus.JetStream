using System;
using System.Threading.Tasks;
using Rebus.Handlers;
using Saga.Messages;

namespace Saga.Client
{
    public class ClientHandler : IHandleMessages<ExampleResult>
    {
        private static int _cnt;
        public Task Handle(ExampleResult message)
        {
            _cnt++;
            if (_cnt % 100 == 0)
            {
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine($"{DateTime.Now:T} Результатов получено: {_cnt}");
            }
            return Task.FromResult(true);
        }
    }
}