using System;
using System.Threading.Tasks;

using Rebus.Activation;
using Rebus.Bus;

using Saga.Messages;

namespace Saga.Client
{
    internal class Program
    {
        private static Random _random = new Random(DateTime.Now.Millisecond);

        private static async Task Main()
        {
            Console.Title = "Тестовый клиент ReBus. SPACE - отправка запроса, ESC - выход.";

            try
            {
                using var bus = await CreateServerBus();
                while (true)
                {
                    var ch = Console.ReadKey(true);
                    switch (ch.Key)
                    {
                        case ConsoleKey.Escape:
                            Console.WriteLine("Выход...");
                            bus.Dispose();
                            return;
                        case ConsoleKey.Spacebar:
                            await SendMessage(bus);
                            break;
                        case ConsoleKey.N:
                            for (var i = 0; i < 10000; i++)
                                await SendMessage(bus);
                            break;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        private static int _cnt;
        private static async Task SendMessage(IBus bus)
        {
            var commandMessage = new ExampleRequest
            {
                RequestId = Guid.NewGuid(),
                A = _random.Next(1, 100),
                B = _random.Next(1, 100),
            };
            _cnt++;
            if (_cnt % 100 == 0)
            {
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine($"{DateTime.Now:T} Запросов отправлено: {_cnt}");
            }
            await bus.Send(commandMessage);
        }

        private static async Task<IBus> CreateServerBus()
        {
            var activator = new BuiltinHandlerActivator();
            activator.Register((b, c) => new ClientHandler());
            return await activator.CreateServerBus(QueueNames.client_queue);
        }
    }
}
