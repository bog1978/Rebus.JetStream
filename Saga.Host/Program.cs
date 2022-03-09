using System;
using System.Threading.Tasks;

using Rebus.Activation;
using Rebus.Bus;

using Saga.Messages;

namespace Saga.Host
{
    internal class Program
    {
        private static async Task Main()
        {
            Console.Title = "Тестовый сервис ReBus. ESC - выход.";

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
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        private static async Task<IBus> CreateServerBus()
        {
            var activator = new BuiltinHandlerActivator();
            activator.Register((b, c) => new ServerHandler(b));
            return await activator.CreateServerBus(QueueNames.example_queue);
        }
    }
}
