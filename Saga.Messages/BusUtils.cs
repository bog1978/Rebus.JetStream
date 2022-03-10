using System;
using System.Configuration;
using System.Threading.Tasks;

using Rebus.Activation;
using Rebus.Backoff;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Persistence.InMem;
using Rebus.Routing.TypeBased;
using Rebus.Transport;

namespace Saga.Messages
{
    public static class BusUtils
    {
        public static async Task<IBus> CreateServerBus(this BuiltinHandlerActivator activator, string queueName)
        {
            var bus = Configure.With(activator)
                .Logging(l => l.ColoredConsole(LogLevel.Info))
                .Transport(t => ConfigureTransport(t, queueName))
                .Subscriptions(s => s.StoreInMemory())
                .Options(ConfigureOptions)
                .Routing(r => r.TypeBased()
                    .Map<ExampleRequest>(QueueNames.example_queue)
                    .Map<ExampleResult>(QueueNames.client_queue))
                .Start();

            await bus.Subscribe<ExampleRequest>();
            return bus;
        }

        private static void ConfigureOptions(OptionsConfigurer o)
        {
            o.SetMaxParallelism(1);
            o.SetBackoffTimes(
                TimeSpan.FromMilliseconds(0)/*,
                TimeSpan.FromMilliseconds(0),
                TimeSpan.FromMilliseconds(0),
                TimeSpan.FromMilliseconds(10),
                TimeSpan.FromMilliseconds(10),
                TimeSpan.FromMilliseconds(10),
                TimeSpan.FromMilliseconds(100),
                TimeSpan.FromMilliseconds(100),
                TimeSpan.FromMilliseconds(100),
                TimeSpan.FromMilliseconds(1000)*/);
        }

        //private static void ConfigureTransport(StandardConfigurer<ITransport> t, string queueName)
        //{
        //    var connectionString = ConfigurationManager.AppSettings["RebusConnectionSetting"];
        //    var opt = new SqlServerTransportOptions(connectionString);
        //    t.UseSqlServer(opt, queueName);
        //}

        private static void ConfigureTransport(StandardConfigurer<ITransport> t, string queueName)
        {
            t.UseNatsStreaming(queueName);
        }
    }
}
