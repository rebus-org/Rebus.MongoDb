using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Tests.Contracts;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

#pragma warning disable 1998

namespace Rebus.MongoDb.Tests.Transport
{
    [TestFixture]
    public class TestDeferLocal : FixtureBase
    {
        protected override void SetUp() => MongoTestHelper.DropMongoDatabase();

        [Test]
        public async Task CanSetupTimemoutsOnMongodbTransport() => await RunTest();

        private async Task RunTest()
        {
            var server = new BuiltinHandlerActivator();
            ManualResetEvent received = new ManualResetEvent(false);
            Boolean handled = false;
            server.Handle<string>(async str =>
            {
                Console.WriteLine($"Received message: {str}");
                handled = true;
                received.Set();
            });

            var bus = Configure.With(Using(server))
                .Transport(t => t.UseMongoDb(new MongoDbTransportOptions(MongoTestHelper.GetUrl()), "server"))
                //.Timeouts(t => t.StoreInMongoDb(db, "rebus-timeouts"))
                .Options(o =>
                {
                    o.SetNumberOfWorkers(0);
                    o.SetMaxParallelism(1);
                })
                .Start();

            await SendMessage(bus).ConfigureAwait(false);

            bus.Advanced.Workers.SetNumberOfWorkers(1);

            Thread.Sleep(0);
            Assert.That(handled, Is.False);

            //wait for the timeout to kick.
            received.WaitOne(10 * 1000);

            Assert.That(handled);
        }

        private static Task SendMessage(IBus clientBus) => clientBus.DeferLocal(TimeSpan.FromSeconds(3), "Test deferred message",
            new Dictionary<string, string>());
    }
}
