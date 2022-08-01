using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Routing.TypeBased;
using Rebus.Tests.Contracts;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

#pragma warning disable 1998

namespace Rebus.MongoDb.Tests.Transport
{
    [TestFixture]
    public class TestDefer : FixtureBase
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

            var serverBus = Configure.With(Using(server))
                .Transport(t => t.UseMongoDb(new MongoDbTransportOptions(MongoTestHelper.GetUrl()), "server"))
                //.Timeouts(t => t.StoreInMongoDb(db, "rebus-timeouts"))
                .Options(o =>
                {
                    o.SetNumberOfWorkers(0);
                    o.SetMaxParallelism(1);
                })
                .Start();

            var clientBus = Configure.With(Using(new BuiltinHandlerActivator()))
                .Transport(t => t.UseMongoDbAsOneWayClient(new MongoDbTransportOptions(MongoTestHelper.GetUrl())))
                //.Timeouts(t => t.StoreInMongoDb(db, "rebus-timeouts"))
                .Routing(t => t.TypeBased().Map<string>("server"))
                .Start();

            await SendMessage(clientBus).ConfigureAwait(false);

            serverBus.Advanced.Workers.SetNumberOfWorkers(1);

            Thread.Sleep(0);
            Assert.That(handled, Is.False);

            //wait for the timeout to kick.
            received.WaitOne(10 * 1000);

            Assert.That(handled);
        }

        private static Task SendMessage(IBus clientBus) => clientBus.Defer(TimeSpan.FromSeconds(3), "Test deferred message",
            new Dictionary<string, string>());
    }
}
