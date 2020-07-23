using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Logging;
using Rebus.MongoDb.Tests;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Utilities;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

#pragma warning disable 1998

namespace Rebus.MongoDb.Tests.Transport
{
    [TestFixture, Category(MongoTestHelper.TestCategory)]
    public class TestMongoDbReceivePerformance : FixtureBase
    {
        private const string QueueName = "perftest";

        protected override void SetUp()
        {
            MongoTestHelper.DropMongoDatabase();
        }

        /// <summary>
        /// Can be used as a raw measurement of performance of using mongodb
        /// as a transport.
        /// </summary>
        /// <param name="messageCount"></param>
        /// <returns></returns>
        [TestCase(10)]
        [TestCase(100)]
        [TestCase(1000)]
        //[TestCase(4000)]
        public async Task CheckReceivePerformance(int messageCount)
        {
            var adapter = Using(new BuiltinHandlerActivator());

            Configure.With(adapter)
                .Logging(l => l.ColoredConsole(LogLevel.Warn))
                .Transport(t =>
                {
                    t.UseMongoDb(new MongoDbTransportOptions(MongoTestHelper.GetUrl()), QueueName);
                })
                .Options(o =>
                {
                    o.SetNumberOfWorkers(0);
                    o.SetMaxParallelism(20);
                })
                .Start();

            Console.WriteLine($"Sending {messageCount} messages...");

            await Task.WhenAll(Enumerable.Range(0, messageCount)
                .Select(i => adapter.Bus.SendLocal($"THIS IS MESSAGE {i}")));

            var counter = Using(new SharedCounter(messageCount));

            adapter.Handle<string>(async message => counter.Decrement());

            Console.WriteLine("Waiting for messages to be received...");

            var stopwtach = Stopwatch.StartNew();

            adapter.Bus.Advanced.Workers.SetNumberOfWorkers(3);

            counter.WaitForResetEvent(timeoutSeconds: messageCount / 100 + 5);

            var elapsedSeconds = stopwtach.Elapsed.TotalSeconds;

            Console.WriteLine($"{messageCount} messages received in {elapsedSeconds:0.0} s - that's {messageCount / elapsedSeconds:0.0} msg/s");
        }
    }
}
