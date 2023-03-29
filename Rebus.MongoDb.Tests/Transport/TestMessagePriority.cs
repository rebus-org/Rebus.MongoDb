//using NUnit.Framework;
//using Rebus.Activation;
//using Rebus.Bus;
//using Rebus.Config;
//using Rebus.MongoDb.Transport;
//using Rebus.Routing.TypeBased;
//using Rebus.Serialization.Json;
//using Rebus.Tests.Contracts;
//using Rebus.Tests.Contracts.Extensions;
//using Rebus.Tests.Contracts.Utilities;
//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Threading.Tasks;

//#pragma warning disable 1998

//namespace Rebus.MongoDb.Tests.Transport
//{
//    [TestFixture]
//    public class TestMessagePriority : FixtureBase
//    {
//        protected override void SetUp() => MongoTestHelper.DropMongoDatabase();

//        [Test]
//        public async Task ReceivedMessagesByPriority_HigherIsMoreImportant_Normal() => await RunTest(20);

//        async Task RunTest(int messageCount)
//        {
//            var counter = new SharedCounter(messageCount);
//            var receivedMessagePriorities = new List<int>();
//            var server = new BuiltinHandlerActivator();

//            server.Handle<string>(async str =>
//            {
//                Console.WriteLine($"Received message: {str}");
//                var parts = str.Split(' ');
//                var priority = int.Parse(parts[1]);
//                receivedMessagePriorities.Add(priority);
//                counter.Decrement();
//            });

//            var serverBus = Configure.With(Using(server))
//                .Transport(t =>
//                {
//                    t.UseMongoDb(new MongoDbTransportOptions(MongoTestHelper.GetUrl()), "server");
//                })
//                .Options(o =>
//                {
//                    o.SetNumberOfWorkers(0);
//                    o.SetMaxParallelism(1);
//                })
//                .Start();

//            var clientBus = Configure.With(Using(new BuiltinHandlerActivator()))
//                .Transport(t =>
//                {
//                    t.UseMongoDbAsOneWayClient(new MongoDbTransportOptions(MongoTestHelper.GetUrl()));
//                })
//                .Routing(t => t.TypeBased().Map<string>("server"))
//                .Start();

//            await Task.WhenAll(Enumerable.Range(0, messageCount)
//                .InRandomOrder()
//                .Select(priority => SendPriMsg(clientBus, priority)));

//            serverBus.Advanced.Workers.SetNumberOfWorkers(1);

//            counter.WaitForResetEvent();

//            await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);

//            Assert.That(receivedMessagePriorities.Count, Is.EqualTo(messageCount));
//            Assert.That(receivedMessagePriorities.ToArray(), Is.EqualTo(Enumerable.Range(0, messageCount).Reverse().ToArray()));
//        }

//        static Task SendPriMsg(IBus clientBus, int priority) => clientBus.Send($"prioritet {priority}", new Dictionary<string, string>
//        {
//            { MongoDbTransport.MessagePriorityHeaderKey, priority.ToString()}
//        });
//    }
//}