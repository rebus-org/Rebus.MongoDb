﻿using NUnit.Framework;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.MongoDb.Transport;
using Rebus.Tests.Contracts;
using Rebus.Threading.TaskParallelLibrary;
using Rebus.Time;
using Rebus.Transport;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.MongoDb.Tests.Transport
{
    [TestFixture, Category(MongoTestHelper.TestCategory)]
    public class TestMongoDbTransportMessageOrdering : FixtureBase
    {
        private const string QueueName = "test-ordering";
        protected override void SetUp() => MongoTestHelper.DropMongoDatabase();

        [TestCase]
        public async Task DeliversMessagesByVisibleTimeAndNotBeInsertionTime()
        {
            var transport = GetTransport();

            var now = DateTime.Now;

            await PutInQueue(transport, GetTransportMessage("first message"));
            await PutInQueue(transport, GetTransportMessage("second message", deferredUntilTime: now.AddMinutes(-1)));
            await PutInQueue(transport, GetTransportMessage("third message", deferredUntilTime: now.AddMinutes(-2)));

            var firstMessage = await ReceiveMessageBody(transport);
            var secondMessage = await ReceiveMessageBody(transport);
            var thirdMessage = await ReceiveMessageBody(transport);

            // expect messages to be received in reverse order because of their visible times
            Assert.That(firstMessage, Is.EqualTo("third message"));
            Assert.That(secondMessage, Is.EqualTo("second message"));
            Assert.That(thirdMessage, Is.EqualTo("first message"));
        }

        private static async Task<string> ReceiveMessageBody(ITransport transport)
        {
            using (var scope = new RebusTransactionScope())
            {
                var transportMessage = await transport.Receive(scope.TransactionContext, CancellationToken.None);

                if (transportMessage == null)
                {
                    return null;
                }

                var body = Encoding.UTF8.GetString(transportMessage.Body);

                await scope.CompleteAsync();

                return body;
            }
        }

        public enum TransportType
        {
            LockedRows,
            LeaseBased,
        }

        private static TransportMessage GetTransportMessage(string body, DateTime? deferredUntilTime = null)
        {
            var headers = new Dictionary<string, string>
            {
                {Headers.MessageId, Guid.NewGuid().ToString()}
            };

            if (deferredUntilTime != null)
            {
                headers[Headers.DeferredRecipient] = QueueName;
                headers[Headers.DeferredUntil] = deferredUntilTime.Value.ToString("o");
            }

            return new TransportMessage(headers, Encoding.UTF8.GetBytes(body));
        }

        private static async Task PutInQueue(ITransport transport, TransportMessage transportMessage)
        {
            using (var scope = new RebusTransactionScope())
            {
                await transport.Send(QueueName, transportMessage, scope.TransactionContext);
                await scope.CompleteAsync();
            }
        }

        private static MongoDbTransport GetTransport()
        {
            var rebusTime = new DefaultRebusTime();
            var loggerFactory = new ConsoleLoggerFactory(false);

            var asyncTaskFactory = new TplAsyncTaskFactory(loggerFactory);
            var config = new MongoDbTransportOptions(MongoTestHelper.GetUrl()).SetInputQueueName(QueueName);

            var transport = new MongoDbTransport(
                    loggerFactory,
                    asyncTaskFactory,
                    rebusTime,
                    config
                );

            transport.EnsureCollectionIsCreated();
            transport.Initialize();

            return transport;
        }
    }
}
