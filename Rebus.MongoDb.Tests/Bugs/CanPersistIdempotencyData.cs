using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using NUnit.Framework;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.MongoDb.Sagas;
using Rebus.Sagas;
using Rebus.Sagas.Idempotent;
using Rebus.Tests.Contracts;

namespace Rebus.MongoDb.Tests.Bugs
{
    [TestFixture]
    public class CanPersistIdempotencyData : FixtureBase
    {
        MongoDbSagaStorage _sagaStorage;
        static readonly IEnumerable<ISagaCorrelationProperty> None = Enumerable.Empty<ISagaCorrelationProperty>();

        protected override void SetUp()
        {
            var database = MongoTestHelper.GetMongoDatabase();
            
            database.DropCollection(nameof(MyIdempotentSaga));
            
            _sagaStorage = new MongoDbSagaStorage(database, new ConsoleLoggerFactory(colored: false));
            _sagaStorage.Initialize();
        }

        [Test]
        public async Task CanRoundtripIdempotencyData()
        {
            var id = Guid.NewGuid();
            var data = GetSagaData(id);

            await _sagaStorage.Insert(data, None);

            Console.WriteLine($@"Here's the original saga data:

{data.ToPrettyJson()}

Here's what the BSON serializer thinks it looks like:

{BsonSerialize(data)}");

            var roundtrippedData = (MyIdempotentSaga)await _sagaStorage.Find(typeof(MyIdempotentSaga), nameof(ISagaData.Id), id);

            Assert.That(roundtrippedData.Name, Is.EqualTo("joe"));

            var idempotencyData = roundtrippedData.IdempotencyData;

            Assert.That(idempotencyData.HandledMessageIds, Contains.Item("id1"));
            Assert.That(idempotencyData.HandledMessageIds, Contains.Item("id2"));
            Assert.That(idempotencyData.OutgoingMessages.Count, Is.EqualTo(1));

            var outgoingMessagesForFirstMessage = idempotencyData.OutgoingMessages.FirstOrDefault(m => m.MessageId == "id1");

            Assert.That(outgoingMessagesForFirstMessage, Is.Not.Null, $@"Did not find the expected outgoing message in this piece of saga data:

{roundtrippedData.ToPrettyJson()}");

            var outgoingMessages = outgoingMessagesForFirstMessage.MessagesToSend.ToList();

            Assert.That(outgoingMessages.Count, Is.EqualTo(1));

            var outgoingMessage = outgoingMessages.Single();

            Assert.That(outgoingMessage.DestinationAddresses, Contains.Item("address1"));
            Assert.That(outgoingMessage.TransportMessage.Headers, Contains.Key(Headers.MessageId).And.ContainValue("id1"));
        }

        [Test]
        //[Ignore("Cannot be executed if the maps have already been registered")]
        public void CanRoundtripSagaData()
        {
            if (!BsonClassMap.IsClassMapRegistered(typeof(IdempotencyData)))
            {
                BsonClassMap.RegisterClassMap<IdempotencyData>(map =>
                {
                    map.MapCreator(obj => new IdempotencyData(obj.OutgoingMessages, obj.HandledMessageIds));
                    map.MapMember(obj => obj.HandledMessageIds);
                    map.MapMember(obj => obj.OutgoingMessages);
                });

                BsonClassMap.RegisterClassMap<OutgoingMessage>(map =>
                {
                    map.MapCreator(obj => new OutgoingMessage(obj.DestinationAddresses, obj.TransportMessage));
                    map.MapMember(obj => obj.DestinationAddresses);
                    map.MapMember(obj => obj.TransportMessage);
                });

                BsonClassMap.RegisterClassMap<OutgoingMessages>(map =>
                {
                    map.MapCreator(obj => new OutgoingMessages(obj.MessageId, obj.MessagesToSend));
                    map.MapMember(obj => obj.MessageId);
                    map.MapMember(obj => obj.MessagesToSend);
                });

                BsonClassMap.RegisterClassMap<TransportMessage>(map =>
                {
                    map.MapCreator(obj => new TransportMessage(obj.Headers, obj.Body));
                    map.MapMember(obj => obj.Headers);
                    map.MapMember(obj => obj.Body);
                });
            }

            var id = Guid.NewGuid();
            var sagaData = GetSagaData(id);

            var bsonDocument = new BsonDocument();
            BsonSerializer.Serialize(new BsonDocumentWriter(bsonDocument), sagaData);

            Console.WriteLine($@"Here's the document:

{bsonDocument.ToJson()}

");

            var roundtrippedSagaData = BsonSerializer.Deserialize<MyIdempotentSaga>(bsonDocument);

            var roundtrippedBsonDocument = new BsonDocument();
            BsonSerializer.Serialize(new BsonDocumentWriter(roundtrippedBsonDocument), roundtrippedSagaData);

            Console.WriteLine($@"Here's what was preserved after roundtripping:

{roundtrippedBsonDocument.ToJson()}");

            Assert.That(roundtrippedSagaData.IdempotencyData.HandledMessageIds, Contains.Item("id1"));
            Assert.That(roundtrippedSagaData.IdempotencyData.HandledMessageIds, Contains.Item("id2"));
        }

        static string BsonSerialize(object obj)
        {
            var target = new BsonDocument();
            BsonSerializer.Serialize(new BsonDocumentWriter(target), obj);
            return target.ToJson();
        }

        static MyIdempotentSaga GetSagaData(Guid id)
        {
            var headers = new Dictionary<string, string> { { Headers.MessageId, "id1" } };
            var body = new byte[] { 1, 2, 3 };
            var message1 = new TransportMessage(headers, body);

            var data = new MyIdempotentSaga
            {
                Id = id,
                Name = "joe",
                IdempotencyData = new IdempotencyData
                {
                    HandledMessageIds = { "id1", "id2" },
                    OutgoingMessages =
                    {
                        new OutgoingMessages("id1", new[] {new OutgoingMessage(new[] {"address1"}, message1)})
                    }
                }
            };
            return data;
        }

        class MyIdempotentSaga : IdempotentSagaData
        {
            public string Name { get; set; }
        }
    }
}