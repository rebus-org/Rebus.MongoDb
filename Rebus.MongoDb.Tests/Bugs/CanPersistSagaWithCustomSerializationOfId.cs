using System;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.MongoDb.Tests.Extensions;
using Rebus.Sagas;
using Rebus.Tests.Contracts;
using Rebus.Transport.InMem;
#pragma warning disable 1998

namespace Rebus.MongoDb.Tests.Bugs
{
    [TestFixture]
    public class CanPersistSagaWithCustomSerializationOfId : FixtureBase
    {
        static CanPersistSagaWithCustomSerializationOfId()
        {
            // this line affects all tests, so we comment it out to be enabled explicitly
            //BsonSerializer.RegisterSerializer(typeof(Guid), new GuidSerializer(BsonType.String));
        }

        IMongoDatabase _database;

        protected override void SetUp()
        {
            MongoTestHelper.DropMongoDatabase();
            _database = MongoTestHelper.GetMongoDatabase();

        }

        protected override void TearDown()
        {
            MongoTestHelper.DropMongoDatabase();
        }

        [TestCase(1)]
        [TestCase(5)]
        public async Task ItWorks(int maxParallelism)
        {
            var activator = new BuiltinHandlerActivator();
            
            Using(activator);

            activator.Register(() => new SomeSaga());

            var bus = Configure.With(activator)
                .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "saga-test"))
                .Sagas(s => s.StoreInMongoDb(_database))
                .Options(o => o.SetMaxParallelism(maxParallelism))
                .Start();

            var correlationId = Guid.NewGuid().ToString();

            await bus.SendLocal(new IncreaseCount(correlationId));
            await bus.SendLocal(new IncreaseCount(correlationId));
            await bus.SendLocal(new IncreaseCount(correlationId));

            var collection = _database.GetCollection<SomeSagaData>(nameof(SomeSagaData));

            await collection.WaitFor(c => c.Count(d => true) >= 1);
            await Task.Delay(TimeSpan.FromSeconds(1));
            
            var docs = collection.Find(x => true).ToList();

            Assert.That(docs.Count, Is.EqualTo(1));

            var data = docs.First();

            Assert.That(data.MessageCount, Is.EqualTo(3));
        }
        
        class SomeSagaData : SagaData
        {
            public string CorrelationId { get; set; }
            public int MessageCount { get; set; }
        }

        class SomeSaga : Saga<SomeSagaData>, IAmInitiatedBy<IncreaseCount>
        {
            protected override void CorrelateMessages(ICorrelationConfig<SomeSagaData> config)
            {
                config.Correlate<IncreaseCount>(m => m.CorrelationId, d => d.CorrelationId);
            }

            public async Task Handle(IncreaseCount message)
            {
                Data.MessageCount++;
            }
        }

        class IncreaseCount
        {
            public IncreaseCount(string correlationId)
            {
                CorrelationId = correlationId;
            }

            public string CorrelationId { get;  }
        }
    }
}