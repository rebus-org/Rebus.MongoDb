using System;
using System.Threading.Tasks;
using MongoDB.Driver;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Sagas;
using Rebus.Tests.Contracts;
using Rebus.Transport.InMem;

namespace Rebus.MongoDb.Tests.Bugs
{
    [TestFixture]
    public class DoesNotFailWhenCorrelatingOnId : FixtureBase
    {
        protected override void SetUp() => MongoTestHelper.DropMongoDatabase();

        [Test]
        public async Task ItWorks()
        {
            var activator = new BuiltinHandlerActivator();

            Using(activator);

            activator.Register(() => new SomeSaga());

            var database = MongoTestHelper.GetMongoDatabase();

            Configure.With(activator)
                .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "bimse"))
                .Sagas(s => s.StoreInMongoDb(database))
                .Start();

            await activator.Bus.SendLocal("hej med dig min ven!");
            await activator.Bus.SendLocal("hej igen min ven!");

            await Task.Delay(TimeSpan.FromSeconds(3));

            var collection = database.GetCollection<SomeSagaData>(nameof(SomeSagaData));

            var data = await collection.Find(d => true).ToListAsync();

            Assert.That(data.Count, Is.EqualTo(2),
                "The second insert would fail, because there would be an index with a unique constraint on 'Id', but since the Id properti is mapped to '_id', values in that index would be null");
        }

        class SomeSagaData : SagaData
        {

        }

        class SomeSaga : Saga<SomeSagaData>, IAmInitiatedBy<string>
        {
            protected override void CorrelateMessages(ICorrelationConfig<SomeSagaData> config)
            {
                config.Correlate<string>(str => str, d => d.Id);
            }

            public Task Handle(string message)
            {
                return Task.CompletedTask;
            }
        }
    }
}