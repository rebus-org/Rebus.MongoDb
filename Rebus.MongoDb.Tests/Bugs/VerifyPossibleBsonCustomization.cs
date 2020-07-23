using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Bson.Serialization.Options;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Logging;
using Rebus.MongoDb.Tests.Helpers;
using Rebus.Sagas;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Utilities;
using Rebus.Transport.InMem;

#pragma warning disable 1998

namespace Rebus.MongoDb.Tests.Bugs
{
    [TestFixture]
    public class VerifyPossibleBsonCustomization : FixtureBase
    {
        [Test]
        public async Task VerifyNiceExceptionWhenSerializerHasBeenImproperlyCustomized()
        {
            if (BsonClassMap.IsClassMapRegistered(typeof(SagaData)))
            {
                MongoDbTestHelper.BsonClassMapHelper.Unregister<SagaData>();
            }
            BsonClassMap.RegisterClassMap<SagaData>(c =>
            {
                c.MapField(d => d.Id);
            });


            var activator = Using(new BuiltinHandlerActivator());
            var listLoggerFactory = new ListLoggerFactory();

            activator.Register(() => new SomeSaga());

            Configure.With(activator)
                .Logging(l => l.Use(listLoggerFactory))
                .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "who-cares"))
                .Sagas(s => s.StoreInMongoDb(MongoTestHelper.GetMongoDatabase()))
                .Start();

            await activator.Bus.SendLocal("hej med dig!");

            await Task.Delay(TimeSpan.FromSeconds(2));

            var error = listLoggerFactory.FirstOrDefault(l => l.Level == LogLevel.Error)
                ?? throw new AssertionException("Did not find any log entries with Level == Error");

            Console.WriteLine($@"----------------------------------------------------------------------------------------------
Found this:

{error.Text}
----------------------------------------------------------------------------------------------");

            var message = error.Text;

            Assert.That(message, Contains.Substring("_id"));
            Assert.That(message, Contains.Substring("Id"));
        }

        class DictionaryRepresentationConvention : ConventionBase, IMemberMapConvention
        {
            readonly DictionaryRepresentation _dictionaryRepresentation;

            public DictionaryRepresentationConvention(DictionaryRepresentation dictionaryRepresentation) => _dictionaryRepresentation = dictionaryRepresentation;

            public void Apply(BsonMemberMap memberMap) => memberMap.SetSerializer(ConfigureSerializer(memberMap.GetSerializer()));

            IBsonSerializer ConfigureSerializer(IBsonSerializer serializer)
            {
                if (serializer is IDictionaryRepresentationConfigurable dictionaryRepresentationConfigurable)
                {
                    serializer = dictionaryRepresentationConfigurable.WithDictionaryRepresentation(_dictionaryRepresentation);
                }
                var childSerializerConfigurable = serializer as IChildSerializerConfigurable;

                return childSerializerConfigurable == null
                    ? serializer
                    : childSerializerConfigurable.WithChildSerializer(ConfigureSerializer(childSerializerConfigurable.ChildSerializer));
            }
        }

        class SomeSaga : Saga<SomeSagaData>, IAmInitiatedBy<string>
        {
            protected override void CorrelateMessages(ICorrelationConfig<SomeSagaData> config)
            {
                config.Correlate<string>(str => str, d => d.Id);
            }

            public async Task Handle(string message) => throw new System.NotImplementedException("should not get this far");
        }

        class SomeSagaData : SagaData
        {
            public string Text { get; set; }

            public Dictionary<string, SomeItem> Items { get; set; }
        }

        class SomeItem { }
    }
}