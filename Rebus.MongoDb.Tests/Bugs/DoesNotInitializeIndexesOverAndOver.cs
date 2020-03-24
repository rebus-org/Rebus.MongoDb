using System;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.MongoDb.Sagas;
using Rebus.Sagas;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Utilities;
// ReSharper disable ArgumentsStyleLiteral

namespace Rebus.MongoDb.Tests.Bugs
{
    [TestFixture]
    public class DoesNotInitializeIndexesOverAndOver : FixtureBase
    {
        [Test]
        public async Task VerifyThatInitializationOnlyOccursOnce()
        {
            var database = MongoTestHelper.GetMongoDatabase();
            var loggerFactory = new ListLoggerFactory(outputToConsole: true);
            var storage = new MongoDbSagaStorage(database, loggerFactory);

            storage.Initialize();

            await storage.Insert(new SomeSagaData { Id = Guid.NewGuid() }, Enumerable.Empty<ISagaCorrelationProperty>());
            await storage.Insert(new SomeSagaData { Id = Guid.NewGuid() }, Enumerable.Empty<ISagaCorrelationProperty>());
            await storage.Insert(new SomeSagaData { Id = Guid.NewGuid() }, Enumerable.Empty<ISagaCorrelationProperty>());

            var numberOfInitializations = loggerFactory
                .Count(line => line.Text.Contains("Initializing index for saga data"));

            Assert.That(numberOfInitializations, Is.EqualTo(1), 
                "Only expected the collection's indexes to be initialized once!");
        }

        class SomeSagaData : SagaData { }
    }
}