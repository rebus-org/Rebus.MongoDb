using MongoDB.Driver;
using Rebus.Logging;
using Rebus.MongoDb.Sagas;
using Rebus.Sagas;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.MongoDb.Tests.Sagas
{
    public class TestMongoDbSagaStorage : ISagaStorageFactory
    {
        IMongoDatabase _mongoDatabase;

        public ISagaStorage GetSagaStorage()
        {
            _mongoDatabase = MongoTestHelper.GetMongoDatabase();

            return new MongoDbSagaStorage(_mongoDatabase, new ConsoleLoggerFactory(colored: false));
        }

        public void CleanUp()
        {
            MongoTestHelper.DropMongoDatabase();
        }
    }
}
