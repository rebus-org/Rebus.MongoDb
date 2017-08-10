using MongoDB.Driver;
using Rebus.DataBus;
using Rebus.MongoDb.DataBus;
using Rebus.Tests.Contracts.DataBus;

namespace Rebus.MongoDb.Tests.DataBus
{
    public class MongoDbDataBusStorageFactory : IDataBusStorageFactory
    {
        readonly IMongoDatabase _mongoDatabase;

        public MongoDbDataBusStorageFactory()
        {
            MongoTestHelper.DropMongoDatabase();

            _mongoDatabase = MongoTestHelper.GetMongoDatabase();
        }

        public IDataBusStorage Create()
        {
            return new MongoDbDataBusStorage(_mongoDatabase, "rbstest");
        }

        public void CleanUp()
        {
        }
    }
}