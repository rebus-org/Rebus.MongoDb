using NUnit.Framework;
using Rebus.Tests.Contracts.Timeouts;

namespace Rebus.MongoDb.Tests.Timeouts
{
    [TestFixture, Category(MongoTestHelper.TestCategory)]
    public class TestMongoDbTimeoutManager : BasicStoreAndRetrieveOperations<MongoDbTimeoutManagerFactory>
    {
    }
}