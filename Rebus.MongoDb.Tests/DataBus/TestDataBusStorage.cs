using NUnit.Framework;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.DataBus;

namespace Rebus.MongoDb.Tests.DataBus
{
    [TestFixture]
    public class TestDataBusStorage : GeneralDataBusStorageTests<MongoDbDataBusStorageFactory>
    {
        
    }
}