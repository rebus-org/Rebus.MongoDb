using Rebus.Logging;
using Rebus.MongoDb.Transport;
using Rebus.Tests.Contracts.Transports;
using Rebus.Transport;

namespace Rebus.MongoDb.Tests.Transport.Contracts.Factories;

public class MongoDbTransportFactory : ITransportFactory
{
    public MongoDbTransportFactory()
    {
        MongoTestHelper.DropMongoDatabase();
    }

    public ITransport CreateOneWayClient()
    {
        var consoleLoggerFactory = new ConsoleLoggerFactory(false);
        var transport = new MongoDbTransport(consoleLoggerFactory, null, new Config.MongoDbTransportOptions(MongoTestHelper.GetUrl()));

        return transport;
    }

    public ITransport Create(string inputQueueAddress)
    {
        var consoleLoggerFactory = new ConsoleLoggerFactory(false);

        var transport = new MongoDbTransport(consoleLoggerFactory, inputQueueAddress,
            new Config.MongoDbTransportOptions(MongoTestHelper.GetUrl()));

        return transport;
    }

    public void CleanUp()
    {
    }
}