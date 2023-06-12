using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Sagas;
using Rebus.Tests.Contracts;
using Rebus.Transport.InMem;
#pragma warning disable CS1998

namespace Rebus.MongoDb.Tests.Examples;

[TestFixture]
[Explicit("Not actual tests. Just showing how to disable index creation.")]
public class DoNotCreateIndexes : FixtureBase
{
    [Test]
    public void ConfigureTransport()
    {
        using var activator = new BuiltinHandlerActivator();

        var mongoDbTransportOptions = new MongoDbTransportOptions(
            connectionString: MongoTestHelper.GetUrl(),
            automaticallyCreateIndex: false //< here's how to disable index creation
        );

        Configure.With(activator)
            .Transport(t => t.UseMongoDb(mongoDbTransportOptions, "some-queue"))
            .Create();
    }

    [Test]
    public async Task ConfigureSagaStorage()
    {
        using var activator = new BuiltinHandlerActivator();

        activator.Register(() => new SomeSaga());

        var bus = Configure.With(activator)
            .Transport(t => t.UseInMemoryTransport(new(), "some-queue"))
            .Sagas(s => s.StoreInMongoDb(
                mongoDatabase: MongoTestHelper.GetMongoDatabase(),
                automaticallyCreateIndexes: false //< here's how to disable index creation
            ))
            .Start();

        await bus.SendLocal("hej");

        await Task.Delay(millisecondsDelay: 2000);
    }

    class SomeSaga : Saga<MySagaData>, IAmInitiatedBy<string>
    {
        protected override void CorrelateMessages(ICorrelationConfig<MySagaData> config)
        {
            config.Correlate<string>(str => str, data => data.Id);
        }

        public async Task Handle(string message)
        {
        }
    }

    class MySagaData : SagaData { }
}