using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;

namespace Rebus.MongoDb.Tests.Bugs;

[TestFixture]
public class CanPublishEventToTwoSubscribers : FixtureBase
{
    [Test]
    public async Task CanDoIt()
    {
        using var eventReceivedBySub1 = new ManualResetEvent(initialState: false);
        using var eventReceivedBySub2 = new ManualResetEvent(initialState: false);

        using var sub1 = CreateBus("subscriber1", activator => activator.Handle<ThisIsTheEvent>(async _ => eventReceivedBySub1.Set()));
        using var sub2 = CreateBus("subscriber2", activator => activator.Handle<ThisIsTheEvent>(async _ => eventReceivedBySub2.Set()));

        using var publisher = CreateBus("publisher");

        await sub1.Subscribe<ThisIsTheEvent>();
        await sub2.Subscribe<ThisIsTheEvent>();

        // stop these two to force the event to be distributed in two copies that must be able to co-exist in the "Messages" collection
        sub1.Advanced.Workers.SetNumberOfWorkers(0);
        sub2.Advanced.Workers.SetNumberOfWorkers(0);

        await publisher.Publish(new ThisIsTheEvent());

        // now start these two...
        sub1.Advanced.Workers.SetNumberOfWorkers(1);
        sub2.Advanced.Workers.SetNumberOfWorkers(1);

        // ...and wait for the events
        eventReceivedBySub1.WaitOrDie(TimeSpan.FromSeconds(3));
        eventReceivedBySub2.WaitOrDie(TimeSpan.FromSeconds(3));
    }

    record ThisIsTheEvent;

    IBus CreateBus(string queueName, Action<BuiltinHandlerActivator> handlers = null)
    {
        var activator = new BuiltinHandlerActivator();
        handlers?.Invoke(activator);
        return Configure.With(activator)
            .Transport(t => t.UseMongoDb(new(MongoTestHelper.GetUrl(), collectionName: "Messages"), queueName))
            .Subscriptions(s => s.StoreInMongoDb(MongoTestHelper.GetMongoDatabase(), collectionName: "Subscriptions", isCentralized: true))
            .Start();
    }
}