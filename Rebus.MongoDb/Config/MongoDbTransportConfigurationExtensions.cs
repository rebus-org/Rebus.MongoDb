using Rebus.Injection;
using Rebus.Logging;
using Rebus.MongoDb;
using Rebus.MongoDb.Transport;
using Rebus.Pipeline;
using Rebus.Pipeline.Receive;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Timeouts;
using Rebus.Transport;

namespace Rebus.Config;

/// <summary>
/// Configuration extensions for the MongoDb transport
/// </summary>
public static class MongoDbTransportConfigurationExtensions
{
    /// <summary>
    /// Configures Rebus to use MongoDb as its transport
    /// </summary>
    /// <param name="configurer">Static to extend</param>
    /// <param name="transportOptions">Options controlling the transport setup</param>
    /// <param name="inputQueueName">Queue name to process messages from</param>
    public static MongoDbTransportOptions UseMongoDb(
        this StandardConfigurer<ITransport> configurer,
        MongoDbTransportOptions transportOptions,
        string inputQueueName)
    {
        if (!string.IsNullOrEmpty(inputQueueName))
        {
            transportOptions.SetInputQueueName(inputQueueName);
        }
        return Configure(
                configurer,
                (context) => new MongoDbTransport(
                    context.Get<IRebusLoggerFactory>(),
                    transportOptions),
                transportOptions
            )
            .ReadFrom(inputQueueName);
    }

    /// <summary>
    /// Configures Rebus to use MongoDb as its transport in "one-way client mode" (i.e. as a send-only endpoint). 
    /// </summary>
    /// <param name="configurer"></param>
    /// <param name="transportOptions"></param>
    /// <returns></returns>
    public static MongoDbTransportOptions UseMongoDbAsOneWayClient(
        this StandardConfigurer<ITransport> configurer, MongoDbTransportOptions transportOptions)
    {
        return Configure(
                configurer,
                (context) => new MongoDbTransport(
                    context.Get<IRebusLoggerFactory>(),
                    transportOptions),
                transportOptions
            )
            .AsOneWayClient();
    }

    delegate MongoDbTransport TransportFactoryDelegate(
        IResolutionContext context);

    static TTransportOptions Configure<TTransportOptions>(
        StandardConfigurer<ITransport> configurer,
        TransportFactoryDelegate transportFactory,
        TTransportOptions transportOptions) where TTransportOptions : MongoDbTransportOptions
    {
        configurer.Register(context =>
            {
                if (transportOptions.IsOneWayQueue)
                {
                    OneWayClientBackdoor.ConfigureOneWayClient(configurer);
                }
                var transport = transportFactory(context);

                return transport;
            }
        );

        configurer.OtherService<ITimeoutManager>().Register(c => new DisabledTimeoutManager(),
            @"A timeout manager cannot be explicitly configured when using MongoDb as the
transport. This is because because the MongoDb transport has built-in deferred 
message capabilities, and therefore it is not necessary to configure anything 
else to be able to delay message delivery.");

        configurer.OtherService<IPipeline>().Decorate(c =>
        {
            var pipeline = c.Get<IPipeline>();

            return new PipelineStepRemover(pipeline)
                .RemoveIncomingStep(s => s.GetType() == typeof(HandleDeferredMessagesStep));
        });

        configurer.OtherService<Options>().Decorate(c =>
        {
            var options = c.Get<Options>();

            if (string.IsNullOrWhiteSpace(options.ExternalTimeoutManagerAddressOrNull))
            {
                options.ExternalTimeoutManagerAddressOrNull = MongoDbTransport.MagicExternalTimeoutManagerAddress;
            }

            return options;
        });

        return transportOptions;
    }
}