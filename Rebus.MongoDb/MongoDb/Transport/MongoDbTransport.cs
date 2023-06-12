using Rebus.Config;
using Rebus.Messages;
using Rebus.Transport;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Logging;
using System;

namespace Rebus.MongoDb.Transport;

public class MongoDbTransport : AbstractRebusTransport
{
    readonly MongoDbMessageConsumer _mongoDbMessageConsumer;
    readonly MongoDbMessageProducer _mongoDbMessageProducer;
    readonly ILog _logger;

    /// <summary>
    /// When a message is sent to this address, it will be deferred into the future!
    /// </summary>
    public const string MagicExternalTimeoutManagerAddress = "##### MagicExternalTimeoutManagerAddress #####";

    public MongoDbTransport(IRebusLoggerFactory rebusLoggerFactory, string inputQueueName, MongoDbTransportOptions mongoDbTransportOptions) : base(inputQueueName)
    {
        _logger = rebusLoggerFactory.GetLogger<MongoDbTransport>();
        try
        {
            var connectionString = mongoDbTransportOptions.ConnectionString;
            
            var configuration = new MongoDbTransportConfiguration(
                mongoUrl: connectionString,
                collectionName: mongoDbTransportOptions.CollectionName,
                automaticallyCreateIndex: mongoDbTransportOptions.AutomaticallyCreateIndex
            );

            _mongoDbMessageProducer = configuration.CreateProducer();

            if (!string.IsNullOrWhiteSpace(Address))
            {
                _mongoDbMessageConsumer = configuration.CreateConsumer(Address);
            }
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "MongoDB transport creation failed.");
            throw;
        }
    }

    public override void CreateQueue(string address)
    {
        // queues do not have to be created with the MongoDB transport
    }

    public override async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
    {
        if (_mongoDbMessageConsumer != null)
        {
            var receivedMessage = await _mongoDbMessageConsumer.GetNextAsync();
            if (receivedMessage != null && cancellationToken.IsCancellationRequested)
            {
                await receivedMessage.Nack();
                return null;
            }
            if (receivedMessage != null && context != null)
            {
                context.OnAck(_ => receivedMessage.Ack());
                context.OnNack(_ => receivedMessage.Nack());
            }
            return receivedMessage;
        }
        else
        {
            _logger.Debug("Receive will always return null because no input queue has been specified.");
        }
        return null;
    }

    protected override async Task SendOutgoingMessages(IEnumerable<OutgoingTransportMessage> outgoingMessages, ITransactionContext context)
    {
        await _mongoDbMessageProducer.SendOutgoingMessagesWithoutTransaction(outgoingMessages);
    }
}