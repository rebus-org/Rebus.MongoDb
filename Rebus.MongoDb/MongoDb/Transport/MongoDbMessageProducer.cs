using MongoDB.Bson;
using MongoDB.Driver;
using Nito.AsyncEx;
using Rebus.Extensions;
using Rebus.Messages;
using Rebus.MongoDb.Transport.Exceptions;
using Rebus.MongoDb.Transport.Internals;
using Rebus.Transport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Rebus.MongoDb.Transport;

/// <summary>
/// Represents a message producer
/// </summary>
class MongoDbMessageProducer
{
    readonly AsyncSemaphore _semaphore;
    readonly MongoDbTransportConfiguration _config;

    /// <summary>
    /// Creates the producer from the given configuration
    /// </summary>
    public MongoDbMessageProducer(MongoDbTransportConfiguration config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _semaphore = new AsyncSemaphore(_config.MaxParallelism);
    }

    /// <summary>
    /// Sends messages to queues without any transactional integrity
    /// </summary>
    /// <param name="outgoingTransportMessages"></param>
    /// <returns></returns>
    public async Task SendOutgoingMessagesWithoutTransaction(IEnumerable<OutgoingTransportMessage> outgoingTransportMessages) => await SendManyAsync(outgoingTransportMessages.Select(
        (outgoingTransportMessage) => CreateMessageDocument(GetDestinationAddressToUse(outgoingTransportMessage.DestinationAddress, outgoingTransportMessage.TransportMessage), outgoingTransportMessage.TransportMessage)));


    /// <summary>
    /// Sends messages to queues
    /// </summary>
    async Task SendManyAsync(IEnumerable<BsonDocument> messageDocuments)
    {
        using var @lock = await _semaphore.LockAsync();

        try
        {
            await _config.Collection.InsertManyAsync(messageDocuments);
        }
        catch (MongoBulkWriteException exception)
        {
            var uniqueMessageIdExceptions = exception.WriteErrors
                .Where(error => error.Category == ServerErrorCategory.DuplicateKey)
                .Select(error => new UniqueMessageIdViolationException(messageDocuments.Skip(error.Index).First().GetElement("_id").Value.AsString));
            
            if (uniqueMessageIdExceptions.Any())
            {
                if (uniqueMessageIdExceptions.First() == uniqueMessageIdExceptions.Last())
                {
                    throw uniqueMessageIdExceptions.First();
                }
                else
                {
                    throw new AggregateException("Messages sent were not unique", uniqueMessageIdExceptions);
                }
            }
            throw;
        }
    }

    BsonDocument CreateMessageDocument(string destinationQueueName, TransportMessage message)
    {
        if (destinationQueueName == null) throw new ArgumentNullException(nameof(destinationQueueName));
        if (message == null) throw new ArgumentNullException(nameof(message));

        if (!message.Headers.TryGetValue(Fields.MessageId, out string id))
        {
            id = Guid.NewGuid().ToString();
            message.Headers[Fields.MessageId] = id;
        }

        var receiveTime = DateTime.MinValue;
        
        if (message.Headers.TryGetValue(Headers.DeferredUntil, out string deferredUntil))
        {
            receiveTime = deferredUntil.ToDateTimeOffset().DateTime.Subtract(_config.DefaultMessageLease);
        }

        var headers = BsonArray.Create(message.Headers
            .Select(kvp => new BsonDocument { { Fields.Key, kvp.Key }, { Fields.Value, kvp.Value } }));

        return new BsonDocument
        {
            {"_id", id},
            {Fields.DestinationQueueName, destinationQueueName},
            {Fields.SendTime, DateTime.UtcNow},
            {Fields.DeliveryAttempts, 0},
            {Fields.ReceiveTime, receiveTime},
            {Fields.Headers, headers},
            {Fields.Body, BsonBinaryData.Create(message.Body)}
        };
    }

    /// <summary>
    /// Gets the address a message will actually be sent to. Handles deferred messsages.
    /// </summary>
    static string GetDestinationAddressToUse(string destinationAddress, TransportMessage message)
    {
        return string.Equals(destinationAddress, MongoDbTransport.MagicExternalTimeoutManagerAddress, StringComparison.CurrentCultureIgnoreCase)
            ? GetDeferredRecipient(message)
            : destinationAddress;
    }

    static string GetDeferredRecipient(TransportMessage message)
    {
        if (message.Headers.TryGetValue(Headers.DeferredRecipient, out var destination))
        {
            return destination;
        }

        throw new InvalidOperationException($"Attempted to defer message, but no '{Headers.DeferredRecipient}' header was on the message");
    }
}