using MongoDB.Bson;
using MongoDB.Driver;
using Nito.AsyncEx;
using Rebus.Extensions;
using Rebus.Messages;
using Rebus.MongoDb.Transport.Internals;
using Rebus.MongoDb.Transport.Model;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Rebus.MongoDb.Transport;

/// <summary>
/// Represents a message consumer for a specific queue
/// </summary>
class MongoDbMessageConsumer
{
    readonly AsyncSemaphore _semaphore;
    readonly MongoDbTransportConfiguration _config;

    /// <summary>
    /// Gets the name of the queue
    /// </summary>
    public string QueueName { get; }

    /// <summary>
    /// Creates the consumer from the given configuration and queue name
    /// </summary>
    public MongoDbMessageConsumer(MongoDbTransportConfiguration config, string queueName)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        QueueName = queueName ?? throw new ArgumentNullException(nameof(queueName));
        _semaphore = new AsyncSemaphore(_config.MaxParallelism);
    }

    /// <summary>
    /// Acknowledges having processed the message with the given <paramref name="messageId"/>.
    /// This will delete the message document from the underlying MongoDB collection.
    /// </summary>
    public async Task Ack(string messageId)
    {
        var collection = _config.Collection;

        using var @lock = await _semaphore.LockAsync();

        _ = await collection.DeleteOneAsync(doc => doc["_id"] == messageId);
    }

    /// <summary>
    /// Abandons the lease for the message with the given <paramref name="messageId"/>.
    /// This will set the <see cref="Fields.ReceiveTime"/> field of the message document to <see cref="DateTime.MinValue"/>.
    /// </summary>
    public async Task Nack(string messageId)
    {
        var collection = _config.Collection;

        var abandonUpdate = new BsonDocument
        {
            {"$set", new BsonDocument {{Fields.ReceiveTime, DateTime.MinValue}}}
        };

        using var @lock = await _semaphore.LockAsync();

        try
        {
            await collection.UpdateOneAsync(doc => doc["_id"] == messageId, new BsonDocumentUpdateDefinition<BsonDocument>(abandonUpdate));
        }
        catch
        {
            // lease will be released eventually
        }
    }

    public async Task Renew(string messageId)
    {
        var collection = _config.Collection;

        var renewUpdate = new BsonDocument
        {
            {"$set", new BsonDocument {{Fields.ReceiveTime, DateTime.UtcNow}}}
        };

        using var @lock = await _semaphore.LockAsync();

        try
        {
            await collection.UpdateOneAsync(doc => doc["_id"] == messageId, new BsonDocumentUpdateDefinition<BsonDocument>(renewUpdate));
        }
        catch
        {
            // lease will be released eventually
        }
    }

    /// <summary>
    /// Gets whether a message with the given ID exists
    /// </summary>
    public async Task<bool> Exists(string messageId)
    {
        var collection = _config.Collection;

        var criteria = new BsonDocument
        {
            {"_id", messageId }
        };

        using var @lock = await _semaphore.LockAsync();

        return await collection.CountDocumentsAsync(new BsonDocumentFilterDefinition<BsonDocument>(criteria)) > 0;
    }

    /// <summary>
    /// Loads the message with the given <paramref name="messageId"/>, returning null if it doesn't exist.
    /// </summary>
    public async Task<MongoDbReceivedMessage> LoadAsync(string messageId)
    {
        if (messageId == null) throw new ArgumentNullException(nameof(messageId));

        var collection = _config.Collection;

        using var @lock = await _semaphore.LockAsync();
        using var cursor = await collection.FindAsync(d => d["_id"] == messageId);

        var document = await cursor.FirstOrDefaultAsync();

        if (document == null) return null;

        return GetReceivedMessage(document);
    }

    /// <summary>
    /// Gets the next available message or immediately returns null if no message was available
    /// </summary>
    public async Task<MongoDbReceivedMessage> GetNextAsync()
    {
        var now = DateTime.UtcNow;

        var receiveTimeCriteria = new BsonDocument { { "$lt", now.Subtract(_config.DefaultMessageLease) } };

        var filter = new BsonDocumentFilterDefinition<BsonDocument>(new BsonDocument
        {
            {Fields.DestinationQueueName, QueueName},
            {Fields.ReceiveTime, receiveTimeCriteria},
            {Fields.DeliveryAttempts, new BsonDocument {{"$lt", _config.MaxDeliveryAttempts}}},
        });

        var update = new BsonDocumentUpdateDefinition<BsonDocument>(new BsonDocument
        {
            {"$set", new BsonDocument {{Fields.ReceiveTime, now}}},
            {"$inc", new BsonDocument {{Fields.DeliveryAttempts, 1}}}
        });

        var options = new FindOneAndUpdateOptions<BsonDocument>
        {
            ReturnDocument = ReturnDocument.After,
            Sort = new BsonDocument()
            {
                { Fields.ReceiveTime, 1 }
            }
        };

        var collection = _config.Collection;

        using var @lock = await _semaphore.LockAsync();

        var document = await collection.FindOneAndUpdateAsync(filter, update, options);

        if (document == null) return null;

        MongoDbReceivedMessage mongoDbReceivedMessage = GetReceivedMessage(document);
        if (await IsExpiredMessageAsync(mongoDbReceivedMessage))
        {
            return null;
        }
        return mongoDbReceivedMessage;
    }

    async Task<bool> IsExpiredMessageAsync(MongoDbReceivedMessage mongoDbReceivedMessage)
    {
        if (mongoDbReceivedMessage.Headers.TryGetValue(Headers.TimeToBeReceived, out string timeToBeReceived) && mongoDbReceivedMessage.Headers.TryGetValue(Headers.SentTime, out string sentTime))
        {
            DateTime expirationTime = sentTime.ToDateTimeOffset().UtcDateTime.Add(TimeSpan.Parse(timeToBeReceived));
            if (expirationTime < DateTime.UtcNow)
            {
                await Ack(mongoDbReceivedMessage.MessageId);
                return true;
            }
        }
        return false;
    }

    MongoDbReceivedMessage GetReceivedMessage(BsonValue document)
    {
        try
        {
            var body = document[Fields.Body].AsByteArray;

            var headers = document[Fields.Headers].AsBsonArray
                .ToDictionary(value => value[Fields.Key].AsString, value => value[Fields.Value].AsString);

            var id = document["_id"].AsString;

            var deliveryCount = document[Fields.DeliveryAttempts].AsInt32;

            var message = new MongoDbReceivedMessage(
                headers: headers, body: body,
                ack: () => Ack(id),
                nack: () => Nack(id),
                renew: () => Renew(id),
                deliveryCount: deliveryCount
            );
            return message;
        }
        catch (Exception exception)
        {
            throw new FormatException($"Could not read received BSON document: {document}", exception);
        }
    }
}