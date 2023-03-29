using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Rebus.Subscriptions;

namespace Rebus.MongoDb.Subscriptions;

/// <summary>
/// Implementation of <see cref="ISubscriptionStorage"/> that uses MongoDB to store subscriptions
/// </summary>
public class MongoDbSubscriptionStorage : ISubscriptionStorage
{
    static readonly string[] NoSubscribers = Array.Empty<string>();

    readonly IMongoCollection<BsonDocument> _subscriptions;

    /// <summary>
    /// Creates the subscription storage
    /// </summary>
    public MongoDbSubscriptionStorage(IMongoDatabase database, string collectionName, bool isCentralized)
    {
        if (database == null) throw new ArgumentNullException(nameof(database));
        if (collectionName == null) throw new ArgumentNullException(nameof(collectionName));
        _subscriptions = database.GetCollection<BsonDocument>(collectionName);
        IsCentralized = isCentralized;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> GetSubscriberAddresses(string topic)
    {
        var doc = await _subscriptions.Find(new BsonDocument("_id", topic)).FirstOrDefaultAsync().ConfigureAwait(false);

        if (doc == null) return NoSubscribers;

        return doc["addresses"].AsBsonArray
            .Select(item => item.ToString())
            .ToArray();
    }

    /// <inheritdoc />
    public async Task RegisterSubscriber(string topic, string subscriberAddress)
    {
        var document = new BsonDocument("_id", topic);
        var update = Builders<BsonDocument>.Update.AddToSet("addresses", subscriberAddress);
        var options = new UpdateOptions { IsUpsert = true };

        await _subscriptions
            .UpdateOneAsync(document, update, options)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task UnregisterSubscriber(string topic, string subscriberAddress)
    {
        var document = new BsonDocument("_id", topic);
        var update = Builders<BsonDocument>.Update.Pull("addresses", subscriberAddress);
        var options = new UpdateOptions { IsUpsert = true };

        await _subscriptions
            .UpdateOneAsync(document, update, options)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public bool IsCentralized { get; }
}