using MongoDB.Driver;
using Rebus.MongoDb.Transport;

namespace Rebus.Config;

/// <summary>
/// Describes options used to configure the <seealso cref="MongoDbTransport"/>
/// </summary>
public class MongoDbTransportOptions
{
    /// <summary>
    /// Indicates the default collection name to use for storing messages. This is not the same as "queue names", as
    /// one single collection can be used for multiple logical queues and must be shared between Rebus instances that must be
    /// able to communication with each other.
    /// </summary>
    public const string DefaultCollectionName = "messages";

    /// <summary>
    /// Indicates that indexes are created by default.
    /// </summary>
    public const bool DefaultAutomaticallyCreateIndex = true;

    /// <summary>
    /// Creates an instance of the transport connecting via <paramref name="connectionString"/>
    /// </summary>
    public MongoDbTransportOptions(
        string connectionString,
        string collectionName = DefaultCollectionName,
        bool automaticallyCreateIndex = DefaultAutomaticallyCreateIndex)
        : this(new MongoUrl(connectionString), collectionName, automaticallyCreateIndex)
    {
    }

    /// <summary>
    /// Creates an instance of transport connection
    /// </summary>
    public MongoDbTransportOptions(MongoUrl connectionString, string collectionName = DefaultCollectionName, bool automaticallyCreateIndex = DefaultAutomaticallyCreateIndex)
    {
        ConnectionString = connectionString;
        CollectionName = collectionName;
        AutomaticallyCreateIndex = automaticallyCreateIndex;
    }


    internal MongoUrl ConnectionString { get; set; }

    internal string CollectionName { get; set; }

    internal bool AutomaticallyCreateIndex { get; }
}