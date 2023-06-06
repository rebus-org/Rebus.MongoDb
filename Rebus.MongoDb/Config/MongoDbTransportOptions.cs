using MongoDB.Driver;
using Rebus.MongoDb.Transport;

namespace Rebus.Config;

/// <summary>
/// Describes options used to configure the <seealso cref="MongoDbTransport"/>
/// </summary>
public class MongoDbTransportOptions
{
   private const string DefaultCollectionName = "messages";

   /// <summary>
   /// Creates an instance of the transport connecting via <paramref name="connectionString"/>
   /// </summary>
   public MongoDbTransportOptions(
       string connectionString, string collectionName = DefaultCollectionName) : this(new MongoUrl(connectionString), collectionName)
   {
   }

   /// <summary>
   /// Creates an instance of transport connection
   /// </summary>
   /// <param name="connectionString"></param>
   public MongoDbTransportOptions(MongoUrl connectionString, string collectionName = DefaultCollectionName)
   {
      ConnectionString = connectionString;
      CollectionName = collectionName;
   }

   /// <summary>
   /// Set input queue name value
   /// </summary>
   /// <param name="inputQueueName"></param>
   /// <returns></returns>
   public MongoDbTransportOptions SetInputQueueName(string inputQueueName)
   {
      this.InputQueueName = inputQueueName;
      return this;
   }

   /// <summary>
   /// Connection string.
   /// </summary>
   public MongoUrl ConnectionString { get; internal set; }
   public string CollectionName { get; internal set; }

   /// <summary>
   /// Name of the input queue to process. If <c>null</c> or whitespace the transport will be configured in one way mode (send only)
   /// </summary>
   public string InputQueueName { get; internal set; }

   /// <summary>
   /// If <c>false</c> collections will not be created and must be created outside of Rebus
   /// </summary>
   public bool EnsureCollectionsAreCreated { get; internal set; } = true;

   /// <summary>
   /// If <c>true</c> the transport is configured in one way mode
   /// </summary>
   public bool IsOneWayQueue => InputQueueName == null;
}