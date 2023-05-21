using MongoDB.Bson;
using MongoDB.Driver;
using Rebus.MongoDb.Transport.Internals;
using System;

namespace Rebus.MongoDb.Transport
{
   internal class MongoDbTransportConfiguration
   {
      /// <summary>
      /// Indicates the default message lease timeout in seconds
      /// </summary>
      public const int DefaultMessageLeaseSeconds = 60;

      /// <summary>
      /// Indicates the default max level of parallelism allowed (i.e. allowed number of concurrent async <see cref="Task"/>-based 
      /// operations, constrained within one <see cref="Producer"/> or <see cref="Consumer"/> instance)
      /// </summary>
      public const int DefaultMaxParallelism = 20;

      /// <summary>
      /// Indicates the default max number of delivery attempts for each message
      /// </summary>
      public const int DefaultMaxDeliveryAttempts = 5;

      /// <summary>
      /// Creates the configuration using the given MongoDB URL (which MUST contain a database name) and <paramref name="collectionName"/>.
      /// Optionally specifies the default lease time in seconds by setting <paramref name="defaultMessageLeaseSeconds"/> (default is <see cref="DefaultMessageLeaseSeconds"/>).
      /// Optionally specifies the default max parallelism by setting <paramref name="maxParallelism"/> (default is <see cref="DefaultMaxParallelism"/>).
      /// </summary>
      public MongoDbTransportConfiguration(MongoUrl mongoUrl, string collectionName, int defaultMessageLeaseSeconds = DefaultMessageLeaseSeconds, int maxParallelism = DefaultMaxParallelism, int maxDeliveryAttempts = DefaultMaxDeliveryAttempts)
          : this(mongoUrl.GetMongoDatabase(), collectionName, defaultMessageLeaseSeconds, maxParallelism, maxDeliveryAttempts)
      {
      }

      /// <summary>
      /// Creates the configuration using the given MongoDB connection string (which MUST contain a database name) and <paramref name="collectionName"/>.
      /// Optionally specifies the default lease time in seconds by setting <paramref name="defaultMessageLeaseSeconds"/> (default is <see cref="DefaultMessageLeaseSeconds"/>).
      /// Optionally specifies the default max parallelism by setting <paramref name="maxParallelism"/> (default is <see cref="DefaultMaxParallelism"/>).
      /// </summary>
      public MongoDbTransportConfiguration(string connectionString, string collectionName, int defaultMessageLeaseSeconds = DefaultMessageLeaseSeconds, int maxParallelism = DefaultMaxParallelism, int maxDeliveryAttempts = DefaultMaxDeliveryAttempts)
          : this(connectionString.GetMongoDatabase(), collectionName, defaultMessageLeaseSeconds, maxParallelism, maxDeliveryAttempts)
      {
      }

      /// <summary>
      /// Creates the configuration using the given MongoDB database and <paramref name="collectionName"/>.
      /// Optionally specifies the default lease time in seconds by setting <paramref name="defaultMessageLeaseSeconds"/> (default is <see cref="DefaultMessageLeaseSeconds"/>).
      /// Optionally specifies the default max parallelism by setting <paramref name="maxParallelism"/> (default is <see cref="DefaultMaxParallelism"/>).
      /// </summary>
      public MongoDbTransportConfiguration(IMongoDatabase database, string collectionName, int defaultMessageLeaseSeconds = DefaultMessageLeaseSeconds, int maxParallelism = DefaultMaxParallelism, int maxDeliveryAttempts = DefaultMaxDeliveryAttempts)
      {
         if (database == null) throw new ArgumentNullException(nameof(database));

         if (defaultMessageLeaseSeconds <= 0)
         {
            throw new ArgumentOutOfRangeException(nameof(defaultMessageLeaseSeconds), defaultMessageLeaseSeconds, "Please specify a positive number of seconds for the lease duration");
         }

         if (maxDeliveryAttempts <= 0)
         {
            throw new ArgumentOutOfRangeException(nameof(maxDeliveryAttempts), maxDeliveryAttempts, "Please specify a positions number for the number of delivery attempts to accept for each message");
         }

         MaxParallelism = maxParallelism;
         Collection = InitializeCollection(collectionName, database);
         DefaultMessageLease = TimeSpan.FromSeconds(defaultMessageLeaseSeconds);
         MaxDeliveryAttempts = maxDeliveryAttempts;
      }

      static IMongoCollection<BsonDocument> InitializeCollection(string collectionName, IMongoDatabase mongoDatabase)
      {
         IMongoCollection<BsonDocument> collection = mongoDatabase.GetCollection<BsonDocument>(collectionName);
         BsonDocument index = new BsonDocument
           {
               {Fields.DestinationQueueName, 1},
               {Fields.ReceiveTime, 1},
               {Fields.DeliveryAttempts, 1},
           };
         collection.Indexes.CreateOne(new CreateIndexModel<BsonDocument>(new BsonDocumentIndexKeysDefinition<BsonDocument>(index)));
         return collection;
      }

      /// <summary>
      /// Creates a producer using the current configuration
      /// </summary>
      public MongoDbMessageProducer CreateProducer() => new MongoDbMessageProducer(this);

      /// <summary>
      /// Creates a consumer using the current configuration and the given <paramref name="queueName"/>
      /// </summary>
      public MongoDbMessageConsumer CreateConsumer(string queueName) => new MongoDbMessageConsumer(this, queueName);

      internal IMongoCollection<BsonDocument> Collection { get; }

      internal int MaxParallelism { get; }

      internal int MaxDeliveryAttempts { get; }

      internal TimeSpan DefaultMessageLease { get; }
   }
}
