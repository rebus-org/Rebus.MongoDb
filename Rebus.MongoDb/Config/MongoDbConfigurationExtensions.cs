using System;
using MongoDB.Driver;
using Rebus.Auditing.Sagas;
using Rebus.DataBus;
using Rebus.Logging;
using Rebus.MongoDb.DataBus;
using Rebus.MongoDb.Sagas;
using Rebus.MongoDb.Subscriptions;
using Rebus.MongoDb.Timeouts;
using Rebus.Sagas;
using Rebus.Subscriptions;
using Rebus.Time;
using Rebus.Timeouts;
// ReSharper disable UnusedMember.Global

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for MongoDB persistence
    /// </summary>
    public static class MongoDbConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to store data bus attachments in MongoDB
        /// </summary>
        public static void StoreInMongoDb(this StandardConfigurer<IDataBusStorage> configurer, IMongoDatabase mongoDatabase, string bucketName)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (mongoDatabase == null) throw new ArgumentNullException(nameof(mongoDatabase));
            if (bucketName == null) throw new ArgumentNullException(nameof(bucketName));

            configurer.OtherService<MongoDbDataBusStorage>().Register(c => new MongoDbDataBusStorage(c.Get<IRebusTime>(), mongoDatabase, bucketName));

            configurer.Register(c => c.Get<MongoDbDataBusStorage>());

            configurer.OtherService<IDataBusStorageManagement>().Register(c => c.Get<MongoDbDataBusStorage>());
        }

        /// <summary>
        /// Configures Rebus to store saga data snapshots in MongoDB
        /// </summary>
        public static void StoreInMongoDb(this StandardConfigurer<ISagaSnapshotStorage> configurer, IMongoDatabase mongoDatabase, string collectionName)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (mongoDatabase == null) throw new ArgumentNullException(nameof(mongoDatabase));
            if (collectionName == null) throw new ArgumentNullException(nameof(collectionName));

            configurer.Register(c => new MongoDbSagaSnapshotStorage(mongoDatabase, collectionName));
        }

        /// <summary>
        /// Configures Rebus to use MongoDB to store sagas, using the specified collection name resolver function. If the collection name resolver is omitted,
        /// collection names will be determined by using the <code>Name</code> property of the saga data's <see cref="Type"/>
        /// </summary>
        public static void StoreInMongoDb(this StandardConfigurer<ISagaStorage> configurer, IMongoDatabase mongoDatabase, Func<Type, string> collectionNameResolver = null, bool automaticallyCreateIndexes = true)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (mongoDatabase == null) throw new ArgumentNullException(nameof(mongoDatabase));

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var sagaStorage = new MongoDbSagaStorage(mongoDatabase, rebusLoggerFactory, collectionNameResolver, automaticallyCreateIndexes);

                return sagaStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use MongoDB to store subscriptions. Use <paramref name="isCentralized"/> = true to indicate whether it's OK to short-circuit
        /// subscribing and unsubscribing by manipulating the subscription directly from the subscriber or just let it default to false to preserve the
        /// default behavior.
        /// </summary>
        public static void StoreInMongoDb(this StandardConfigurer<ISubscriptionStorage> configurer, IMongoDatabase mongoDatabase, string collectionName, bool isCentralized = false)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (mongoDatabase == null) throw new ArgumentNullException(nameof(mongoDatabase));
            if (collectionName == null) throw new ArgumentNullException(nameof(collectionName));

            configurer.Register(c =>
            {
                var subscriptionStorage = new MongoDbSubscriptionStorage(mongoDatabase, collectionName, isCentralized);

                return subscriptionStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use MongoDB to store timeouts.
        /// </summary>
        public static void StoreInMongoDb(this StandardConfigurer<ITimeoutManager> configurer, IMongoDatabase mongoDatabase, string collectionName)
        {
            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var rebusTime = c.Get<IRebusTime>();
                var subscriptionStorage = new MongoDbTimeoutManager(rebusTime, mongoDatabase, collectionName, rebusLoggerFactory);

                return subscriptionStorage;
            });
        }
    }
}