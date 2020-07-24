using MongoDB.Bson;
using MongoDB.Driver;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Exceptions;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Transport;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.MongoDb.Transport
{
    /// <summary>
    /// <para>Implementation of <see cref="ITransport"/> that uses MongoDb to do its thing</para>
    /// <para>
    /// There are a couple of problems to this: all related to how $currentDate is NOT yet handled
    /// for:
    /// - inserts: https://jira.mongodb.org/browse/SERVER-13695
    /// - queries: https://jira.mongodb.org/browse/SERVER-28144
    /// </para>
    /// <para>
    /// also:
    /// - there are no native transactions so we need to rely on some in-memory storage to actively
    /// support transaction.
    /// </para>
    /// </summary>
    public class MongoDbTransport : ITransport, IInitializable
    {
        /// <summary>
        /// When a message is sent to this address, it will be deferred into the future!
        /// </summary>
        public const string MagicExternalTimeoutManagerAddress = "##### MagicExternalTimeoutManagerAddress #####";

        /// <summary>
        /// Special message priority header that can be used with the <see cref="MongoDbTransport"/>. The value must be an <see cref="Int32"/>
        /// </summary>
        public const string MessagePriorityHeaderKey = "rbs2-msg-priority";

        /// <summary>
        /// Key of the transport's currently used database connection. Can be retrieved from the context and used e.g.
        /// in a connection provider which is then in turn used in repositories and such. This way, "exactly once delivery" can actually be had.
        /// </summary>
        public const string CurrentConnectionKey = "mongo-db-transport-current-connection";

        /// <summary>
        /// Default interval that will be used for <see cref="ExpiredMessagesCleanupInterval"/> unless it is explicitly set to something else
        /// </summary>
        public static readonly TimeSpan DefaultExpiredMessagesCleanupInterval = TimeSpan.FromSeconds(20);

        /// <summary>
        /// Size, in the database, of the recipient column
        /// </summary>
        protected const int RecipientColumnSize = 200;

        private readonly IRebusTime _rebusTime; // IRebusTime
        private readonly MongoDbTransportOptions _mongoDbTransportOptions;
        private readonly AsyncBottleneck _bottleneck = new AsyncBottleneck(20);
        private readonly ILog _log;

        private readonly IMongoDatabase _database;

        /// <summary>
        /// This is the collection that points to _receiveCollectionNAme and it is the 
        /// collection that will receive messages.
        /// </summary>
        private IMongoCollection<TransportMessageMongoDb> _collectionQueue;

        /// <summary>
        /// Constructs the transport with the given <see cref="IMongoDatabase"/>
        /// </summary>
        public MongoDbTransport(
            IRebusLoggerFactory rebusLoggerFactory,
            IAsyncTaskFactory asyncTaskFactory,
            IRebusTime rebusTime,
            MongoDbTransportOptions mongoDbTransportOptions
            )
        {
            if (rebusLoggerFactory == null)
            {
                throw new ArgumentNullException(nameof(rebusLoggerFactory));
            }

            _log = rebusLoggerFactory.GetLogger<MongoDbTransport>();

            try
            {
                if (asyncTaskFactory == null)
                {
                    throw new ArgumentNullException(nameof(asyncTaskFactory));
                }

                var client = new MongoClient(mongoDbTransportOptions.ConnectionString);
                _database = client.GetDatabase(mongoDbTransportOptions.ConnectionString.DatabaseName);

                _rebusTime = rebusTime ?? throw new ArgumentNullException(nameof(rebusTime));
                _mongoDbTransportOptions = mongoDbTransportOptions;

                if (!mongoDbTransportOptions.IsOneWayQueue)
                {
                    Address = mongoDbTransportOptions.InputQueueName ?? throw new ArgumentNullException(nameof(rebusTime));
                }

                ExpiredMessagesCleanupInterval = DefaultExpiredMessagesCleanupInterval;
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Exception during creation of Mongodbtransport");
                throw;
            }
        }

        /// <summary>
        /// Initializes the transport by starting a task that deletes expired messages from the collection
        /// </summary>
        public void Initialize()
        {
            //Initialize the collection
            if (!_mongoDbTransportOptions.IsOneWayQueue)
            {
                _collectionQueue = _database.GetCollection<TransportMessageMongoDb>(Address);
            }
        }

        /// <summary>
        /// Configures the interval between periodic deletion of expired messages. Defaults to <see cref="DefaultExpiredMessagesCleanupInterval"/>
        /// </summary>
        public TimeSpan ExpiredMessagesCleanupInterval { get; set; }

        /// <summary>
        /// Gets the name that this Mongodb transport will use to query by when checking the messages table
        /// </summary>
        public string Address { get; private set; }

        /// <summary>
        /// Creates the collection named after the given <paramref name="address"/>
        /// </summary>
        public void CreateQueue(string address)
        {
            if (address == null)
            {
                return;
            }

            AsyncHelpers.RunSync(() => InnerEnsureTableIsCreatedAsync(address));
        }

        /// <summary>
        /// Checks if the collection with the configured name exists - if not, it will be created
        /// </summary>
        public void EnsureCollectionIsCreated()
        {
            AsyncHelpers.RunSync(() => InnerEnsureTableIsCreatedAsync(_mongoDbTransportOptions.InputQueueName));
        }

        private async Task InnerEnsureTableIsCreatedAsync(string queueName)
        {
            // index creation
            var coll = _database.GetCollection<TransportMessageMongoDb>(queueName);

            // Receice index: priority, visible, expiration, id
            await coll.Indexes.CreateOneAsync(
                new CreateIndexModel<TransportMessageMongoDb>(
                Builders<TransportMessageMongoDb>.IndexKeys
                    .Ascending(o => o.Priority)
                    .Ascending(o => o.Visibile)
                    .Ascending(o => o.Expiration)
                    .Ascending(o => o.Id),
                new CreateIndexOptions
                {
                    Name = "IDX_RECEIVE_" + queueName,
                    Background = true,
                    Unique = true,
                })
                ).ConfigureAwait(true);

            // Expiration index: expiration
            await coll.Indexes.CreateOneAsync(
                new CreateIndexModel<TransportMessageMongoDb>(
                Builders<TransportMessageMongoDb>.IndexKeys
                    .Ascending(o => o.Expiration),
                new CreateIndexOptions
                {
                    Name = "IDX_EXPIRATION_" + queueName,
                    Background = true,
                    Unique = false,
                    ExpireAfter = TimeSpan.Zero,
                })
                ).ConfigureAwait(true);
        }

        private Int32 _concurrentSend = 0;

        /// <summary>
        /// Sends the given transport message to the specified destination queue address by adding it to the queue's table.
        /// </summary>
        public virtual async Task Send(
            string destinationAddress,
            TransportMessage message,
            ITransactionContext context)
        {
            var destinationAddressToUse = GetDestinationAddressToUse(destinationAddress, message);

            try
            {
                if (_concurrentSend > 5)
                {
                    SpinWait.SpinUntil(() => _concurrentSend < 10);
                }
                Interlocked.Increment(ref _concurrentSend);
                var insertedRecord = await InnerSend(destinationAddressToUse, message).ConfigureAwait(false);
                if (context != null)
                {
                    context.OnAborted(_ =>
                    {
                        var collection = GetCollectionFromDestination(destinationAddress);
                        collection.DeleteOne(Builders<TransportMessageMongoDb>.Filter.Eq(r => r.Id, insertedRecord.Id));
                        _log.Info("Rollback insertion of message {0}", insertedRecord.Id);
                    });
                }
            }
            catch (Exception e)
            {
                throw new RebusApplicationException(e, $"Unable to send to destination {destinationAddress}");
            }
            finally
            {
                Interlocked.Decrement(ref _concurrentSend);
            }
        }

        /// <summary>
        /// Receives the next message by querying the input queue table for a message with a recipient matching this transport's <see cref="Address"/>
        /// </summary>
        public async Task<TransportMessage> Receive(
            ITransactionContext context,
            CancellationToken cancellationToken)
        {
            using (await _bottleneck.Enter(cancellationToken).ConfigureAwait(false))
            {
                var message = await ReceiveInternal(context, cancellationToken).ConfigureAwait(false);
                if (message != null && context != null)
                {
                    context.OnAborted(c => _collectionQueue.InsertOne(message));
                }

                return ExtractTransportMessageFromReader(message);
            }
        }

        /// <summary>
        /// Handle retrieving a message from the queue, decoding it, and performing any 
        /// transaction maintenance.
        /// </summary>
        /// <param name="context">Tranasction context the receive is operating on</param>
        /// <param name="cancellationToken">Token to abort processing</param>
        /// <returns>A <seealso cref="TransportMessage"/> or <c>null</c> if no message can be dequeued</returns>
        protected virtual async Task<TransportMessageMongoDb> ReceiveInternal(ITransactionContext context, CancellationToken cancellationToken)
        {
            if (_mongoDbTransportOptions.IsOneWayQueue)
            {
                return null;
            }

            TransportMessageMongoDb receivedTransportMessage;

            var filter =
                Builders<TransportMessageMongoDb>.Filter.And(
                Builders<TransportMessageMongoDb>.Filter.Gt(f => f.Expiration, DateTime.UtcNow)
                , Builders<TransportMessageMongoDb>.Filter.Lt(f => f.Visibile, DateTime.UtcNow));

            var sort = Builders<TransportMessageMongoDb>.Sort
                .Descending(t => t.Priority)
                .Ascending(t => t.Visibile)
                .Ascending(t => t.Id);
            try
            {
                var record = await _collectionQueue.FindOneAndDeleteAsync(
                    filter,
                    new FindOneAndDeleteOptions<TransportMessageMongoDb, TransportMessageMongoDb>()
                    {
                        Sort = sort
                    }).ConfigureAwait(false);
                receivedTransportMessage = record;
            }
            catch (Exception exception) when (cancellationToken.IsCancellationRequested)
            {
                // ADO.NET does not throw the right exception when the task gets cancelled - therefore we need to do this:
                throw new TaskCanceledException("Receive operation was cancelled", exception);
            }

            return receivedTransportMessage;
        }

        /// <summary>
        /// Simple convert from internal object to return TransportMessage
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        protected static TransportMessage ExtractTransportMessageFromReader(
           TransportMessageMongoDb reader)
        {
            if (reader == null)
            {
                return null;
            }

            return new TransportMessage(reader.Headers, reader.Body);
        }


        /// <summary>
        /// Gets the address a message will actually be sent to. Handles deferred messsages.
        /// </summary>
        protected static string GetDestinationAddressToUse(string destinationAddress, TransportMessage message)
        {
            return string.Equals(destinationAddress, MagicExternalTimeoutManagerAddress, StringComparison.CurrentCultureIgnoreCase)
                ? GetDeferredRecipient(message)
                : destinationAddress;
        }

        private static string GetDeferredRecipient(TransportMessage message)
        {
            if (message.Headers.TryGetValue(Headers.DeferredRecipient, out var destination))
            {
                return destination;
            }

            throw new InvalidOperationException($"Attempted to defer message, but no '{Headers.DeferredRecipient}' header was on the message");
        }

        /// <summary>
        /// Performs persistence of a message to the underlying table
        /// </summary>
        /// <param name="destinationAddress">Address the message will be sent to</param>
        /// <param name="message">Message to be sent</param>
        protected async Task<TransportMessageMongoDb> InnerSend(string destinationAddress, TransportMessage message)
        {
            IMongoCollection<TransportMessageMongoDb> destinationQueue = GetCollectionFromDestination(destinationAddress);

            var headers = message.Headers.Clone();

            var priority = GetMessagePriority(headers);
            var visible = GetInitialVisibilityDelay(headers);
            var ttl = GetTtl(headers);

            TransportMessageMongoDb record = new TransportMessageMongoDb();
            record.Id = ObjectId.GenerateNewId();
            record.Headers = headers;
            record.Body = message.Body;
            record.Priority = priority;
            record.Visibile = DateTime.UtcNow.Add(visible);
            record.Expiration = DateTime.UtcNow.Add(ttl);

            await destinationQueue.InsertOneAsync(record).ConfigureAwait(false);
            return record;
        }

        private ConcurrentDictionary<String, IMongoCollection<TransportMessageMongoDb>> _collectionCache = new ConcurrentDictionary<string, IMongoCollection<TransportMessageMongoDb>>();

        private IMongoCollection<TransportMessageMongoDb> GetCollectionFromDestination(string destinationAddress)
        {
            if (_collectionCache.TryGetValue(destinationAddress, out var connection))
            {
                return connection;
            }
            connection = _database.GetCollection<TransportMessageMongoDb>(destinationAddress);
            _collectionCache[destinationAddress] = connection;
            return connection;
        }

        private TimeSpan GetInitialVisibilityDelay(IDictionary<string, string> headers)
        {
            if (!headers.TryGetValue(Headers.DeferredUntil, out var deferredUntilDateTimeOffsetString))
            {
                return TimeSpan.Zero;
            }

            var deferredUntilTime = deferredUntilDateTimeOffsetString.ToDateTimeOffset();

            headers.Remove(Headers.DeferredUntil);

            var visibilityDelay = deferredUntilTime - _rebusTime.Now;
            return visibilityDelay;
        }

        private static TimeSpan GetTtl(IReadOnlyDictionary<string, string> headers)
        {
            const int defaultTtlSecondsAbout60Years = int.MaxValue;

            if (!headers.ContainsKey(Headers.TimeToBeReceived))
            {
                return TimeSpan.FromSeconds(defaultTtlSecondsAbout60Years);
            }

            var timeToBeReceivedStr = headers[Headers.TimeToBeReceived];
            return TimeSpan.Parse(timeToBeReceivedStr);
        }

        private static int GetMessagePriority(Dictionary<string, string> headers)
        {
            var valueOrNull = headers.GetValueOrNull(MessagePriorityHeaderKey);
            if (valueOrNull == null)
            {
                return 0;
            }

            try
            {
                return int.Parse(valueOrNull);
            }
            catch (Exception exception)
            {
                throw new FormatException($"Could not parse '{valueOrNull}' into an Int32!", exception);
            }
        }
    }
}
