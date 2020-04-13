using MongoDB.Driver;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Serialization;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Transport;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.MongoDb.Transport
{
    /// <summary>
    /// Implementation of <see cref="ITransport"/> that uses MongoDb to do its thing
    /// 
    /// There are a couple of problems to this: all related to how $currentDate is NOT yet handled
    /// for:
    /// - inserts: https://jira.mongodb.org/browse/SERVER-13695
    /// - queries: https://jira.mongodb.org/browse/SERVER-28144
    /// 
    /// also:
    /// - there are no transactions, so once a message is extracted from the queue, it's gone
    ///   even if processing it result in errors: the message is lost.
    /// - In SQL implementation there's a context around each Operation that opens up a transaction
    ///   and complete it when the context gets committed, or roll back it in sace of error (con context
    ///   disposal), it might need an explicit ACK to delete the message from the queue.
    ///   we also need to mark a message as "in-flight" / "In-process" so we can tell the queue
    ///   that the message was already in processing.
    /// 
    /// With these limitations
    /// This kind of transport will work only if everything runs on the same machine, there's no
    /// way to guarantee clock sync, not a good way to insert and query with the database server time
    /// </summary>
    public class MongoDbTransport : ITransport, IInitializable, IDisposable
    {
        static readonly HeaderSerializer HeaderSerializer = new HeaderSerializer();

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

        private readonly RebusTime _rebusTime; // IRebusTime

        private readonly AsyncBottleneck _bottleneck = new AsyncBottleneck(20);
        private readonly ILog _log;
        private readonly IAsyncTask _expiredMessagesCleanupTask;
        private bool _disposed;

        private readonly IMongoDatabase _database;

        protected IMongoCollection<TransportMessageDb> _collectionQueue { get; private set; }

        /// <summary>
        /// Name of the collection this transport is using for storage
        /// </summary>
        protected readonly string ReceiveTableName;

        /// <summary>
        /// Constructs the transport with the given <see cref="IMongoDatabase"/>
        /// </summary>
        public MongoDbTransport(
            IMongoDatabase database,
            string inputQueueName,
            IRebusLoggerFactory rebusLoggerFactory,
            IAsyncTaskFactory asyncTaskFactory
            ) // ,IRebusTime rebusTime
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            if (asyncTaskFactory == null) throw new ArgumentNullException(nameof(asyncTaskFactory));
            _database = database ?? throw new ArgumentNullException(nameof(database));

            _rebusTime = new RebusTime(); // rebusTime ?? throw new ArgumentNullException(nameof(rebusTime));
            ReceiveTableName = inputQueueName; // inputQueueName != null ? TableName.Parse(inputQueueName) : null;

            _log = rebusLoggerFactory.GetLogger<MongoDbTransport>();

            ExpiredMessagesCleanupInterval = DefaultExpiredMessagesCleanupInterval;

            _expiredMessagesCleanupTask = asyncTaskFactory.Create("ExpiredMessagesCleanup", PerformExpiredMessagesCleanupCycle, intervalSeconds: 60);
        }

        /// <summary>
        /// Initializes the transport by starting a task that deletes expired messages from the collection
        /// </summary>
        public void Initialize()
        {
            if (ReceiveTableName == null) return;

            _expiredMessagesCleanupTask.Start();
        }

        /// <summary>
        /// Configures the interval between periodic deletion of expired messages. Defaults to <see cref="DefaultExpiredMessagesCleanupInterval"/>
        /// </summary>
        public TimeSpan ExpiredMessagesCleanupInterval { get; set; }

        /// <summary>
        /// Gets the name that this SQL transport will use to query by when checking the messages table
        /// </summary>
        public string Address => ReceiveTableName; // ReceiveTableName?.QualifiedName;

        /// <summary>
        /// Creates the collection named after the given <paramref name="address"/>
        /// </summary>
        public void CreateQueue(string address)
        {
            if (address == null) return;

            AsyncHelpers.RunSync(() => EnsureTableIsCreatedAsync(ReceiveTableName));
        }

        /// <summary>
        /// Checks if the table with the configured name exists - if not, it will be created
        /// </summary>
        public void EnsureTableIsCreated()
        {
            try
            {
                AsyncHelpers.RunSync(() => EnsureTableIsCreatedAsync(ReceiveTableName));
            }
            catch
            {
                // if it failed because of a collision between another thread doing the same thing, just try again once:
                AsyncHelpers.RunSync(() => EnsureTableIsCreatedAsync(ReceiveTableName));
            }
        }

        async Task EnsureTableIsCreatedAsync(string queueName)
        {
            try
            {
                await InnerEnsureTableIsCreatedAsync(queueName).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // if it fails the first time, and if it's because of some kind of conflict,
                // we should run it again and see if the situation has stabilized
                await InnerEnsureTableIsCreatedAsync(queueName).ConfigureAwait(false);
            }
        }

        async Task InnerEnsureTableIsCreatedAsync(string queueName)
        {
            // index creation
            _collectionQueue = _database.GetCollection<TransportMessageDb>(queueName);
            // Receice index: priority, visible, expiration, id
            await _collectionQueue.Indexes.CreateOneAsync(
                Builders<TransportMessageDb>.IndexKeys
                    .Ascending(o => o.Priority)
                    .Ascending(o => o.Visibile)
                    .Ascending(o => o.Expiration)
                    .Ascending(o => o.Id),
                new CreateIndexOptions
                {
                    Name = "IDX_RECEIVE_" + queueName,
                    Background = true,
                    Unique = true,
                }
                ).ConfigureAwait(true);
            // Expiration index: expiration
            await _collectionQueue.Indexes.CreateOneAsync(
                Builders<TransportMessageDb>.IndexKeys
                    .Ascending(o => o.Expiration),
                new CreateIndexOptions
                {
                    Name = "IDX_EXPIRATION_" + queueName,
                    Background = true,
                    Unique = false,
                }
                ).ConfigureAwait(true);
            AdditionalSchemaModifications(queueName);
        }

        /// <summary>
        /// Provides an opportunity for derived implementations to also update the schema
        /// </summary>
        /// <param name="queueName">Name of the table to create schema modifications for</param>
        protected virtual void AdditionalSchemaModifications(string queueName)
        {
            // intentionally left blank
        }

        /// <summary>
        /// Sends the given transport message to the specified destination queue address by adding it to the queue's table.
        /// </summary>
        public virtual async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            var destinationAddressToUse = GetDestinationAddressToUse(destinationAddress, message);

            try
            {
                await InnerSend(destinationAddressToUse, message).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new RebusApplicationException(e, $"Unable to send to destination {destinationAddress}");
            }
        }

        /// <summary>
        /// Receives the next message by querying the input queue table for a message with a recipient matching this transport's <see cref="Address"/>
        /// </summary>
        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            using (await _bottleneck.Enter(cancellationToken).ConfigureAwait(false))
            {
                return await ReceiveInternal(context, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Handle retrieving a message from the queue, decoding it, and performing any transaction maintenance.
        /// </summary>
        /// <param name="context">Tranasction context the receive is operating on</param>
        /// <param name="cancellationToken">Token to abort processing</param>
        /// <returns>A <seealso cref="TransportMessage"/> or <c>null</c> if no message can be dequeued</returns>
        protected virtual async Task<TransportMessage> ReceiveInternal(ITransactionContext context, CancellationToken cancellationToken)
        {
            var connection = await GetConnection(context).ConfigureAwait(false);

            TransportMessage receivedTransportMessage;

            using (var selectCommand = connection.CreateCommand())
            {
                selectCommand.CommandText = $@"
	SET NOCOUNT ON
	;WITH TopCTE AS (
		SELECT	TOP 1
				[id],
				[headers],
				[body]
		FROM	{ReceiveTableName.QualifiedName} M WITH (ROWLOCK, READPAST, READCOMMITTEDLOCK)
		WHERE	
                M.[visible] < sysdatetimeoffset()
		AND		M.[expiration] > sysdatetimeoffset()
		ORDER
		BY		[priority] DESC,
				[visible] ASC,
				[id] ASC
	)
	DELETE	FROM TopCTE
	OUTPUT	deleted.[id] as [id],
			deleted.[headers] as [headers],
			deleted.[body] as [body]
						
						";

                try
                {
                    using (var reader = await selectCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                    {
                        receivedTransportMessage = await ExtractTransportMessageFromReader(reader, cancellationToken).ConfigureAwait(false);
                    }
                }
                catch (Exception exception) when (cancellationToken.IsCancellationRequested)
                {
                    // ADO.NET does not throw the right exception when the task gets cancelled - therefore we need to do this:
                    throw new TaskCanceledException("Receive operation was cancelled", exception);
                }
            }

            return receivedTransportMessage;
        }

        /// <summary>
        /// Maps a <seealso cref="SqlDataReader"/> that's read a result from the message table into a <seealso cref="TransportMessage"/>
        /// </summary>
        /// <returns>A <seealso cref="TransportMessage"/> representing the row or <c>null</c> if no row was available</returns>
        protected static async Task<TransportMessage> ExtractTransportMessageFromReader(SqlDataReader reader, CancellationToken cancellationToken)
        {
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false) == false)
            {
                return null;
            }

            var headers = reader["headers"];
            var headersDictionary = HeaderSerializer.Deserialize((byte[])headers);
            var body = (byte[])reader["body"];

            return new TransportMessage(headersDictionary, body);
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

        static string GetDeferredRecipient(TransportMessage message)
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
        protected async Task InnerSend(string destinationAddress, TransportMessage message)
        {
            var destinationQueue = _database.GetCollection<TransportMessageDb>(destinationAddress);

            var headers = message.Headers.Clone();

            var priority = GetMessagePriority(headers);
            var visible = GetInitialVisibilityDelay(headers);
            var ttl = GetTtl(headers);

            var update = Builders<TransportMessageDb>.Update
                .Set(t => t.Headers, headers)
                .Set(t => t.Body, message.Body)
                .Set(t => t.Priority, priority)
                .CurrentDate(t => t.Visibile, UpdateDefinitionCurrentDateType.Date)
                .Inc(t => t.Visibile, visible.TotalMilliseconds)
                .CurrentDate(t => t.Expiration, UpdateDefinitionCurrentDateType.Date)
                .Inc(t => t.Expiration, ttl.TotalMilliseconds)
                ;

            await destinationQueue.UpdateOneAsync(null, update, new UpdateOptions
            {
                IsUpsert = true
            }).ConfigureAwait(false);
        }

        TimeSpan GetInitialVisibilityDelay(IDictionary<string, string> headers)
        {
            if (!headers.TryGetValue(Headers.DeferredUntil, out var deferredUntilDateTimeOffsetString))
            {
                return TimeSpan.Zero;
            }

            var deferredUntilTime = deferredUntilDateTimeOffsetString.ToDateTimeOffset();

            headers.Remove(Headers.DeferredUntil);

            var visibilityDelay = deferredUntilTime - RebusTime.Now;
            return visibilityDelay;
        }

        static TimeSpan GetTtl(IReadOnlyDictionary<string, string> headers)
        {
            const int defaultTtlSecondsAbout60Years = int.MaxValue;

            if (!headers.ContainsKey(Headers.TimeToBeReceived))
            {
                return TimeSpan.FromSeconds(defaultTtlSecondsAbout60Years);
            }

            var timeToBeReceivedStr = headers[Headers.TimeToBeReceived];
            var timeToBeReceived = TimeSpan.Parse(timeToBeReceivedStr);

            return timeToBeReceived;
        }

        async Task PerformExpiredMessagesCleanupCycle()
        {
            var results = 0;
            var stopwatch = Stopwatch.StartNew();

            while (true)
            {
                using (var connection = await ConnectionProvider.GetConnection())
                {
                    int affectedRows;

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText =
                            $@"
;with TopCTE as (
	SELECT TOP 1 [id] FROM {ReceiveTableName.QualifiedName} WITH (ROWLOCK, READPAST)
				WHERE 
                    [expiration] < sysdatetimeoffset()
)
DELETE FROM TopCTE
";

                        affectedRows = await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }

                    results += affectedRows;

                    await connection.Complete();

                    if (affectedRows == 0) break;
                }
            }

            if (results > 0)
            {
                _log.Info("Performed expired messages cleanup in {cleanupTimeSeconds} - {expiredMessageCount} expired messages with recipient {queueName} were deleted",
                    stopwatch.Elapsed.TotalSeconds, results, ReceiveTableName.QualifiedName);
            }
        }

        static int GetMessagePriority(Dictionary<string, string> headers)
        {
            var valueOrNull = headers.GetValueOrNull(MessagePriorityHeaderKey);
            if (valueOrNull == null) return 0;

            try
            {
                return int.Parse(valueOrNull);
            }
            catch (Exception exception)
            {
                throw new FormatException($"Could not parse '{valueOrNull}' into an Int32!", exception);
            }
        }

        /// <summary>
        /// Shuts down the background timer
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;

            try
            {
                _expiredMessagesCleanupTask.Dispose();
            }
            finally
            {
                _disposed = true;
            }
        }
    }
}
