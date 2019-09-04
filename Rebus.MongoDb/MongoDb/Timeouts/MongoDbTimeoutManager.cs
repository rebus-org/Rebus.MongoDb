using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Time;
using Rebus.Timeouts;

namespace Rebus.MongoDb.Timeouts
{
    /// <summary>
    /// Implementation of <see cref="ITimeoutManager"/> that uses MongoDB to save timeouts
    /// </summary>
    public class MongoDbTimeoutManager : ITimeoutManager
    {
        readonly IRebusTime _rebusTime;
        readonly IMongoCollection<Timeout> _timeouts;
        readonly ILog _log;

        /// <summary>
        /// Constructs the timeout manager
        /// </summary>
        public MongoDbTimeoutManager(IRebusTime rebusTime, IMongoDatabase database, string collectionName, IRebusLoggerFactory rebusLoggerFactory)
        {
            if (database == null) throw new ArgumentNullException(nameof(database));
            if (collectionName == null) throw new ArgumentNullException(nameof(collectionName));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            _rebusTime = rebusTime ?? throw new ArgumentNullException(nameof(rebusTime));
            _log = rebusLoggerFactory.GetLogger<MongoDbTimeoutManager>();
            _timeouts = database.GetCollection<Timeout>(collectionName);
        }

        /// <inheritdoc />
        public async Task Defer(DateTimeOffset approximateDueTime, Dictionary<string, string> headers, byte[] body)
        {
            var newTimeout = new Timeout(headers, body, approximateDueTime.UtcDateTime);

            _log.Debug("Deferring message with ID {messageId} until {dueTime} (doc ID {documentId})", 
                headers.GetValue(Headers.MessageId), approximateDueTime, newTimeout.Id);

            await _timeouts.InsertOneAsync(newTimeout).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<DueMessagesResult> GetDueMessages()
        {
            var now = _rebusTime.Now.UtcDateTime;
            var dueTimeouts = new List<Timeout>();

            while (dueTimeouts.Count < 100)
            {
                var filter = Builders<Timeout>.Filter.Lte(t => t.DueTimeUtc, now);
                var update = Builders<Timeout>.Update.Set(t => t.DueTimeUtc, now.AddMinutes(1));

                var dueTimeout = await _timeouts.FindOneAndUpdateAsync(filter, update).ConfigureAwait(false);

                if (dueTimeout == null)
                {
                    break;
                }

                dueTimeouts.Add(dueTimeout);
            }

            var timeoutsNotCompleted = dueTimeouts.ToDictionary(t => t.Id);

            var dueMessages = dueTimeouts
                .Select(timeout => new DueMessage(timeout.Headers, timeout.Body, async () =>
                {
                    _log.Debug("Completing timeout for message with ID {messageId} (doc ID {documentId})", 
                        timeout.Headers.GetValue(Headers.MessageId), timeout.Id);

                    var filter = Builders<Timeout>.Filter.Eq(t => t.Id, timeout.Id);

                    await _timeouts.DeleteOneAsync(filter).ConfigureAwait(false);


                    timeoutsNotCompleted.Remove(timeout.Id);
                }))
                .ToList();

            return new DueMessagesResult(dueMessages, async () =>
            {
                foreach (var timeoutNotCompleted in timeoutsNotCompleted.Values)
                {
                    try
                    {
                        _log.Debug("Timeout for message with ID {messageId} (doc ID {documentId}) was not completed - will set due time back to {dueTime} now",
                            timeoutNotCompleted.Headers.GetValue(Headers.MessageId), timeoutNotCompleted.Id,
                            timeoutNotCompleted.OriginalDueTimeUtc);

                        var filter = Builders<Timeout>.Filter.Eq(t => t.Id, timeoutNotCompleted.Id);
                        var update = Builders<Timeout>.Update.Set(t => t.DueTimeUtc, timeoutNotCompleted.OriginalDueTimeUtc);

                        await _timeouts.UpdateOneAsync(filter, update).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        _log.Warn("Could not set due time for timeout with doc ID {documentId}: {exception}", timeoutNotCompleted.Id, exception);
                    }
                }
            });
        }

        class Timeout
        {
            public Timeout(Dictionary<string, string> headers, byte[] body, DateTime dueTimeUtc)
            {
                Id = ObjectId.GenerateNewId();
                Headers = headers;
                Body = body;
                DueTimeUtc = dueTimeUtc;
                OriginalDueTimeUtc = dueTimeUtc;
            }

            // the following properties have public setters because the Mongo driver requires it!
            // ReSharper disable MemberCanBePrivate.Local
            public ObjectId Id { get; set; }
            public Dictionary<string, string> Headers { get; set; }
            public byte[] Body { get; set; }
            public DateTime DueTimeUtc { get; set; }
            public DateTime OriginalDueTimeUtc { get; set; }
            // ReSharper restore MemberCanBePrivate.Local
        }
    }
}