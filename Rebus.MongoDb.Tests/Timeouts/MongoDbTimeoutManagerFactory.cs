using System;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;
using Rebus.Logging;
using Rebus.MongoDb.Tests.DataBus;
using Rebus.MongoDb.Timeouts;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Timeouts;
using Rebus.Timeouts;

namespace Rebus.MongoDb.Tests.Timeouts
{
    public class MongoDbTimeoutManagerFactory : ITimeoutManagerFactory
    {
        readonly IMongoDatabase _mongoDatabase;
        readonly string _collectionName = $"timeouts_{TestConfig.Suffix}";
        readonly FakeRebusTime _fakeRebusTime = new FakeRebusTime();

        public MongoDbTimeoutManagerFactory()
        {
            _mongoDatabase = MongoTestHelper.GetMongoDatabase();
            DropCollection(_collectionName);
        }

        public ITimeoutManager Create()
        {
            return new MongoDbTimeoutManager(_fakeRebusTime, _mongoDatabase, _collectionName, new ConsoleLoggerFactory(false));
        }

        public void Cleanup()
        {
            DropCollection(_collectionName);
        }

        public string GetDebugInfo()
        {
            var docStrings = _mongoDatabase
                .GetCollection<BsonDocument>(_collectionName)
                .FindAsync(d => true)
                .Result
                .ToListAsync()
                .Result
                .Select(FormatDocument);

            return string.Join(Environment.NewLine, docStrings);
        }

        public void FakeIt(DateTimeOffset fakeTime)
        {
            _fakeRebusTime.SetTime(fakeTime);
        }

        static string FormatDocument(BsonDocument document)
        {
            return document.ToString();
        }

        void DropCollection(string collectionName)
        {
            _mongoDatabase.DropCollectionAsync(collectionName).Wait();
        }
    }
}