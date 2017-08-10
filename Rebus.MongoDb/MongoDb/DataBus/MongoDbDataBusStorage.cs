using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using Rebus.DataBus;
using Rebus.Time;

namespace Rebus.MongoDb.DataBus
{
    /// <summary>
    /// Implementation of <see cref="IDataBusStorage"/> that uses MongoDB to store attachments
    /// </summary>
    public class MongoDbDataBusStorage : IDataBusStorage
    {
        readonly IMongoDatabase _database;
        readonly string _bucketName;
        readonly GridFSBucket _bucket;

        /// <summary>
        /// Creates the storage using the given Mongo database
        /// </summary>
        public MongoDbDataBusStorage(IMongoDatabase database, string bucketName)
        {
            if (string.IsNullOrWhiteSpace(bucketName)) throw new ArgumentException("Please pass a valid MongoDB GridFS bucket name", nameof(bucketName));
            _database = database ?? throw new ArgumentNullException(nameof(database));

            _bucketName = bucketName;
            _bucket = new GridFSBucket(_database, new GridFSBucketOptions
            {
                BucketName = _bucketName,
                WriteConcern = WriteConcern.Acknowledged,
                ReadPreference = ReadPreference.SecondaryPreferred,
            });
        }

        /// <inheritdoc />
        public async Task Save(string id, Stream source, Dictionary<string, string> metadata = null)
        {
            var metadataDocument = metadata?.ToBsonDocument() ?? new BsonDocument();

            metadataDocument[MetadataKeys.SaveTime] = RebusTime.Now.ToString("o");

            await _bucket.UploadFromStreamAsync(id, source, new GridFSUploadOptions
            {
                Metadata = metadataDocument
            });
        }

        /// <inheritdoc />
        public async Task<Stream> Read(string id)
        {
            try
            {
                var downloadStream = await _bucket.OpenDownloadStreamByNameAsync(id);

                await UpdateLastReadTime(id, RebusTime.Now);

                return downloadStream;
            }
            catch (GridFSFileNotFoundException exception)
            {
                throw new ArgumentException($"Could not read attachment with ID '{id}'", exception);
            }
        }

        /// <inheritdoc />
        public async Task<Dictionary<string, string>> ReadMetadata(string id)
        {
            try
            {
                var result = await GetGridFsFileInfo(id);
                var metadataDocument = result.Metadata;

                var metadata = new Dictionary<string, string>();

                foreach (var kvp in metadataDocument)
                {
                    metadata[kvp.Name] = kvp.Value.ToString();
                }

                // Assert.That<string>(dictionary["Rbs2DataBusLength"], (IResolveConstraint) Is.EqualTo((object) "3"));
                // Assert.That<string>(dictionary["Rbs2DataBusSaveTime"], (IResolveConstraint)Is.EqualTo((object)fakeTime.ToString("O")));

                metadata[MetadataKeys.Length] = result.Length.ToString();

                return metadata;
            }
            catch (GridFSFileNotFoundException exception)
            {
                throw new ArgumentException($"Could not get metadata for attachment with ID '{id}'", exception);
            }
        }

        async Task UpdateLastReadTime(string id, DateTimeOffset now)
        {
            var gridFsFileInfo = await GetGridFsFileInfo(id);

            gridFsFileInfo.Metadata[MetadataKeys.ReadTime] = now.ToString("o");

            var files = _database.GetCollection<BsonDocument>($"{_bucketName}.files");

            var criteria = new JsonFilterDefinition<BsonDocument>($@"{{'filename': '{id}'}}");
            var update = new JsonUpdateDefinition<BsonDocument>($@"{{$set: {{'metadata.{MetadataKeys.ReadTime}': '{now:o}'}} }}");

            await files.UpdateOneAsync(criteria, update);
        }

        async Task<GridFSFileInfo> GetGridFsFileInfo(string id)
        {
            var cursor = await _bucket.FindAsync(new ExpressionFilterDefinition<GridFSFileInfo>(f => f.Filename == id));
            var result = await cursor.FirstOrDefaultAsync();
            return result;
        }
    }
}