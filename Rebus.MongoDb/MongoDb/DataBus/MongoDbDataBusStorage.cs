using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using Rebus.DataBus;
using Rebus.Time;

namespace Rebus.MongoDb.DataBus;

/// <summary>
/// Implementation of <see cref="IDataBusStorage"/> that uses MongoDB to store attachments
/// </summary>
public class MongoDbDataBusStorage : IDataBusStorage, IDataBusStorageManagement
{
    readonly IRebusTime _rebusTime;
    readonly IMongoDatabase _database;
    readonly string _bucketName;
    readonly GridFSBucket _bucket;

    /// <summary>
    /// Creates the storage using the given Mongo database
    /// </summary>
    public MongoDbDataBusStorage(IRebusTime rebusTime, IMongoDatabase database, string bucketName)
    {
        if (string.IsNullOrWhiteSpace(bucketName)) throw new ArgumentException("Please pass a valid MongoDB GridFS bucket name", nameof(bucketName));
        _rebusTime = rebusTime ?? throw new ArgumentNullException(nameof(rebusTime));
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

        metadataDocument[MetadataKeys.SaveTime] = _rebusTime.Now.ToString("o");

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

            await UpdateLastReadTime(id, _rebusTime.Now);

            return downloadStream;
        }
        catch (GridFSFileNotFoundException exception)
        {
            throw new ArgumentException($"Could not read attachment with ID '{id}'", exception);
        }
    }

    /// <inheritdoc />
    public IEnumerable<string> Query(TimeRange readTime = null, TimeRange saveTime = null)
    {
        //var filter = new BsonDocument();

        //if (readTime != null)
        //{
        //    filter.Add($"metadata.{MetadataKeys.ReadTime}", new BsonDocument
        //    {
        //        {"$gte", BsonValue.Create(readTime.From?.UtcDateTime ?? DateTime.MinValue)},
        //        {"$lt", BsonValue.Create(readTime.To?.UtcDateTime ?? DateTime.MaxValue)}
        //    });
        //}
        //if (saveTime != null)
        //{
        //    filter.Add($"metadata.{MetadataKeys.SaveTime}", new BsonDocument
        //    {
        //        {"$gte", BsonValue.Create(saveTime.From?.UtcDateTime ?? DateTime.MinValue)},
        //        {"$lt", BsonValue.Create(saveTime.To?.UtcDateTime ?? DateTime.MaxValue)}
        //    });
        //}

        using (var cursor = _bucket.Find(new ExpressionFilterDefinition<GridFSFileInfo>(i => true)))
        {
            foreach (var doc in cursor.ToEnumerable())
            {
                var metadata = doc.Metadata;

                if (metadata.TryGetValue(MetadataKeys.ReadTime, out var readTimeString))
                {
                    if (DateTimeOffset.TryParseExact(readTimeString.AsString, "o", CultureInfo.InvariantCulture,
                            DateTimeStyles.RoundtripKind, out var readTimeValue))
                    {
                        if (readTime?.From != null && readTimeValue < readTime.From) continue;
                        if (readTime?.To != null && readTimeValue >= readTime.To) continue;
                    }
                }

                if (metadata.TryGetValue(MetadataKeys.SaveTime, out var saveTimeString))
                {
                    if (DateTimeOffset.TryParseExact(saveTimeString.AsString, "o", CultureInfo.InvariantCulture,
                            DateTimeStyles.RoundtripKind, out var saveTimeValue))
                    {
                        if (saveTime?.From != null && saveTimeValue < saveTime.From) continue;
                        if (saveTime?.To != null && saveTimeValue >= saveTime.To) continue;
                    }
                }

                yield return doc.Filename;
            }
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

            metadata[MetadataKeys.Length] = result.Length.ToString();

            return metadata;
        }
        catch (GridFSFileNotFoundException exception)
        {
            throw new ArgumentException($"Could not get metadata for attachment with ID '{id}'", exception);
        }
    }

    /// <inheritdoc />
    public async Task Delete(string id)
    {
        try
        {
            var result = await GetGridFsFileInfo(id);

            await _bucket.DeleteAsync(result.Id);
        }
        catch (GridFSFileNotFoundException)
        {
        }
        catch (Exception exception)
        {
            throw new ArgumentException($"Could not delete attachment with ID '{id}'", exception);
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
        var filter = new ExpressionFilterDefinition<GridFSFileInfo>(f => f.Filename == id);

        using (var cursor = await _bucket.FindAsync(filter))
        {
            var result = await cursor.FirstOrDefaultAsync();

            return result;
        }
    }
}