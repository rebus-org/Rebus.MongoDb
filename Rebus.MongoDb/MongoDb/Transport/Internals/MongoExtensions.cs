using MongoDB.Driver;
using System;

namespace Rebus.MongoDb.Transport.Internals;

static class MongoExtensions
{
    internal static IMongoDatabase GetMongoDatabase(this MongoUrl mongoUrl)
    {
        if (mongoUrl == null) throw new ArgumentNullException(nameof(mongoUrl));

        var databaseName = mongoUrl.DatabaseName;

        if (string.IsNullOrWhiteSpace(databaseName))
        {
            throw new ArgumentException("The MongoDB URL does not contain a database name!");
        }

        return new MongoClient(mongoUrl).GetDatabase(databaseName);
    }

    internal static IMongoDatabase GetMongoDatabase(this string connectionString)
    {
        if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));

        return new MongoUrl(connectionString).GetMongoDatabase();
    }
}