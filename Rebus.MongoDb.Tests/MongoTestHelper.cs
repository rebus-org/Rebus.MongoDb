using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using Rebus.Tests.Contracts;

namespace Rebus.MongoDb.Tests;

public class MongoTestHelper
{
    public const string TestCategory = "mongodb";

    static MongoTestHelper()
    {
        // One-time setup, use Standard Guid representation by default
        BsonSerializer.RegisterSerializer(new GuidSerializer(GuidRepresentation.Standard));
    }

    public static MongoUrl GetUrl()
    {
        var suffix = TestConfig.Suffix;

        var databaseName = $"rebus2_test_{suffix}".TrimEnd('_');

        var serverUrl = Environment.GetEnvironmentVariable("REBUS_MONGODB") ?? "mongodb://localhost";
        var builder = new MongoUrlBuilder(serverUrl) { DatabaseName = databaseName };
        var mongoUrl = builder.ToMongoUrl();

        Console.WriteLine("Using MongoDB {0}", mongoUrl);

        return mongoUrl;
    }

    internal static void DropCollection(string collectionName)
    {
        GetMongoDatabase().DropCollection(collectionName);
    }

    public static IMongoDatabase GetMongoDatabase()
    {
        return GetMongoDatabase(GetMongoClient());
    }

    public static void DropMongoDatabase()
    {
        GetMongoClient().DropDatabaseAsync(GetUrl().DatabaseName).Wait();
    }

    static IMongoDatabase GetMongoDatabase(IMongoClient mongoClient)
    {
        var url = GetUrl();
        var settings = new MongoDatabaseSettings
        {
            WriteConcern = WriteConcern.Acknowledged,
        };
        return mongoClient.GetDatabase(url.DatabaseName, settings);
    }

    static IMongoClient GetMongoClient()
    {
        var url = GetUrl();

        return new MongoClient(url);
    }
}