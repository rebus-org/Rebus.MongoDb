﻿using System;
using MongoDB.Bson;
using MongoDB.Driver;
using Rebus.Tests.Contracts;

namespace Rebus.MongoDb.Tests
{
    public class MongoTestHelper
    {
        public const string TestCategory = "mongodb";

        public static MongoUrl GetUrl()
        {
            var suffix = TestConfig.Suffix + "_" + TargetFramework;

            var databaseName = $"rebus2_test_{suffix}".TrimEnd('_');

            var mongoUrl = new MongoUrl($"mongodb://admin:123456##@localhost/{databaseName}?authSource=admin");

            Console.WriteLine("Using MongoDB {0}", mongoUrl);

            return mongoUrl;
        }

        public static string TargetFramework
        {
            get
            {
                #if NET461
                return "net461";
                #elif NETCOREAPP21
                return "netcoreapp21";
                #endif
            }
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
                GuidRepresentation = GuidRepresentation.Standard,
                WriteConcern = WriteConcern.Acknowledged
            };
            return mongoClient.GetDatabase(url.DatabaseName, settings);
        }

        static IMongoClient GetMongoClient()
        {
            var url = GetUrl();

            return new MongoClient(url);
        }
    }
}