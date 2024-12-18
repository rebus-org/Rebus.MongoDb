using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using NUnit.Framework;
using Rebus.Sagas;

namespace Rebus.MongoDb.Tests;

[SetUpFixture]
public class PrepareGlobalBsonSerializer
{
    [OneTimeSetUp]
    [Description("The MongoDB driver has a global BSON serializer, which we'll prep once and for all here.")]
    public void PrettRidiculousButWeWillDoItAnyway()
    {
        BsonSerializer.RegisterSerializer(new ObjectSerializer(_ => true));

        // Default class map for all saga data inheriting from this base class
        BsonClassMap.RegisterClassMap<SagaData>(map =>
        {
            map.MapIdMember(obj => obj.Id).SetSerializer(new GuidSerializer(GuidRepresentation.Standard));
            map.MapMember(obj => obj.Revision);
        });

        /*
         * Default Guid representation to Standard (required for test cases with private saga data which directly
         * implements ISagaData
         */
        BsonSerializer.RegisterSerializer(new GuidSerializer(GuidRepresentation.Standard));
    }
}