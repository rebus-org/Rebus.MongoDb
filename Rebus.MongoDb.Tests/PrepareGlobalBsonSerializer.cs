using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using NUnit.Framework;

namespace Rebus.MongoDb.Tests;

[SetUpFixture]
public class PrepareGlobalBsonSerializer
{
    [OneTimeSetUp]
    [Description("The MongoDB driver has a global BSON serializer, which we'll prep once and for all here.")]
    public void PrettRidiculousButWeWillDoItAnyway() => BsonSerializer.RegisterSerializer(new ObjectSerializer(_ => true));
}