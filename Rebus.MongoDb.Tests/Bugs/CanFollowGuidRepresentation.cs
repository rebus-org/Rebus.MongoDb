using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using NUnit.Framework;
using Rebus.Logging;
using Rebus.MongoDb.Sagas;
using Rebus.Sagas;
using Rebus.Tests.Contracts;

namespace Rebus.MongoDb.Tests.Bugs;

[TestFixture]
public class CanFollowGuidRepresentation : FixtureBase
{
    static readonly IEnumerable<ISagaCorrelationProperty> None = Enumerable.Empty<ISagaCorrelationProperty>();
    MongoDbSagaStorage _sagaStorage;

    [OneTimeSetUp]
    protected void Init()
    {
        BsonClassMap.RegisterClassMap<MyGuidRepresentationSagaData>(map =>
        {
            map.MapMember(obj => obj.StandardId)
                .SetSerializer(new GuidSerializer(GuidRepresentation.Standard));

            map.MapMember(obj => obj.CSharpLegacyId)
                .SetSerializer(new GuidSerializer(GuidRepresentation.CSharpLegacy));

            map.MapMember(obj => obj.JavaLegacyId)
                .SetSerializer(new GuidSerializer(GuidRepresentation.JavaLegacy));

            map.MapMember(obj => obj.PythonLegacyId)
                .SetSerializer(new GuidSerializer(GuidRepresentation.PythonLegacy));
        });

        BsonClassMap.RegisterClassMap<StandardIdSagaData>(map =>
        {
            map.MapIdMember(obj => obj.Id)
                .SetSerializer(new GuidSerializer(GuidRepresentation.Standard));

            map.MapMember(obj => obj.Revision);
            map.MapMember(obj => obj.State);
        });

        BsonClassMap.RegisterClassMap<CSharpLegacyIdSagaData>(map =>
        {
            map.MapIdMember(obj => obj.Id)
                .SetSerializer(new GuidSerializer(GuidRepresentation.CSharpLegacy));

            map.MapMember(obj => obj.Revision);
            map.MapMember(obj => obj.State);
        });

        BsonClassMap.RegisterClassMap<JavaLegacyIdSagaData>(map =>
        {
            map.MapIdMember(obj => obj.Id)
                .SetSerializer(new GuidSerializer(GuidRepresentation.JavaLegacy));

            map.MapMember(obj => obj.Revision);
            map.MapMember(obj => obj.State);
        });
        BsonClassMap.RegisterClassMap<PythonLegacyIdSagaData>(map =>
        {
            map.MapIdMember(obj => obj.Id)
                .SetSerializer(new GuidSerializer(GuidRepresentation.PythonLegacy));

            map.MapMember(obj => obj.Revision);
            map.MapMember(obj => obj.State);
        });
    }

    protected override void SetUp()
    {
        var database = MongoTestHelper.GetMongoDatabase();
        database.DropCollection(nameof(MyGuidRepresentationSagaData));
        database.DropCollection(nameof(StandardIdSagaData));
        database.DropCollection(nameof(CSharpLegacyIdSagaData));
        database.DropCollection(nameof(JavaLegacyIdSagaData));
        database.DropCollection(nameof(PythonLegacyIdSagaData));

        _sagaStorage = new MongoDbSagaStorage(database, new ConsoleLoggerFactory(colored: false));
        _sagaStorage.Initialize();
    }

    [Test]
    [TestCase(nameof(MyGuidRepresentationSagaData.StandardId))]
    [TestCase(nameof(MyGuidRepresentationSagaData.CSharpLegacyId))]
    [TestCase(nameof(MyGuidRepresentationSagaData.JavaLegacyId))]
    [TestCase(nameof(MyGuidRepresentationSagaData.PythonLegacyId))]
    public async Task CanFind(string propertyName)
    {
        var id = Guid.NewGuid();
        var expected = new MyGuidRepresentationSagaData
        {
            Id = Guid.NewGuid(),
            StandardId = propertyName == nameof(MyGuidRepresentationSagaData.StandardId) ? id : Guid.NewGuid(),
            CSharpLegacyId = propertyName == nameof(MyGuidRepresentationSagaData.CSharpLegacyId) ? id : Guid.NewGuid(),
            JavaLegacyId = propertyName == nameof(MyGuidRepresentationSagaData.JavaLegacyId) ? id : Guid.NewGuid(),
            PythonLegacyId = propertyName == nameof(MyGuidRepresentationSagaData.PythonLegacyId) ? id : Guid.NewGuid(),
        };
        await _sagaStorage.Insert(expected, None);

        var actual = await _sagaStorage.Find(typeof(MyGuidRepresentationSagaData), propertyName, id);
        Assert.That(actual, Is.Not.Null
            .And.Property("Id").EqualTo(expected.Id));
    }

    [Test]
    [TestCase(nameof(StandardIdSagaData))]
    [TestCase(nameof(CSharpLegacyIdSagaData))]
    [TestCase(nameof(JavaLegacyIdSagaData))]
    [TestCase(nameof(PythonLegacyIdSagaData))]
    public async Task CanUpdate(string implementation)
    {
        var id = Guid.NewGuid();
        var initial = CreateSagaDataWithState(implementation, id, "Initial");
        await _sagaStorage.Insert(initial, None);

        await _sagaStorage.Update(CreateSagaDataWithState(implementation, id, "Updated"), None);

        var actual = await _sagaStorage.Find(initial.GetType(), nameof(ISagaData.Id), id);
        Assert.That(actual, Is.Not.Null
            .And.InstanceOf(initial.GetType())
            .With.Property("State").EqualTo("Updated"));
    }

    [Test]
    [TestCase(nameof(StandardIdSagaData))]
    [TestCase(nameof(CSharpLegacyIdSagaData))]
    [TestCase(nameof(JavaLegacyIdSagaData))]
    [TestCase(nameof(PythonLegacyIdSagaData))]
    public async Task CanDelete(string implementation)
    {
        var id = Guid.NewGuid();
        var initial = CreateSagaDataWithState(implementation, id, "Initial");
        await _sagaStorage.Insert(initial, None);

        await _sagaStorage.Delete(initial);

        var actual = await _sagaStorage.Find(initial.GetType(), nameof(ISagaData.Id), id);
        Assert.That(actual, Is.Null);
    }

    private static ISagaDataWithState CreateSagaDataWithState(string implementation, Guid id, string state)
    {
        return implementation switch
        {
            nameof(StandardIdSagaData) => new StandardIdSagaData { Id = id, State = state },
            nameof(CSharpLegacyIdSagaData) => new CSharpLegacyIdSagaData { Id = id, State = state },
            nameof(JavaLegacyIdSagaData) => new JavaLegacyIdSagaData { Id = id, State = state },
            nameof(PythonLegacyIdSagaData) => new PythonLegacyIdSagaData { Id = id, State = state },
            _ => throw new ArgumentException("Unknown implementation", nameof(implementation))
        };
    }

    class MyGuidRepresentationSagaData : SagaData
    {
        public Guid StandardId { get; init; }
        public Guid CSharpLegacyId { get; init; }
        public Guid JavaLegacyId { get; init; }
        public Guid PythonLegacyId { get; init; }
    }

    interface ISagaDataWithState : ISagaData
    {
        string State { get; set; }
    }

    class StandardIdSagaData : ISagaDataWithState
    {
        public Guid Id { get; set; }
        public int Revision { get; set; }
        public string State { get; set; }
    }

    class CSharpLegacyIdSagaData : ISagaDataWithState
    {
        public Guid Id { get; set; }
        public int Revision { get; set; }
        public string State { get; set; }
    }

    class JavaLegacyIdSagaData : ISagaDataWithState
    {
        public Guid Id { get; set; }
        public int Revision { get; set; }
        public string State { get; set; }
    }

    class PythonLegacyIdSagaData : ISagaDataWithState
    {
        public Guid Id { get; set; }
        public int Revision { get; set; }
        public string State { get; set; }
    }
}