using NUnit.Framework;
using Rebus.MongoDb.Tests.Transport.Contracts.Factories;
using Rebus.Tests.Contracts.Transports;

namespace Rebus.MongoDb.Tests.Transport.Contracts;

[TestFixture, Category(MongoTestHelper.TestCategory)]
public class MongoDbTransportMessageExpiration : MessageExpiration<MongoDbTransportFactory> { }