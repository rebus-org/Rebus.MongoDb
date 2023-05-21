using Rebus.Logging;
using Rebus.MongoDb.Transport;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Transports;
using Rebus.Threading.TaskParallelLibrary;
using Rebus.Time;
using Rebus.Transport;
using System;
using System.Collections.Generic;

namespace Rebus.MongoDb.Tests.Transport.Contract.Factories
{
   public class MongoDbTransportFactory : ITransportFactory
   {
      readonly HashSet<string> _tablesToDrop = new HashSet<string>();
      readonly List<IDisposable> _disposables = new List<IDisposable>();

      public MongoDbTransportFactory()
      {
         MongoTestHelper.DropMongoDatabase();
      }

      public ITransport CreateOneWayClient()
      {
         var consoleLoggerFactory = new ConsoleLoggerFactory(false);
         var transport = new MongoDbTransport(consoleLoggerFactory, new Config.MongoDbTransportOptions(MongoTestHelper.GetUrl()));


         return transport;
      }

      public ITransport Create(string inputQueueAddress)
      {
         var tableName = ("RebusMessages_" + TestConfig.Suffix).TrimEnd('_');

         MongoTestHelper.DropCollection(tableName);

         _tablesToDrop.Add(tableName);

         var consoleLoggerFactory = new ConsoleLoggerFactory(false);

         var transport = new MongoDbTransport(consoleLoggerFactory,
             new Config.MongoDbTransportOptions(MongoTestHelper.GetUrl(), tableName).SetInputQueueName(inputQueueAddress));


         return transport;
      }

      public void CleanUp()
      {
         _disposables.ForEach(d => d.Dispose());
         _disposables.Clear();

         foreach (var table in _tablesToDrop)
         {
            MongoTestHelper.DropCollection(table);
         }

         _tablesToDrop.Clear();
      }
   }
}
