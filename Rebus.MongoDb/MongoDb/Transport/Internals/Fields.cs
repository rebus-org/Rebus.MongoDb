using System;
using System.Collections.Generic;
using System.Text;

namespace Rebus.MongoDb.Transport.Internals
{
   internal static class Fields
   {
      public const string MessageId = "id";
      public const string DestinationQueueName = "q";
      public const string SendTime = "st";
      public const string DeliveryAttempts = "n";
      public const string ReceiveTime = "rt";
      public const string Headers = "h";
      public const string Body = "b";
      public const string Key = "k";
      public const string Value = "v";
   }
}
