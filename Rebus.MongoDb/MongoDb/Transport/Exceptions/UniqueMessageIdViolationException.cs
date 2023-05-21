using System;
using System.Collections.Generic;
using System.Text;

namespace Rebus.MongoDb.Transport.Exceptions
{
   /// <summary>
   /// Exception thrown when a message with an explicitly set ID is sent, and a message already exists with that ID
   /// </summary>
   [Serializable]
   public class UniqueMessageIdViolationException : Exception
   {
      /// <summary>
      /// Gets the problematic ID
      /// </summary>
      public string Id { get; }

      /// <summary>
      /// Constructs the exception with the given ID, generating a sensible message at the same time
      /// </summary>
      public UniqueMessageIdViolationException(string id) : base($"Cannot send message with ID {id} because a message already exists with that ID")
      {
         Id = id ?? throw new ArgumentNullException(nameof(id));
      }
   }
}
