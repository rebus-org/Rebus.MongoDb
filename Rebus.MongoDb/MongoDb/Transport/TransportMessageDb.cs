using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Collections.Generic;

namespace Rebus.MongoDb.Transport
{
    /// <summary>
    /// Represents a message that needs to be delivered
    /// 
    /// indexes to create: 
    /// priority, visible, expiration, id
    /// expiration
    /// 
    /// todo: rename "QueuedMessage"
    /// </summary>
    public class TransportMessageDb
    {
        /// <summary>
        /// identity column in SQL server (it gives sorting)
        /// maybe an ObjectId is enough ? if not consider a counter collection
        /// </summary>
        public ObjectId Id { get; set; }

        public int Priority { get; set; }

        /// <summary>
        /// expiration time in ticks
        /// </summary>
        public long Expiration { get; set; }

        /// <summary>
        /// Visible time in ticks
        /// </summary>
        public long Visibile { get; set; }

        [BsonDictionaryOptions(MongoDB.Bson.Serialization.Options.DictionaryRepresentation.ArrayOfArrays)]
        public Dictionary<string, string> Headers { get; set; }

        public object Body { get; set; }
    }
}
