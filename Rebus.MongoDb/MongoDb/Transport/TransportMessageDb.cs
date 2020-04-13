using MongoDB.Bson;

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
        /// maybe an ObjectId is enough ? if ot consider a counter collection
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

        public object Headers { get; set; }

        public object Body { get; set; }
    }
}
