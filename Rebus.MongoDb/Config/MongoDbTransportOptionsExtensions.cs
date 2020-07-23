using System;

namespace Rebus.Config
{
    /// <summary>
    /// Provides extensions for managing <seealso cref="MongoDbTransportOptions"/>
    /// </summary>
    public static class MongoDbTransportOptionsExtensions
    {
        /// <summary>
        /// Flags the transport as only being used for sending
        /// </summary>
        public static TTransportOptions AsOneWayClient<TTransportOptions>(this TTransportOptions options) where TTransportOptions : MongoDbTransportOptions
        {
            options.InputQueueName = null;
            return options;
        }

        /// <summary>
        /// Configures the transport to read from <paramref name="inputQueueName"/>
        /// </summary>
        public static TTransportOptions ReadFrom<TTransportOptions>(this TTransportOptions options, string inputQueueName) where TTransportOptions : MongoDbTransportOptions
        {
            options.InputQueueName = inputQueueName;
            return options;
        }
    }
}
