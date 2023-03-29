using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;

namespace Rebus.MongoDb.Transport;

/// <summary>
/// <para>Represents a message that needs to be delivered</para>
/// <para>
/// indexes to create: 
/// priority, visible, expiration, id
/// expiration
/// </para>
/// <para>todo: rename "QueuedMessage"</para>
/// </summary>
public class TransportMessageMongoDb
{
    /// <summary>
    /// Standard objectid id.
    /// </summary>
    public ObjectId Id { get; set; }

    /// <summary>
    /// This allows for priority dequeueing
    /// </summary>
    public int Priority { get; set; }

    /// <summary>
    /// expiration time in ticks
    /// </summary>
    public DateTime Expiration { get; set; }

    /// <summary>
    /// Visible time in ticks
    /// </summary>
    public DateTime Visibile { get; set; }

    /// <summary>
    /// HEaders
    /// </summary>
    [BsonDictionaryOptions(MongoDB.Bson.Serialization.Options.DictionaryRepresentation.ArrayOfArrays)]
    public Dictionary<string, string> Headers { get; set; }

    /// <summary>
    /// Body
    /// </summary>
    public byte[] Body { get; set; }
}