using Rebus.Messages;
using Rebus.MongoDb.Transport.Internals;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Rebus.MongoDb.Transport.Model;

/// <summary>
/// Represents a received message, which is just a <see cref="Message"/> with the added capabilities to be ACKed or NACKed
/// </summary>
class MongoDbReceivedMessage : TransportMessage
{
    readonly Func<Task> _ack;
    readonly Func<Task> _nack;
    readonly Func<Task> _renew;

    /// <summary>
    /// Creates the message
    /// </summary>
    public MongoDbReceivedMessage(Dictionary<string, string> headers, byte[] body, Func<Task> ack, Func<Task> nack, Func<Task> renew, int deliveryCount) : base(headers, body)
    {
        DeliveryCount = deliveryCount;
        _ack = ack ?? throw new ArgumentNullException(nameof(ack));
        _nack = nack ?? throw new ArgumentNullException(nameof(nack));
        _renew = renew ?? throw new ArgumentNullException(nameof(renew));

        if (!headers.TryGetValue(Fields.MessageId, out var messageId))
        {
            throw new ArgumentException($"ReceivedMessage model requires that the '{Fields.MessageId}' header is present");
        }

        MessageId = messageId;
    }

    /// <summary>
    /// Gets the message ID
    /// </summary>
    public string MessageId { get; }

    /// <summary>
    /// Gets how many times this message has been delivered to someone. Starts out as 0 (before the first delivery attempt),
    /// and is then incremented on each delivery. This means that the first to receive a message gets to see the value 1,
    /// and so on.
    /// </summary>
    public int DeliveryCount { get; }

    /// <summary>
    /// ACKs the message (deleting it from the underlying storage)
    /// </summary>
    public Task Ack() => _ack();

    /// <summary>
    /// NACKs the message (making it visible again to other consumers)
    /// </summary>
    public Task Nack() => _nack();

    /// <summary>
    /// Renews the lease for this message, prolonging the time it stays invisible to other consumers
    /// </summary>
    /// <returns></returns>
    public Task Renew() => _renew();
}