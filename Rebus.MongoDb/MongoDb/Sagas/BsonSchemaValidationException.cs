using System;
using Rebus.Exceptions;

namespace Rebus.MongoDb.Sagas;

/// <summary>
/// Exception thrown by Rebus if it encounters a saga data type that would not work with Rebus.
/// </summary>
public class BsonSchemaValidationException : Exception, IFailFastException
{
    /// <summary>
    /// Gets the problematic type
    /// </summary>
    public Type SagaDataType { get; }

    /// <summary>
    /// Creates the exception
    /// </summary>
    public BsonSchemaValidationException(Type sagaDataType, string message) : base(message)
    {
        SagaDataType = sagaDataType;
    }
}