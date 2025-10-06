using System;
using System.Runtime.Serialization;

namespace PeaceDatabase.WebApi.Exceptions;

/// <summary>
/// Thrown when a requested resource cannot be located.
/// </summary>
[Serializable]
public sealed class ResourceNotFoundException : Exception
{
    public ResourceNotFoundException(string message)
        : base(message)
    {
    }

    public ResourceNotFoundException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }

    private ResourceNotFoundException(SerializationInfo info, StreamingContext context)
        : base(info, context)
    {
    }
}
