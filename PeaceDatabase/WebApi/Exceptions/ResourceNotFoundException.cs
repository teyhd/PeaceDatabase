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

#pragma warning disable SYSLIB0051 // Binary serialization is obsolete but preserved for compatibility with existing consumers.
    [Obsolete("Serialization constructor is obsolete.", DiagnosticId = "SYSLIB0051")]
    private ResourceNotFoundException(SerializationInfo info, StreamingContext context)
        : base(info, context)
    {
    }
#pragma warning restore SYSLIB0051
}
