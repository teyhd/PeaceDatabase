using System;
using System.Runtime.Serialization;

namespace PeaceDatabase.WebApi.Exceptions;

/// <summary>
/// Represents a conflicting state detected during a request.
/// </summary>
[Serializable]
public sealed class ConflictException : Exception
{
    public ConflictException(string message)
        : base(message)
    {
    }

    public ConflictException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }

#pragma warning disable SYSLIB0051 // Binary serialization is obsolete but preserved for compatibility with existing consumers.
    [Obsolete("Serialization constructor is obsolete.", DiagnosticId = "SYSLIB0051")]
    private ConflictException(SerializationInfo info, StreamingContext context)
        : base(info, context)
    {
    }
#pragma warning restore SYSLIB0051
}
