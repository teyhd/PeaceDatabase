using System;
using System.Runtime.Serialization;

namespace PeaceDatabase.WebApi.Exceptions;

/// <summary>
/// Represents an operation that cannot be performed due to missing authentication or authorization.
/// </summary>
[Serializable]
public sealed class UnauthorizedOperationException : Exception
{
    /// <summary>
    /// When true the caller is not authenticated (401). Otherwise forbidden (403).
    /// </summary>
    public bool RequiresAuthentication { get; }

    public UnauthorizedOperationException(string message, bool requiresAuthentication = false)
        : base(message)
    {
        RequiresAuthentication = requiresAuthentication;
    }

    public UnauthorizedOperationException(string message, bool requiresAuthentication, Exception? innerException)
        : base(message, innerException)
    {
        RequiresAuthentication = requiresAuthentication;
    }

#pragma warning disable SYSLIB0051 // Binary serialization is obsolete but preserved for compatibility with existing consumers.
    [Obsolete("Serialization constructor is obsolete.", DiagnosticId = "SYSLIB0051")]
    private UnauthorizedOperationException(SerializationInfo info, StreamingContext context)
        : base(info, context)
    {
        RequiresAuthentication = info.GetBoolean(nameof(RequiresAuthentication));
    }

    [Obsolete("Serialization support is obsolete.", DiagnosticId = "SYSLIB0051")]
    public override void GetObjectData(SerializationInfo info, StreamingContext context)
    {
        ArgumentNullException.ThrowIfNull(info);
        info.AddValue(nameof(RequiresAuthentication), RequiresAuthentication);
        base.GetObjectData(info, context);
    }
#pragma warning restore SYSLIB0051
}
