using System;

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

}
