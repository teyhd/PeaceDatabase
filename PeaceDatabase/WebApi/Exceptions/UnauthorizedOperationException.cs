using System;

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

}
