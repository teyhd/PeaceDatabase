using System;
using System.Collections.Generic;

namespace PeaceDatabase.WebApi.Exceptions;

/// <summary>
/// Represents a domain validation failure that should result in a 400 response.
/// </summary>
[Serializable]
public sealed class DomainValidationException : Exception
{
    public IReadOnlyDictionary<string, string[]> Errors { get; }

    public DomainValidationException(string message)
        : this(message, null, null)
    {
    }

    public DomainValidationException(string message, IDictionary<string, string[]>? errors)
        : this(message, errors, null)
    {
    }

    public DomainValidationException(string message, IDictionary<string, string[]>? errors, Exception? innerException)
        : base(message, innerException)
    {
        Errors = errors != null
            ? new Dictionary<string, string[]>(errors, StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
    }

}
