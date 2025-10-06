using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

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

    private DomainValidationException(SerializationInfo info, StreamingContext context)
        : base(info, context)
    {
        Errors = (info.GetValue(nameof(Errors), typeof(Dictionary<string, string[]>)) as Dictionary<string, string[]>)
                 ?? new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
    }

    public override void GetObjectData(SerializationInfo info, StreamingContext context)
    {
        ArgumentNullException.ThrowIfNull(info);
        var payload = Errors is Dictionary<string, string[]> dict
            ? dict
            : Errors.ToDictionary(k => k.Key, v => v.Value, StringComparer.OrdinalIgnoreCase);
        info.AddValue(nameof(Errors), payload);
        base.GetObjectData(info, context);
    }
}
