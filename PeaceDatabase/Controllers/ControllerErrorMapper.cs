using System;
using System.Collections.Generic;
using PeaceDatabase.Core.Models;
using PeaceDatabase.WebApi.Exceptions;

namespace PeaceDatabase.WebApi.Controllers;

internal static class ControllerErrorMapper
{
    public static void ThrowIfFailed((bool Ok, string? Error) result, string db, string? id = null)
    {
        if (result.Ok)
        {
            return;
        }

        throw MapException(result.Error, db, id);
    }

    public static Document EnsureDocument((bool Ok, Document? Doc, string? Error) result, string db, string? id = null)
    {
        if (!result.Ok)
        {
            throw MapException(result.Error, db, id);
        }

        return result.Doc ?? throw new InvalidOperationException("Operation succeeded but did not return a document.");
    }

    private static Exception MapException(string? error, string db, string? id)
    {
        _ = id; // reserved for future mapping rules that may rely on resource identifiers.
        if (string.IsNullOrWhiteSpace(error))
        {
            return new InvalidOperationException("Operation failed without error details.");
        }

        var normalized = error.Trim();
        var lower = normalized.ToLowerInvariant();

        if (lower.Contains("not found", StringComparison.Ordinal))
        {
            return new ResourceNotFoundException(normalized);
        }

        if (lower.Contains("conflict", StringComparison.Ordinal))
        {
            return new ConflictException(normalized);
        }

        if (lower.Contains("missing _id", StringComparison.Ordinal))
        {
            return new DomainValidationException(
                "Request validation failed",
                new Dictionary<string, string[]>
                {
                    ["_id"] = new[] { "The _id field is required." }
                });
        }

        if (lower.Contains("missing _rev", StringComparison.Ordinal))
        {
            return new DomainValidationException(
                "Request validation failed",
                new Dictionary<string, string[]>
                {
                    ["_rev"] = new[] { "The _rev field is required." }
                });
        }

        if (lower.Contains("new document must not provide _rev", StringComparison.Ordinal))
        {
            return new DomainValidationException(
                "Request validation failed",
                new Dictionary<string, string[]>
                {
                    ["_rev"] = new[] { "A new document must not specify _rev." }
                });
        }

        if (lower.Contains("bad database name", StringComparison.Ordinal))
        {
            return new DomainValidationException(
                "Request validation failed",
                new Dictionary<string, string[]>
                {
                    ["db"] = new[] { normalized }
                });
        }

        if (lower.Contains("database not found", StringComparison.Ordinal))
        {
            return new ResourceNotFoundException($"Database '{db}' was not found.");
        }

        return new InvalidOperationException(normalized);
    }
}
