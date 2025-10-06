using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Infrastructure;
using Microsoft.Extensions.Logging;
using PeaceDatabase.WebApi.Exceptions;

namespace PeaceDatabase.WebApi.Middleware;

/// <summary>
/// Centralized exception handling middleware that converts exceptions into RFC 7807 <see cref="ProblemDetails"/> responses.
/// The mapping is intentionally compact so new exception types can be added by extending <see cref="MapException"/>.
/// </summary>
public sealed class ExceptionHandlingMiddleware : IMiddleware
{
    private static readonly Uri ValidationType = new("https://example.com/errors/validation");
    private static readonly Uri DomainValidationType = new("https://example.com/errors/domain-validation");
    private static readonly Uri NotFoundType = new("https://example.com/errors/not-found");
    private static readonly Uri ConflictType = new("https://example.com/errors/conflict");
    private static readonly Uri UnauthorizedType = new("https://example.com/errors/unauthorized");
    private static readonly Uri ForbiddenType = new("https://example.com/errors/forbidden");
    private static readonly Uri InternalType = new("https://example.com/errors/internal");

    private readonly ILogger<ExceptionHandlingMiddleware> _logger;
    private readonly ProblemDetailsFactory _problemDetailsFactory;

    public ExceptionHandlingMiddleware(ILogger<ExceptionHandlingMiddleware> logger, ProblemDetailsFactory problemDetailsFactory)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _problemDetailsFactory = problemDetailsFactory ?? throw new ArgumentNullException(nameof(problemDetailsFactory));
    }

    public async Task InvokeAsync(HttpContext context, RequestDelegate next)
    {
        try
        {
            await next(context);
        }
        catch (Exception ex)
        {
            await HandleExceptionAsync(context, ex);
        }
    }

    public Task HandleExceptionAsync(HttpContext context, Exception? exception)
    {
        if (context.Response.HasStarted)
        {
            _logger.LogWarning("Cannot write problem response because the response has already started for {Path}", context.Request.Path);
            return Task.CompletedTask;
        }

        var traceId = Activity.Current?.Id ?? context.TraceIdentifier;

        var mapping = MapException(exception);
        var problem = _problemDetailsFactory.CreateProblemDetails(
            context,
            statusCode: mapping.StatusCode,
            title: mapping.Title,
            type: mapping.Type.ToString(),
            detail: mapping.Detail);

        problem.Extensions["traceId"] = traceId;

        if (mapping.Errors?.Count > 0)
        {
            problem.Extensions["errors"] = mapping.Errors;
        }

        context.Response.Clear();
        context.Response.StatusCode = mapping.StatusCode;
        context.Response.ContentType = "application/problem+json";

        LogException(mapping.LogLevel, context, exception, traceId);

        return context.Response.WriteAsJsonAsync(problem);
    }

    private (int StatusCode, Uri Type, string Title, string? Detail,
         IReadOnlyDictionary<string, string[]>? Errors, LogLevel LogLevel)
        MapException(Exception? exception)
    {
        if (exception is null)
        {
            return (StatusCodes.Status500InternalServerError, InternalType, "Unexpected server error", null, null, LogLevel.Error);
        }

        return exception switch
        {
            DomainValidationException dve => (
                StatusCodes.Status400BadRequest,
                DomainValidationType,
                "Domain validation failed",
                dve.Message,
                dve.Errors.Count > 0 ? dve.Errors : null,
                LogLevel.Warning),

            ResourceNotFoundException rnf => (
                StatusCodes.Status404NotFound,
                NotFoundType,
                "Resource not found",
                rnf.Message,
                null,
                LogLevel.Information),

            ConflictException conflict => (
                StatusCodes.Status409Conflict,
                ConflictType,
                "Operation conflict",
                conflict.Message,
                null,
                LogLevel.Warning),

            UnauthorizedOperationException unauthorized => (
                unauthorized.RequiresAuthentication ? StatusCodes.Status401Unauthorized : StatusCodes.Status403Forbidden,
                unauthorized.RequiresAuthentication ? UnauthorizedType : ForbiddenType,
                unauthorized.RequiresAuthentication ? "Authentication required" : "Operation is forbidden",
                unauthorized.Message,
                null,
                LogLevel.Warning),

            JsonException => BuildValidationResult("Malformed JSON payload", "body", "The request body contains invalid JSON."),
            FormatException => BuildValidationResult("Request validation failed", "body", "The request payload format is invalid."),
            ArgumentException arg => BuildValidationResult("Request validation failed", arg.ParamName ?? "argument", arg.Message),

            // HTTP 499 is non-standard but supported by Kestrel. It reflects a cancelled request without blaming the server.
            OperationCanceledException => (
                StatusCodes.Status499ClientClosedRequest,
                ValidationType,
                "Request was cancelled",
                "The operation was cancelled by the caller.",
                null,
                LogLevel.Information),

            _ => (
                StatusCodes.Status500InternalServerError,
                InternalType,
                "Unexpected server error",
                null,
                null,
                LogLevel.Error)
        };
    }

    private static (int StatusCode, Uri Type, string Title, string? Detail,
               IReadOnlyDictionary<string, string[]>? Errors, LogLevel LogLevel)
    BuildValidationResult(string title, string key, string message)
    {
        var errors = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase)
        {
            [key] = new[] { message }
        };
        return (
            StatusCodes.Status400BadRequest,
            ValidationType,
            title,
            message,
            errors,
            LogLevel.Warning);
    }

    private void LogException(LogLevel level, HttpContext context, Exception? exception, string traceId)
    {
        var payload = exception?.ToString() ?? "(no exception)";
        if (payload.Length > 2048)
        {
            payload = payload[..2048] + "…";
        }

        _logger.Log(level,
            "Handled exception {ExceptionType} for {Method} {Path} with trace {TraceId}. Details: {Details}",
            exception?.GetType().Name ?? "Unknown",
            context.Request.Method,
            context.Request.Path,
            traceId,
            payload);
    }
}
