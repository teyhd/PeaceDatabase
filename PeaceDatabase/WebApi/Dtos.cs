// Dtos.cs
// DTO для web-слоя: отделяем контракт API от внутренних моделей, добавляем удобные формы запросов.

using System.Collections.Generic;
using System.Text.Json;
using PeaceDatabase.Core.Models;

namespace PeaceDatabase.WebApi
{
    // --- Страница документов ---
    public sealed class DocsPage
    {
        public int Offset { get; set; }
        public int Limit { get; set; }
        public List<DocumentDto> Items { get; set; } = new();
    }

    // --- Документ для API ---
    public sealed class DocumentDto
    {
        public string Id { get; set; } = string.Empty;     // "_id"
        public string? Rev { get; set; }                   // "_rev"
        public bool Deleted { get; set; }                  // "_deleted"

        public Dictionary<string, object>? Data { get; set; }
        public List<string>? Tags { get; set; }
        public string? Content { get; set; }

        public static DocumentDto From(Document d) => new()
        {
            Id = d.Id,
            Rev = d.Rev,
            Deleted = d.Deleted,
            Data = d.Data,
            Tags = d.Tags,
            Content = d.Content
        };

        public Document ToCore() => new()
        {
            Id = Id,
            Rev = Rev,
            Deleted = Deleted,
            Data = Data,
            Tags = Tags,
            Content = Content
        };
    }

    // --- Поиск по полям ---
    public sealed class FindByFieldsRequest
    {
        // Равенства: field -> value
        public Dictionary<string, string>? EqualFields { get; set; }

        // Один числовой диапазон
        public string? NumericRangeField { get; set; }
        public double? Min { get; set; }
        public double? Max { get; set; }

        // Пагинация
        public int Skip { get; set; } = 0;
        public int Limit { get; set; } = 100;
    }

    // --- Поиск по тегам ---
    public sealed class FindByTagsRequest
    {
        public List<string>? AllOf { get; set; }
        public List<string>? AnyOf { get; set; }
        public List<string>? NoneOf { get; set; }

        public int Skip { get; set; } = 0;
        public int Limit { get; set; } = 100;
    }
}
