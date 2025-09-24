using System.Collections.Generic;

namespace PeaceDatabase.Core.Models
{
    /// <summary>
    /// Документ (аналог CouchDB): ревизии, мягкое удаление, произвольные данные и индексация.
    /// </summary>
    public class Document
    {
        public string Id { get; set; } = string.Empty; // "_id"
        public string? Rev { get; set; }               // "_rev"
        public bool Deleted { get; set; }              // "_deleted"
        // Поля для равенств/диапазонов:
        public Dictionary<string, object>? Data { get; set; }
        // Мета-теги:
        public List<string>? Tags { get; set; }

        // Полнотекстовый контент:
        public string? Content { get; set; }
    }
}
