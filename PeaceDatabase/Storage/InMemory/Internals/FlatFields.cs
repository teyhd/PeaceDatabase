using System.Collections.Generic;

namespace PeaceDatabase.Storage.InMemory.Internals
{
    /// <summary>
    /// Вспомогательная структура: расплющенные поля для индексации.
    /// </summary>
    internal sealed class FlatFields
    {
        public Dictionary<string, string> Eq { get; } = new(System.StringComparer.Ordinal);
        public Dictionary<string, double> Num { get; } = new(System.StringComparer.Ordinal);
        public List<string> StringsForFulltext { get; } = new();
    }
}
