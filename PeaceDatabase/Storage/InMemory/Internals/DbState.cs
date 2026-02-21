using System.Collections.Generic;
using System.Threading;
using PeaceDatabase.Storage.Compact;

namespace PeaceDatabase.Storage.InMemory.Internals
{
    /// <summary>
    /// Состояние одной базы в InMemory-движке: ревизии, головы, индексы, последовательность, блокировка.
    /// </summary>
    internal sealed class DbState
    {
        public readonly SortedDictionary<string, Head> Heads = new(System.StringComparer.Ordinal); // id -> head
        public readonly Dictionary<string, SortedDictionary<string, string>> Revs = new();          // id -> (rev -> json)
        public int Seq;
        public readonly ReaderWriterLockSlim Lock = new(LockRecursionPolicy.NoRecursion);

        // ---- Индексы ----
        // Равенство по полям: field -> value -> set(ids)
        public readonly Dictionary<string, Dictionary<string, HashSet<string>>> EqIndex =
            new(System.StringComparer.Ordinal);

        // Числовые диапазоны: field -> SortedDictionary(value -> set(ids))
        public readonly Dictionary<string, SortedDictionary<double, HashSet<string>>> NumIndex =
            new(System.StringComparer.Ordinal);

        // Теги: tag -> set(ids)
        public readonly Dictionary<string, HashSet<string>> TagIndex =
            new(System.StringComparer.OrdinalIgnoreCase);

        // Полнотекст: token -> set(ids) (обычный индекс)
        public readonly Dictionary<string, HashSet<string>> FullText =
            new(System.StringComparer.Ordinal);

        // ---- Компактный полнотекстовый индекс (Elias-Fano) ----
        /// <summary>
        /// Компактный полнотекстовый индекс на основе Elias-Fano encoding.
        /// Используется параллельно с обычным индексом для сравнения.
        /// </summary>
        public readonly CompactFullTextIndex CompactFullText = new();

        /// <summary>
        /// Флаг: использовать компактный индекс для поиска.
        /// По умолчанию false — используется обычный HashSet-индекс.
        /// </summary>
        public bool UseCompactIndex { get; set; }
    }
}
