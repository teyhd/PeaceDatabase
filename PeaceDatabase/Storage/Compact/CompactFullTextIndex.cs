using System;
using System.Collections.Generic;
using System.Linq;

namespace PeaceDatabase.Storage.Compact
{
    /// <summary>
    /// Компактный полнотекстовый индекс на основе Elias-Fano encoding.
    /// Заменяет Dictionary[token, HashSet[docId]] на Dictionary[token, EliasFanoList].
    /// 
    /// Для работы с Elias-Fano нужны целые числа, поэтому используется маппинг:
    /// - stringId -> numericId (для индексации)
    /// - numericId -> stringId (для результатов)
    /// </summary>
    public sealed class CompactFullTextIndex
    {
        // Маппинг string docId <-> numeric docId
        private readonly Dictionary<string, int> _docIdToNum = new(StringComparer.Ordinal);
        private readonly List<string> _numToDocId = new();

        // Индекс: token -> список числовых ID документов (несжатый, для накопления)
        private readonly Dictionary<string, List<int>> _pendingIndex = new(StringComparer.Ordinal);

        // Сжатый индекс: token -> EliasFanoList
        private readonly Dictionary<string, EliasFanoList> _compactIndex = new(StringComparer.Ordinal);

        // Флаг: нужна ли перестройка сжатого индекса
        private bool _isDirty;

        /// <summary>Количество уникальных токенов в индексе.</summary>
        public int TokenCount => _pendingIndex.Count + _compactIndex.Count;

        /// <summary>Количество проиндексированных документов.</summary>
        public int DocumentCount => _numToDocId.Count;

        /// <summary>
        /// Добавляет документ в индекс.
        /// </summary>
        /// <param name="docId">Строковый ID документа.</param>
        /// <param name="tokens">Токены документа.</param>
        public void Index(string docId, IEnumerable<string> tokens)
        {
            if (string.IsNullOrEmpty(docId)) return;

            // Получаем или создаём числовой ID
            if (!_docIdToNum.TryGetValue(docId, out int numId))
            {
                numId = _numToDocId.Count;
                _docIdToNum[docId] = numId;
                _numToDocId.Add(docId);
            }

            foreach (var token in tokens)
            {
                if (string.IsNullOrEmpty(token)) continue;

                if (!_pendingIndex.TryGetValue(token, out var list))
                {
                    // Если токен есть в сжатом индексе, распаковываем
                    if (_compactIndex.TryGetValue(token, out var efList))
                    {
                        list = new List<int>(efList);
                        _compactIndex.Remove(token);
                    }
                    else
                    {
                        list = new List<int>();
                    }
                    _pendingIndex[token] = list;
                }

                // Добавляем только если ещё нет (поддерживаем уникальность)
                if (list.Count == 0 || list[list.Count - 1] != numId)
                {
                    // Вставляем в отсортированном порядке (для Elias-Fano)
                    int insertIdx = list.BinarySearch(numId);
                    if (insertIdx < 0)
                    {
                        list.Insert(~insertIdx, numId);
                        _isDirty = true;
                    }
                }
            }
        }

        /// <summary>
        /// Удаляет документ из индекса.
        /// </summary>
        /// <param name="docId">Строковый ID документа.</param>
        /// <param name="tokens">Токены документа для удаления.</param>
        public void Unindex(string docId, IEnumerable<string> tokens)
        {
            if (!_docIdToNum.TryGetValue(docId, out int numId))
                return;

            foreach (var token in tokens)
            {
                if (string.IsNullOrEmpty(token)) continue;

                // Если токен в сжатом индексе, распаковываем
                if (_compactIndex.TryGetValue(token, out var efList))
                {
                    var list = new List<int>(efList);
                    _pendingIndex[token] = list;
                    _compactIndex.Remove(token);
                }

                if (_pendingIndex.TryGetValue(token, out var pendingList))
                {
                    int idx = pendingList.BinarySearch(numId);
                    if (idx >= 0)
                    {
                        pendingList.RemoveAt(idx);
                        _isDirty = true;

                        if (pendingList.Count == 0)
                            _pendingIndex.Remove(token);
                    }
                }
            }
        }

        /// <summary>
        /// Сжимает все pending posting lists в Elias-Fano формат.
        /// Вызывайте периодически или перед поиском для оптимальной производительности.
        /// </summary>
        public void Compact()
        {
            if (!_isDirty && _pendingIndex.Count == 0)
                return;

            foreach (var kv in _pendingIndex)
            {
                if (kv.Value.Count > 0)
                {
                    _compactIndex[kv.Key] = new EliasFanoList(kv.Value.ToArray());
                }
            }

            _pendingIndex.Clear();
            _isDirty = false;
        }

        /// <summary>
        /// Полнотекстовый поиск: возвращает ID документов, содержащих ВСЕ токены.
        /// </summary>
        /// <param name="queryTokens">Токены запроса.</param>
        /// <param name="skip">Пропустить первые N результатов.</param>
        /// <param name="limit">Максимум результатов.</param>
        /// <returns>Строковые ID документов.</returns>
        public IReadOnlyList<string> Search(IEnumerable<string> queryTokens, int skip = 0, int limit = 100)
        {
            var tokens = queryTokens.Where(t => !string.IsNullOrEmpty(t)).ToList();
            if (tokens.Count == 0)
                return Array.Empty<string>();

            // Собираем posting lists для каждого токена
            var postingLists = new List<IReadOnlyList<int>>();

            foreach (var token in tokens)
            {
                IReadOnlyList<int>? list = null;

                if (_compactIndex.TryGetValue(token, out var efList))
                {
                    list = efList.ToList();
                }
                else if (_pendingIndex.TryGetValue(token, out var pendingList))
                {
                    list = pendingList;
                }

                if (list == null || list.Count == 0)
                {
                    // Токен не найден — пересечение пустое
                    return Array.Empty<string>();
                }

                postingLists.Add(list);
            }

            // Пересечение всех posting lists
            var intersection = IntersectSortedLists(postingLists);

            // Применяем skip/limit и конвертируем в строковые ID
            return intersection
                .Skip(skip)
                .Take(limit)
                .Select(numId => numId < _numToDocId.Count ? _numToDocId[numId] : null)
                .Where(id => id != null)
                .ToList()!;
        }

        /// <summary>
        /// Поиск с использованием сжатого индекса (после Compact()).
        /// Более эффективен для памяти, использует galloping intersection.
        /// </summary>
        public IReadOnlyList<string> SearchCompact(IEnumerable<string> queryTokens, int skip = 0, int limit = 100)
        {
            // Сначала сжимаем pending
            Compact();

            var tokens = queryTokens.Where(t => !string.IsNullOrEmpty(t)).ToList();
            if (tokens.Count == 0)
                return Array.Empty<string>();

            // Собираем EliasFanoList для каждого токена
            var efLists = new List<EliasFanoList>();

            foreach (var token in tokens)
            {
                if (_compactIndex.TryGetValue(token, out var efList))
                {
                    efLists.Add(efList);
                }
                else
                {
                    // Токен не найден
                    return Array.Empty<string>();
                }
            }

            // Пересечение через Elias-Fano
            var intersection = EliasFanoList.IntersectMany(efLists);

            return intersection
                .Skip(skip)
                .Take(limit)
                .Select(numId => numId < _numToDocId.Count ? _numToDocId[numId] : null)
                .Where(id => id != null)
                .ToList()!;
        }

        /// <summary>
        /// Возвращает приблизительный размер индекса в байтах.
        /// </summary>
        public CompactIndexStats GetStats()
        {
            long compactBytes = 0;
            long pendingBytes = 0;
            int compactTokens = 0;
            int pendingTokens = 0;
            long compactPostings = 0;
            long pendingPostings = 0;

            foreach (var kv in _compactIndex)
            {
                compactBytes += kv.Value.GetSizeInBytes();
                compactBytes += kv.Key.Length * 2; // примерно для ключа
                compactTokens++;
                compactPostings += kv.Value.Count;
            }

            foreach (var kv in _pendingIndex)
            {
                pendingBytes += kv.Value.Count * sizeof(int);
                pendingBytes += kv.Key.Length * 2;
                pendingTokens++;
                pendingPostings += kv.Value.Count;
            }

            // Оценка размера обычного HashSet<string> индекса для сравнения
            // HashSet<string> ~= 40 bytes per entry + string overhead
            long hashSetEstimate = (compactPostings + pendingPostings) * 48;

            return new CompactIndexStats
            {
                CompactSizeBytes = compactBytes,
                PendingSizeBytes = pendingBytes,
                TotalSizeBytes = compactBytes + pendingBytes,
                CompactTokenCount = compactTokens,
                PendingTokenCount = pendingTokens,
                TotalPostings = compactPostings + pendingPostings,
                DocumentCount = _numToDocId.Count,
                EstimatedHashSetSizeBytes = hashSetEstimate,
                CompressionRatio = hashSetEstimate > 0 ? (double)hashSetEstimate / (compactBytes + pendingBytes + 1) : 0
            };
        }

        /// <summary>
        /// Пересечение нескольких отсортированных списков.
        /// </summary>
        private static List<int> IntersectSortedLists(List<IReadOnlyList<int>> lists)
        {
            if (lists.Count == 0)
                return new List<int>();

            if (lists.Count == 1)
                return lists[0].ToList();

            // Сортируем по размеру, начинаем с самого маленького
            lists.Sort((a, b) => a.Count.CompareTo(b.Count));

            var result = new List<int>(lists[0]);

            for (int i = 1; i < lists.Count && result.Count > 0; i++)
            {
                var other = lists[i];
                var newResult = new List<int>();
                int j = 0;

                foreach (int val in result)
                {
                    // Бинарный поиск в other
                    while (j < other.Count && other[j] < val)
                        j++;

                    if (j < other.Count && other[j] == val)
                        newResult.Add(val);
                }

                result = newResult;
            }

            return result;
        }
    }

    /// <summary>
    /// Статистика компактного индекса.
    /// </summary>
    public sealed class CompactIndexStats
    {
        public long CompactSizeBytes { get; init; }
        public long PendingSizeBytes { get; init; }
        public long TotalSizeBytes { get; init; }
        public int CompactTokenCount { get; init; }
        public int PendingTokenCount { get; init; }
        public long TotalPostings { get; init; }
        public int DocumentCount { get; init; }
        public long EstimatedHashSetSizeBytes { get; init; }
        public double CompressionRatio { get; init; }

        public override string ToString()
        {
            return $"Compact: {CompactSizeBytes / 1024.0:F1} KB ({CompactTokenCount} tokens), " +
                   $"Pending: {PendingSizeBytes / 1024.0:F1} KB ({PendingTokenCount} tokens), " +
                   $"Postings: {TotalPostings}, Docs: {DocumentCount}, " +
                   $"vs HashSet: {EstimatedHashSetSizeBytes / 1024.0:F1} KB, " +
                   $"Compression: {CompressionRatio:F1}x";
        }
    }
}

