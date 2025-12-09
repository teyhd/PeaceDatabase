namespace PeaceDatabase.Storage.Sharding.Routing;

/// <summary>
/// Интерфейс маршрутизатора запросов к шардам.
/// Определяет, на какой шард направить запрос на основе ключа.
/// </summary>
public interface IShardRouter
{
    /// <summary>
    /// Количество шардов в кластере.
    /// </summary>
    int ShardCount { get; }

    /// <summary>
    /// Определяет индекс шарда для заданного ключа.
    /// </summary>
    /// <param name="key">Ключ шардирования (обычно _id документа).</param>
    /// <returns>Индекс шарда от 0 до ShardCount-1.</returns>
    int GetShardId(string key);

    /// <summary>
    /// Определяет индексы шардов для списка ключей.
    /// Полезно для batch-операций.
    /// </summary>
    /// <param name="keys">Список ключей.</param>
    /// <returns>Словарь: shardId -> список ключей для этого шарда.</returns>
    Dictionary<int, List<string>> GroupByShards(IEnumerable<string> keys);

    /// <summary>
    /// Возвращает все индексы шардов (для scatter-gather операций).
    /// </summary>
    IEnumerable<int> GetAllShardIds();
}

