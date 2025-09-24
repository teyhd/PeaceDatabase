// Fiusing System;

namespace PeaceDatabase.Storage.Disk.Internals
{
    /// <summary>
    /// Конфигурация дискового хранилища.
    /// </summary>
    public enum DurabilityLevel
    {
        Relaxed = 0,   // Быстро: без fsync, только Flush в ОС
        Commit = 1,   // Баланс: периодический fsync по порогу
        Strong = 2    // Надёжно: fsync на каждую запись
    }

    public sealed class StorageOptions
    {
        // --- Константы по умолчанию (разумные) ---
        public const string DefaultDataRoot = "data";
        public const int DefaultSnapshotEveryNOps = 5_000;
        public const int DefaultSnapshotMaxWalSizeMb = 64;
        public const DurabilityLevel DefaultDurability = DurabilityLevel.Commit;
        public const int DefaultNHsards = 1; // Пока 1, чтобы не раздувать структуру
        public const bool DefaultIndexRebuildOnStart = true;

        // --- Параметры ---
        public string DataRoot { get; init; } = DefaultDataRoot;
        public int SnapshotEveryNOperations { get; init; } = DefaultSnapshotEveryNOps;
        public int SnapshotMaxWalSizeMb { get; init; } = DefaultSnapshotMaxWalSizeMb;
        public DurabilityLevel Durability { get; init; } = DefaultDurability;
        public int NShards { get; init; } = DefaultNHsards;
        public bool IndexRebuildOnStart { get; init; } = DefaultIndexRebuildOnStart;

        // Вкл./выкл. ведения снапшотов (для тестов можно отключать)
        public bool EnableSnapshots { get; init; } = true;
        // Префиксы имён файлов
        public string WalFileName { get; init; } = "wal.log";
        public string ManifestFileName { get; init; } = "manifest.json";
        public string SnapshotPrefix { get; init; } = "snapshot-";
        public string SnapshotExt { get; init; } = ".jsonl";
    }
}
