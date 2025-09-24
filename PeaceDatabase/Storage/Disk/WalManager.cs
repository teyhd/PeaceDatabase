// File: Storage/Disk/WalManager.cs
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using PeaceDatabase.Storage.Disk.Internals;

namespace PeaceDatabase.Storage.Disk
{
    /// <summary>
    /// Простой append-only WAL (по строке JSON на операцию).
    /// Формат записи: { "op":"put|del", "id":"...", "rev":"...", "seq":123, "doc":{...}, "ts":"..." }
    /// </summary>
    internal sealed class WalManager : IDisposable
    {
        private readonly string _path;
        private readonly StorageOptions _opt;
        private FileStream? _stream;
        private readonly object _lock = new();
        private long _bytesWrittenSinceLastSync = 0;

        public WalManager(string dbDir, StorageOptions opt)
        {
            Directory.CreateDirectory(dbDir);
            _path = Path.Combine(dbDir, opt.WalFileName);
            _opt = opt;
            _stream = new FileStream(_path, FileMode.Append, FileAccess.Write, FileShare.Read,
                                     bufferSize: 64 * 1024, FileOptions.WriteThrough);
        }

        public string WalPath => _path;

        public void Append(object record)
        {
            // Сериализуем и записываем в один проход (строка + '\n')
            var json = JsonSerializer.Serialize(record);
            var bytes = Encoding.UTF8.GetBytes(json);
            var nl = new byte[] { (byte)'\n' };

            lock (_lock)
            {
                EnsureOpen();
                _stream!.Write(bytes, 0, bytes.Length);
                _stream!.Write(nl, 0, 1);

                // Политика durability
                switch (_opt.Durability)
                {
                    case DurabilityLevel.Relaxed:
                        _stream.Flush(); // без fsync
                        break;
                    case DurabilityLevel.Commit:
                        _stream.Flush(); // периодический fsync по порогу
                        _bytesWrittenSinceLastSync += bytes.Length + 1;
                        if (_bytesWrittenSinceLastSync > (1024 * 1024))
                        {
                            _stream.Flush(true);
                            _bytesWrittenSinceLastSync = 0;
                        }
                        break;
                    case DurabilityLevel.Strong:
                        _stream.Flush(true); // fsync каждую запись
                        break;
                }
            }
        }

        public IEnumerable<string> ReadAllLines()
        {
            if (!File.Exists(_path))
                yield break;

            using var fs = new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var sr = new StreamReader(fs, Encoding.UTF8);
            string? line;
            while ((line = sr.ReadLine()) != null)
                if (!string.IsNullOrWhiteSpace(line))
                    yield return line;
        }

        public void Rotate() // обнулить WAL после снапшота
        {
            lock (_lock)
            {
                CloseInternal();
                File.Delete(_path);
                _stream = new FileStream(_path, FileMode.Append, FileAccess.Write, FileShare.Read,
                                         bufferSize: 64 * 1024, FileOptions.WriteThrough);
                _bytesWrittenSinceLastSync = 0;
            }
        }

        public long GetSizeBytes()
        {
            try
            {
                var fi = new FileInfo(_path);
                return fi.Exists ? fi.Length : 0L;
            }
            catch { return 0L; }
        }

        private void EnsureOpen()
        {
            if (_stream is null)
            {
                _stream = new FileStream(_path, FileMode.Append, FileAccess.Write, FileShare.Read,
                                         bufferSize: 64 * 1024, FileOptions.WriteThrough);
            }
        }

        private void CloseInternal()
        {
            try { _stream?.Flush(true); } catch { /* ignore */ }
            try { _stream?.Dispose(); } catch { /* ignore */ }
            _stream = null;
        }

        public void Dispose()
        {
            lock (_lock) CloseInternal();
        }
    }
}
