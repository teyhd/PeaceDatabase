# GZIP Compression

Собственная реализация алгоритма GZIP.

## Обзор

Сжатие DEFLATE/GZIP по RFC 1951, 1952:

- **LZ77** - алгоритм поиска повторяющихся последовательностей со скользящим окном 32KB
- **Huffman кодирование** - статические таблицы кодов по стандарту DEFLATE
- **CRC32** - контрольная сумма для проверки целостности данных
- **GZIP формат** - стандартный формат файлов, совместимый с gunzip/7z

## Запуск

### Проект

```bash
# Сборка
dotnet build PeaceDatabase\PeaceDatabase.sln

# Тесты GZIP
dotnet test --filter "FullyQualifiedName~Gzip"
```

### API

```csharp
using PeaceDatabase.Storage.Compression.Gzip;

// Сжатие
byte[] data = Encoding.UTF8.GetBytes("Hello, World!");
byte[] compressed = GzipCodec.Compress(data);

// Сжатие с указанием имени файла
byte[] compressedWithName = GzipCodec.Compress(data, "hello.txt");

// Распаковка
byte[] decompressed = GzipCodec.Decompress(compressed);

// Проверка формата
bool isGzip = GzipCodec.IsGzipData(compressed);
```

### CLI

```bash
# Сжатие
dotnet run --project PeaceDatabase.Gzip -- compress input.txt output.gz

# Распаковка
dotnet run --project PeaceDatabase.Gzip -- decompress output.gz restored.txt

# Бенчмарк (сравнение с System.IO.Compression)
dotnet run --project PeaceDatabase.Gzip -- benchmark 100

# Тест совместимости
dotnet run --project PeaceDatabase.Gzip -- test
```

### Docker

```bash
docker build -t peacedb-gzip -f PeaceDatabase.Gzip/Dockerfile .

# Сжатие
docker run -v $(pwd):/data peacedb-gzip compress /data/input.txt /data/output.gz

# Бенчмарк
docker-compose run gzip benchmark 100
```

### Совместимость

Файлы совместимы с gunzip (gzip -d), 7z, System.IO.Compression.GZipStream

```bash
# Проверка целостности
gunzip -t output.gz

# Распаковка gunzip
gunzip -c output.gz > restored.txt

# Распаковка 7z
7z x output.gz
```
## Проверка

```powershell
PS D:\_ITMOStudy\_Master\OPVP\PeaceDatabase> dotnet run --project PeaceDatabase.Gzip -- test 2>&1
GZIP Compatibility Test
=======================

Test data: Generated JSON document
Size: 410 bytes

Compressed size: 357 bytes
Compression ratio: 87.1%

Compressed file: C:\Users\admin\AppData\Local\Temp\peacedb-test-22440d63a40c4663a9ec5317d3b74424.gz

Verification with System.IO.Compression.GZipStream:
  Status: SUCCESS - Data matches!
  Status: SUCCESS - Data matches!
  Decompressed size: 410 bytes

Verification with custom decompressor:
  Status: SUCCESS - Data matches!

To test with external tools:
  gunzip -c "C:\Users\admin\AppData\Local\Temp\peacedb-test-22440d63a40c4663a9ec5317d3b74424.gz" > "C:\Users\admin\AppData\Local\Temp\peacedb-test-22440d63a40c4663a9ec5317d3b74424"
  7z x "C:\Users\admin\AppData\Local\Temp\peacedb-test-22440d63a40c4663a9ec5317d3b74424.gz" -o"C:\Users\admin\AppData\Local\Temp"

Press Enter to cleanup temp files, or Ctrl+C to keep them...
```

## Сравнение с System.IO.Compression.GZipStream на 100KB данных.

Меньше ratio - лучше сжатие, Custom/Sys - соотношение времени работы.

```powershell
PS D:\_ITMOStudy\_Master\OPVP\PeaceDatabase> docker-compose run gzip benchmark 100
[+] Creating 1/1
 ✔ Network peacedatabase_benchnet  Created                                                                 0.0s 
GZIP Compression Benchmark (Custom vs System.IO.Compression)
============================================================

Data Type            Size         Custom          Ratio      System          Ratio      Custom/Sys  
----------------------------------------------------------------------------------------------
JSON-like                 100 KB         5 ms       7.5%           1 ms       5.7%        5.00x
Repeated text             100 KB         1 ms       0.9%           0 ms       0.4%        1.00x
Random binary             100 KB         6 ms     105.4%           2 ms     100.1%        3.00x

Notes:
- Custom implementation uses static Huffman (simpler, slightly less efficient)
- System.IO.Compression uses dynamic Huffman (more complex, better ratio)

Roundtrip verification...
  JSON-like: OK
  Repeated text: OK
  Random binary: OK
```

## Как сделано

Наш использует статические Huffman таблицы (проще, чуть менее эффективно)

Системный использует динамические Huffman таблицы (сложнее, лучшее сжатие)

**Сжатие** - `PeaceDatabase/Storage/Compression/`:
- `Gzip/Crc32.cs` - CRC32 с полиномом 0xEDB88320
- `Gzip/GzipCodec.cs` - главный API: Compress/Decompress по RFC 1952
- `Deflate/BitStream.cs` - побитовое чтение/запись LSB-first
- `Deflate/HuffmanTable.cs` - статические Huffman таблицы по RFC 1951
- `Deflate/Lz77Encoder.cs` - LZ77 со скользящим окном 32KB
- `Deflate/DeflateEncoder.cs` - DEFLATE сжатие (блоки типа 1)
- `Deflate/DeflateDecoder.cs` - DEFLATE распаковка

**CLI** - `PeaceDatabase.Gzip/`:
- `Program.cs` - инструмент для сжатия/распаковки/бенчмарка
- `Dockerfile` - Docker образ для CLI

## Тесты - PeaceDatabase.Tests/GzipCodecTests.cs

```bash
dotnet test PeaceDatabase.Tests --filter "FullyQualifiedName~Gzip|FullyQualifiedName~Crc32|FullyQualifiedName~Lz77|FullyQualifiedName~Deflate|FullyQualifiedName~Huffman" --verbosity minimal 2>&1

dotnet test PeaceDatabase.Tests
```
CRC32 вычисления

LZ77 кодирование

DEFLATE encoder/decoder roundtrip

GZIP сжатие/распаковка

Совместимость с System.IO.Compression

Обработка ошибок

## Заметки

Только static Huffman для сжатия - dynamic Huffman поддерживается только при распаковке

Greedy LZ77 - без lazy matching оптимизации

Не потоковый - работает с полными буферами в памяти, возможны проблемы

[RFC 1951 - DEFLATE Compressed Data Format](https://tools.ietf.org/html/rfc1951)

[RFC 1952 - GZIP File Format](https://tools.ietf.org/html/rfc1952)

[An Explanation of the Deflate Algorithm](https://zlib.net/feldspar.html)

### LZ77 алгоритм

- Размер окна: 32KB
- Минимальная длина совпадения: 3 байта
- Максимальная длина совпадения: 258 байт
- Хеш-цепочки для быстрого поиска совпадений

### DEFLATE блоки

- Тип 0 (Stored): без сжатия, для несжимаемых данных
- Тип 1 (Static Huffman): предопределённые таблицы по RFC 1951
- Тип 2 (Dynamic Huffman): поддерживается при декомпрессии

### Huffman кодирование

Статические коды по RFC 1951 Section 3.2.6:
- Литералы 0-143: 8 бит (коды 0x30-0xBF)
- Литералы 144-255: 9 бит (коды 0x190-0x1FF)
- Символы 256-279: 7 бит (коды 0x00-0x17)
- Символы 280-287: 8 бит (коды 0xC0-0xC7)
- Дистанции 0-29: 5 бит

### Формат GZIP (взято с RFC 1952)

```
+---+---+---+---+---+---+---+---+---+---+
|ID1|ID2|CM |FLG|     MTIME     |XFL|OS | Header (10 bytes)
+---+---+---+---+---+---+---+---+---+---+
|   ...compressed data...              | DEFLATE data
+---+---+---+---+---+---+---+---+
|     CRC32     |     ISIZE     |       Footer (8 bytes)
+---+---+---+---+---+---+---+---+

ID1, ID2 = 0x1F, 0x8B (magic number)
CM = 8 (deflate compression)
FLG = flags (FNAME, etc.)
MTIME = modification time (Unix timestamp)
XFL = extra flags
OS = operating system
CRC32 = checksum of uncompressed data
ISIZE = size of uncompressed data mod 2^32
```
