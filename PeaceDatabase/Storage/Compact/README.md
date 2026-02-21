# Практическая работа № 5: Компактные структуры данных

## Elias-Fano Encoding

### Описание

Реализована компактная структура данных **Elias-Fano** для хранения posting lists полнотекстового индекса. Структура заменяет `HashSet<string>` на компактное битовое представление отсортированных целых чисел.

### Теория

Для последовательности из `n` чисел в диапазоне `[0, U]`:
- Занимает `n × (2 + ⌈log₂(U/n)⌉)` бит
- При равномерном распределении теоретически может занимать ~2-3 бита на элемент
- Операции `Contains`, `NextGEQ` за O(log n)

#### Структура кодирования:
1. **lowBits**: младшие L бит каждого числа (L = ⌊log₂(U/n)⌋)
2. **highBits**: унарное кодирование старших бит с операциями Rank/Select

### Реализованные компоненты

| Файл | Описание |
|------|----------|
| `CompactBitVector.cs` | Битовый вектор с операциями Rank(i) и Select(k) |
| `EliasFanoList.cs` | Elias-Fano encoding для монотонных последовательностей |
| `CompactFullTextIndex.cs` | Полнотекстовый индекс на основе Elias-Fano |

### Интеграция

Компактный индекс интегрирован в `InMemoryDocumentService`:

```csharp
// Включить компактный режим
service.SetUseCompactIndex("mydb", true);

// Поиск работает автоматически через компактный индекс
var results = service.FullTextSearch("mydb", "query");

// Получить статистику компрессии
var stats = service.GetCompactIndexStats("mydb");
```

### Результаты бенчмарков

#### Сравнение памяти

| Документов | Токенов/док | HashSet (KB) | Elias-Fano (KB) | Компрессия |
|------------|-------------|--------------|-----------------|------------|
| 100 | 10 | 122.7 | 35.5 | **3.5x** |
| 1,000 | 50 | 4,631.6 | 88.7 | **52x** |
| 10,000 | 100 | 93,585.7 | 824.3 | **113x** |

![Memory Comparison](../../../docs/benchmarks/elias_fano_memory.png)

#### Коэффициент компрессии и эффективность

![Compression Ratio](../../../docs/benchmarks/elias_fano_compression.png)

#### Сравнение скорости поиска

![Speed Comparison](../../../docs/benchmarks/elias_fano_speed.png)

#### 10,000 документов, 500,000 postings

```
Compact: 837.0 KB (5000 tokens)
vs HashSet: 23,437.5 KB
Compression: 28.0x
Bits per posting: 13.71
```

### Запуск

```powershell
cd PeaceDatabase
dotnet test PeaceDatabase.Tests --filter "EliasFano" --logger "console;verbosity=normal"

dotnet test PeaceDatabase.Tests --filter "EliasFano" --logger "console;verbosity=detailed"

dotnet test PeaceDatabase.Tests --filter "Benchmark_MemoryComparison" --no-build --logger "console;verbosity=detailed" 2>&1 | Select-String -Pattern "===" -Context 0,5

dotnet test PeaceDatabase.Tests --filter "Benchmark_DetailedMemoryAnalysis" --no-build --logger "console;verbosity=detailed" 2>&1 | Select-String -Pattern "===" -Context 0,4

dotnet test PeaceDatabase.Tests --filter "Benchmark_IntegratedWithDocumentService" --no-build --logger "console;verbosity=detailed" 2>&1 | Select-String -Pattern "(Inserted|Compact|===|Normal|results)" -Context 0,1
```

#### Графики

```powershell
# запуск бенча
python tools/elias_fano_bench.py --out docs/benchmarks

# сохранённый вывод тестов
python tools/elias_fano_bench.py --skip-tests --test-output docs/benchmarks/test_output.txt

# кастомные настройки
python tools/elias_fano_bench.py --url http://localhost:5000 --scales 100,1000,5000 --out docs/bench_api 2>&1 | Select-Object -Last 20
```

`docs/benchmarks/`:
- `elias_fano_memory.png` — сравнение памяти
- `elias_fano_compression.png` — коэффициент компрессии
- `elias_fano_speed.png` — сравнение скорости


### Сравнение

| Характеристика | HashSet<string> | Elias-Fano           |
| -------------- | --------------- | -------------------- |
| Память на ID   | ~80 байт        | **~2 бита**          |
| Contains       | O(1)            | O(log n)             |
| Пересечение    | O(min(n,m))     | O(n log m) galloping |
| Вставка        | O(1)            | Требует перестройки  |
| Удаление       | O(1)            | Требует перестройки  |

### Выводы

**Elias-Fano подходит для:**
- Статических или редко обновляемых индексов
- Больших posting lists (тысячи элементов)
- Систем с ограниченной памятью

**HashSet лучше для:**
- Частых вставок/удалений
- Маленьких списков (< 100 элементов)
- Когда память не критична
