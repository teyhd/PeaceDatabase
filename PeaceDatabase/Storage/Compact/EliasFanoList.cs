using System;
using System.Collections;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace PeaceDatabase.Storage.Compact
{
    /// <summary>
    /// Elias-Fano encoding для компактного хранения отсортированной последовательности целых чисел.
    /// 
    /// Теория:
    /// Для n чисел из диапазона [0, U] занимает n * (2 + ceil(log2(U/n))) бит.
    /// При равномерном распределении: ~2-3 бита на элемент.
    /// 
    /// Структура:
    /// - lowBits: младшие L бит каждого числа (L = floor(log2(U/n)))
    /// - highBits: унарное кодирование старших бит (разделители между группами)
    /// </summary>
    public sealed class EliasFanoList : IEnumerable<int>
    {
        private readonly CompactBitVector _highBits;
        private readonly ulong[] _lowBits;
        private readonly int _count;
        private readonly int _lowBitWidth;
        private readonly int _maxValue;

        /// <summary>
        /// Создаёт Elias-Fano список из отсортированного массива.
        /// </summary>
        /// <param name="sortedValues">Отсортированные по возрастанию неотрицательные числа.</param>
        public EliasFanoList(int[] sortedValues)
        {
            if (sortedValues == null || sortedValues.Length == 0)
            {
                _count = 0;
                _lowBitWidth = 0;
                _maxValue = 0;
                _highBits = new CompactBitVector(Array.Empty<ulong>(), 0);
                _lowBits = Array.Empty<ulong>();
                return;
            }

            _count = sortedValues.Length;
            _maxValue = sortedValues[_count - 1];

            // Вычисляем ширину младших бит: L = floor(log2(U/n))
            // Если U < n, то L = 0
            if (_maxValue < _count)
            {
                _lowBitWidth = 0;
            }
            else
            {
                _lowBitWidth = BitOperations.Log2((uint)(_maxValue / _count));
            }

            // Кодируем младшие биты
            if (_lowBitWidth > 0)
            {
                int totalLowBits = _count * _lowBitWidth;
                int numLowBlocks = (totalLowBits + 63) / 64;
                _lowBits = new ulong[numLowBlocks];

                ulong lowMask = (1UL << _lowBitWidth) - 1;
                for (int i = 0; i < _count; i++)
                {
                    ulong lowVal = (ulong)sortedValues[i] & lowMask;
                    WriteBits(_lowBits, i * _lowBitWidth, _lowBitWidth, lowVal);
                }
            }
            else
            {
                _lowBits = Array.Empty<ulong>();
            }

            // Кодируем старшие биты в унарной форме
            // Каждое число вносит: (high[i] - high[i-1]) нулей + 1 единицу
            // Общая длина: n + (maxHigh + 1), где maxHigh = maxValue >> lowBitWidth
            int maxHigh = _maxValue >> _lowBitWidth;
            int highBitsLength = _count + maxHigh + 1;

            var highBuilder = new BitVectorBuilder(highBitsLength);
            int prevHigh = 0;

            for (int i = 0; i < _count; i++)
            {
                int high = sortedValues[i] >> _lowBitWidth;
                int gap = high - prevHigh;

                // gap нулей (переход к следующей группе)
                highBuilder.AppendZeros(gap);
                // одна единица (элемент)
                highBuilder.AppendOne();

                prevHigh = high;
            }

            _highBits = highBuilder.Build();
        }

        /// <summary>Количество элементов в списке.</summary>
        public int Count => _count;

        /// <summary>Максимальное значение в списке.</summary>
        public int MaxValue => _maxValue;

        /// <summary>Ширина младших бит (L).</summary>
        public int LowBitWidth => _lowBitWidth;

        /// <summary>
        /// Получить элемент по индексу. O(1).
        /// </summary>
        public int this[int index]
        {
            get
            {
                if ((uint)index >= (uint)_count)
                    throw new ArgumentOutOfRangeException(nameof(index));

                // Позиция i-й единицы в highBits
                int highPos = _highBits.Select(index);
                // Старшие биты = позиция - индекс (количество нулей до этой единицы)
                int high = highPos - index;

                // Младшие биты
                int low = 0;
                if (_lowBitWidth > 0)
                {
                    low = (int)ReadBits(_lowBits, index * _lowBitWidth, _lowBitWidth);
                }

                return (high << _lowBitWidth) | low;
            }
        }

        /// <summary>
        /// Проверяет, содержится ли значение в списке. O(log n).
        /// </summary>
        public bool Contains(int value)
        {
            if (value < 0 || value > _maxValue || _count == 0)
                return false;

            // Бинарный поиск
            int lo = 0, hi = _count - 1;
            while (lo <= hi)
            {
                int mid = lo + (hi - lo) / 2;
                int midVal = this[mid];

                if (midVal == value)
                    return true;
                if (midVal < value)
                    lo = mid + 1;
                else
                    hi = mid - 1;
            }
            return false;
        }

        /// <summary>
        /// Находит первый элемент >= value (NextGEQ).
        /// Возвращает индекс или -1, если такого нет.
        /// O(log n).
        /// </summary>
        public int NextGEQ(int value)
        {
            if (_count == 0 || value > _maxValue)
                return -1;
            if (value <= 0)
                return 0;

            // Бинарный поиск первого элемента >= value
            int lo = 0, hi = _count - 1;
            int result = -1;

            while (lo <= hi)
            {
                int mid = lo + (hi - lo) / 2;
                int midVal = this[mid];

                if (midVal >= value)
                {
                    result = mid;
                    hi = mid - 1;
                }
                else
                {
                    lo = mid + 1;
                }
            }

            return result;
        }

        /// <summary>
        /// Находит значение первого элемента >= value.
        /// Возвращает -1, если такого нет.
        /// </summary>
        public int NextGEQValue(int value)
        {
            int idx = NextGEQ(value);
            return idx >= 0 ? this[idx] : -1;
        }

        /// <summary>
        /// Размер структуры в байтах (приблизительно).
        /// </summary>
        public long GetSizeInBytes()
        {
            return _highBits.GetSizeInBytes() 
                 + _lowBits.Length * sizeof(ulong)
                 + sizeof(int) * 3; // count, lowBitWidth, maxValue
        }

        /// <summary>
        /// Количество бит на элемент (среднее).
        /// </summary>
        public double BitsPerElement
        {
            get
            {
                if (_count == 0) return 0;
                long totalBits = _highBits.Length + _lowBits.Length * 64;
                return (double)totalBits / _count;
            }
        }

        /// <summary>
        /// Перечисление всех элементов.
        /// </summary>
        public IEnumerator<int> GetEnumerator()
        {
            for (int i = 0; i < _count; i++)
                yield return this[i];
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        #region Bit manipulation helpers

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void WriteBits(ulong[] array, int bitOffset, int width, ulong value)
        {
            if (width == 0) return;

            int blockIdx = bitOffset / 64;
            int bitIdx = bitOffset % 64;

            if (bitIdx + width <= 64)
            {
                // Все биты в одном блоке
                ulong mask = ((1UL << width) - 1) << bitIdx;
                array[blockIdx] = (array[blockIdx] & ~mask) | (value << bitIdx);
            }
            else
            {
                // Биты разделены между двумя блоками
                int bitsInFirst = 64 - bitIdx;
                int bitsInSecond = width - bitsInFirst;

                ulong mask1 = ((1UL << bitsInFirst) - 1) << bitIdx;
                array[blockIdx] = (array[blockIdx] & ~mask1) | (value << bitIdx);

                ulong mask2 = (1UL << bitsInSecond) - 1;
                array[blockIdx + 1] = (array[blockIdx + 1] & ~mask2) | (value >> bitsInFirst);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong ReadBits(ulong[] array, int bitOffset, int width)
        {
            if (width == 0) return 0;

            int blockIdx = bitOffset / 64;
            int bitIdx = bitOffset % 64;

            if (bitIdx + width <= 64)
            {
                // Все биты в одном блоке
                return (array[blockIdx] >> bitIdx) & ((1UL << width) - 1);
            }
            else
            {
                // Биты разделены между двумя блоками
                int bitsInFirst = 64 - bitIdx;
                ulong low = array[blockIdx] >> bitIdx;
                ulong high = array[blockIdx + 1] & ((1UL << (width - bitsInFirst)) - 1);
                return low | (high << bitsInFirst);
            }
        }

        #endregion

        #region Static intersection methods

        /// <summary>
        /// Пересечение двух Elias-Fano списков с использованием galloping.
        /// Возвращает новый список с общими элементами.
        /// </summary>
        public static List<int> Intersect(EliasFanoList a, EliasFanoList b)
        {
            var result = new List<int>();
            if (a.Count == 0 || b.Count == 0)
                return result;

            // Используем galloping intersection: итерируем по меньшему списку,
            // ищем в большем через NextGEQ
            if (a.Count > b.Count)
                (a, b) = (b, a);

            int bIdx = 0;
            foreach (int val in a)
            {
                // Ищем val или больший в b
                bIdx = b.NextGEQ(val);
                if (bIdx < 0)
                    break;

                int bVal = b[bIdx];
                if (bVal == val)
                    result.Add(val);
            }

            return result;
        }

        /// <summary>
        /// Пересечение нескольких Elias-Fano списков.
        /// </summary>
        public static List<int> IntersectMany(IReadOnlyList<EliasFanoList> lists)
        {
            if (lists == null || lists.Count == 0)
                return new List<int>();

            if (lists.Count == 1)
                return new List<int>(lists[0]);

            // Сортируем по размеру (начинаем с самого маленького)
            var sorted = new List<EliasFanoList>(lists);
            sorted.Sort((x, y) => x.Count.CompareTo(y.Count));

            var result = new List<int>(sorted[0]);
            
            for (int i = 1; i < sorted.Count && result.Count > 0; i++)
            {
                var next = sorted[i];
                var newResult = new List<int>();

                foreach (int val in result)
                {
                    if (next.Contains(val))
                        newResult.Add(val);
                }

                result = newResult;
            }

            return result;
        }

        #endregion
    }
}

