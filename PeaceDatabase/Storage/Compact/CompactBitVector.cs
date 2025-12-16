using System;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace PeaceDatabase.Storage.Compact
{
    /// <summary>
    /// Компактный битовый вектор с операциями Rank и Select.
    /// Rank(i) — количество единиц до позиции i (не включая).
    /// Select(k) — позиция k-й единицы (0-based).
    /// </summary>
    public sealed class CompactBitVector
    {
        private readonly ulong[] _bits;
        private readonly int _length;
        private readonly int _popCount;

        // Для ускорения Rank: предподсчёт суммы popcount по блокам
        // _rankIndex[i] = количество единиц в блоках [0..i-1]
        private readonly int[] _rankIndex;
        private const int BlockSize = 64; // бит в ulong

        /// <summary>
        /// Создаёт битовый вектор из массива битов.
        /// </summary>
        /// <param name="bits">Массив ulong, где каждый элемент содержит 64 бита.</param>
        /// <param name="length">Общее количество бит в векторе.</param>
        public CompactBitVector(ulong[] bits, int length)
        {
            _bits = bits ?? throw new ArgumentNullException(nameof(bits));
            _length = length;

            // Подсчёт общего количества единиц и построение индекса для Rank
            int numBlocks = bits.Length;
            _rankIndex = new int[numBlocks + 1];
            _rankIndex[0] = 0;

            int total = 0;
            for (int i = 0; i < numBlocks; i++)
            {
                total += BitOperations.PopCount(bits[i]);
                _rankIndex[i + 1] = total;
            }
            _popCount = total;
        }

        /// <summary>Общее количество бит в векторе.</summary>
        public int Length => _length;

        /// <summary>Количество единиц в векторе.</summary>
        public int PopCount => _popCount;

        /// <summary>
        /// Получить бит на позиции i.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Get(int i)
        {
            if ((uint)i >= (uint)_length)
                throw new ArgumentOutOfRangeException(nameof(i));

            int blockIdx = i / BlockSize;
            int bitIdx = i % BlockSize;
            return ((_bits[blockIdx] >> bitIdx) & 1) == 1;
        }

        /// <summary>
        /// Rank(i) — количество единиц в позициях [0, i).
        /// O(1) благодаря предподсчёту.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Rank(int i)
        {
            if (i <= 0) return 0;
            if (i >= _length) return _popCount;

            int blockIdx = i / BlockSize;
            int bitIdx = i % BlockSize;

            // Сумма по предыдущим блокам
            int rank = _rankIndex[blockIdx];

            // Добавляем popcount внутри текущего блока до позиции bitIdx
            if (bitIdx > 0)
            {
                ulong mask = (1UL << bitIdx) - 1; // маска для первых bitIdx бит
                rank += BitOperations.PopCount(_bits[blockIdx] & mask);
            }

            return rank;
        }

        /// <summary>
        /// Select(k) — позиция k-й единицы (0-based).
        /// Возвращает -1, если единиц меньше k+1.
        /// O(log n) бинарным поиском по блокам + O(64) внутри блока.
        /// </summary>
        public int Select(int k)
        {
            if (k < 0 || k >= _popCount)
                return -1;

            // Бинарный поиск блока, содержащего k-ю единицу
            int lo = 0, hi = _bits.Length - 1;
            while (lo < hi)
            {
                int mid = (lo + hi) / 2;
                if (_rankIndex[mid + 1] <= k)
                    lo = mid + 1;
                else
                    hi = mid;
            }

            int blockIdx = lo;
            int remaining = k - _rankIndex[blockIdx]; // сколько единиц нужно найти внутри блока

            // Находим позицию remaining-й единицы внутри блока
            ulong block = _bits[blockIdx];
            int pos = SelectInBlock(block, remaining);

            return blockIdx * BlockSize + pos;
        }

        /// <summary>
        /// Находит позицию k-й единицы (0-based) внутри 64-битного слова.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int SelectInBlock(ulong block, int k)
        {
            // Используем PDEP если доступен (на современных x64 процессорах)
            // Fallback: последовательный поиск
            for (int i = 0; i < 64; i++)
            {
                if ((block & 1) == 1)
                {
                    if (k == 0)
                        return i;
                    k--;
                }
                block >>= 1;
            }
            return -1; // не должно происходить при корректном вызове
        }

        /// <summary>
        /// Размер структуры в байтах (приблизительно).
        /// </summary>
        public long GetSizeInBytes()
        {
            // ulong[] bits + int[] rankIndex + поля
            return _bits.Length * sizeof(ulong) 
                 + _rankIndex.Length * sizeof(int)
                 + sizeof(int) * 2; // _length, _popCount
        }
    }

    /// <summary>
    /// Builder для создания CompactBitVector.
    /// </summary>
    public sealed class BitVectorBuilder
    {
        private ulong[] _bits;
        private int _length;
        private int _capacity;

        public BitVectorBuilder(int initialCapacity = 1024)
        {
            _capacity = initialCapacity;
            _bits = new ulong[(initialCapacity + 63) / 64];
            _length = 0;
        }

        /// <summary>Текущая длина (количество добавленных бит).</summary>
        public int Length => _length;

        /// <summary>Добавить бит в конец.</summary>
        public void Append(bool bit)
        {
            EnsureCapacity(_length + 1);
            if (bit)
            {
                int blockIdx = _length / 64;
                int bitIdx = _length % 64;
                _bits[blockIdx] |= 1UL << bitIdx;
            }
            _length++;
        }

        /// <summary>Добавить несколько нулевых бит.</summary>
        public void AppendZeros(int count)
        {
            if (count <= 0) return;
            EnsureCapacity(_length + count);
            _length += count;
        }

        /// <summary>Добавить один установленный бит (1).</summary>
        public void AppendOne()
        {
            EnsureCapacity(_length + 1);
            int blockIdx = _length / 64;
            int bitIdx = _length % 64;
            _bits[blockIdx] |= 1UL << bitIdx;
            _length++;
        }

        /// <summary>Построить CompactBitVector.</summary>
        public CompactBitVector Build()
        {
            int numBlocks = (_length + 63) / 64;
            var bits = new ulong[numBlocks];
            Array.Copy(_bits, bits, numBlocks);
            return new CompactBitVector(bits, _length);
        }

        private void EnsureCapacity(int minCapacity)
        {
            if (minCapacity <= _capacity) return;

            int newCapacity = Math.Max(_capacity * 2, minCapacity);
            int newBlockCount = (newCapacity + 63) / 64;
            var newBits = new ulong[newBlockCount];
            Array.Copy(_bits, newBits, _bits.Length);
            _bits = newBits;
            _capacity = newCapacity;
        }
    }
}

