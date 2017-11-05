using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CharlotteDB.Core;

namespace CharlotteDB.JamieStorage.Hashing
{
    public class BloomFilter<THash> : IBloomFilter where THash : IHash, new()
    {
        private THash _hasher;
        private int _bitCount;
        private int _numberOfHashes;
        private int[] _backingArray;
        private ThreadLocal<long> _count = new ThreadLocal<long>(true);
        private ThreadLocal<ulong[]> _hashWorkingSet;

        public BloomFilter(int estimatedElements, int bitsPerElement, THash hasher)
            : this(estimatedElements * bitsPerElement, hasher, (int)Math.Round(Math.Log(2.0) * bitsPerElement / estimatedElements))
        {
        }

        public BloomFilter(int bitCount, THash hasher, int numberOfHashes)
        {
            _hasher = hasher;
            _numberOfHashes = numberOfHashes;
            _hashWorkingSet = new ThreadLocal<ulong[]>(() => new ulong[_numberOfHashes]);

            _bitCount = bitCount;
            _backingArray = new int[(_bitCount / (sizeof(int) * 8)) + 1];
            _bitCount = _backingArray.Length * sizeof(int) * 8;
        }

        public long Count => _count.Values.Sum();

        public void Add(Span<byte> buffer)
        {
            var value = _hasher.Hash(buffer);
            var index = value % (ulong)_bitCount;
            SetBit(index);

            for (var i = 1; i < _numberOfHashes; i++)
            {
                value = _hasher.ReHash(buffer, value);
                index = value % (ulong)_bitCount;
                SetBit(index);
            }

            _count.Value++;
        }

        private void SetBit(ulong index)
        {
            var byteIndex = index >> 5;
            var bitShift = 1 << (byte)(index & 0x1F);
            var val = _backingArray[byteIndex];
            if ((val & bitShift) != 0)
            {
                return;
            }

            var newValue = val | bitShift;
            while (Interlocked.CompareExchange(ref _backingArray[byteIndex], newValue, val) != val)
            {
                val = _backingArray[byteIndex];
                if ((val & bitShift) != 0)
                {
                    return;
                }
                newValue = val | bitShift;
            }
        }

        private bool GetBit(ulong index)
        {
            var byteIndex = index >> 5;
            var bitShift = 1 << (byte)(index & 0x1F);
            var val = _backingArray[byteIndex];
            var result = (val & bitShift) != 0;
            return result;
        }

        public bool PossiblyContains(Span<byte> buffer)
        {
            var value = _hasher.Hash(buffer);
            var index = value % (ulong)_bitCount;
            if (!GetBit(index))
            {
                return false;
            }

            for (var i = 1; i < _numberOfHashes; i++)
            {
                value = _hasher.ReHash(buffer, value);
                index = value % (ulong)_bitCount;
                if (!GetBit(index))
                {
                    return false;
                }
            }
            return true;
        }

        public Task SaveAsync(Stream outputStream)
        {
            var backingArray = new byte[OutputSize];
            var span = new Span<byte>(backingArray);
            span = span.WriteAdvance(OutputSize - sizeof(int));
            span = span.WriteAdvance(_numberOfHashes);
            span = span.WriteAdvance(Count);
            ((Span<int>)_backingArray).NonPortableCast<int, byte>().CopyTo(span);
            return outputStream.WriteAsync(backingArray);
        }

        public double FalsePositiveErrorRate
        {
            get
            {
                var x = 1.0 - (1.0 / _bitCount);
                x = Math.Pow(x, _numberOfHashes * _count.Values.Sum());
                x = 1.0 - x;
                x = Math.Pow(x, _numberOfHashes);
                return x;
            }
        }

        public int OutputSize => (_bitCount / 8) + sizeof(int) * 2 + sizeof(long);
    }
}
