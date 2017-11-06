using System;
using System.Collections.Generic;
using System.Text;

namespace CharlotteDB.JamieStorage.Hashing
{
    public static class BloomFilter
    {
        public static BloomFilter<T> Create<T>(int estimatedElements, int bitCount, int hashCount, T hashType) where T : IHash
            => new BloomFilter<T>(estimatedElements , bitCount, hashCount, hashType);
    }
}
