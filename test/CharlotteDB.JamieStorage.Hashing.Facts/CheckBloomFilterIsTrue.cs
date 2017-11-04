using System;
using System.Collections.Generic;
using Xunit;

namespace CharlotteDB.JamieStorage.Hashing.Facts
{
    public class CheckBloomFilterIsTrue
    {
        [Fact]
        public void FuzzTest()
        {
            var count = 4_000_000;
            var size = 8;
            var list = new byte[count][];
            var rnd = new Random();

            var bloom = new BloomFilter<FNVHash>(count, 4, new FNVHash());

            for (var i = 0; i < list.Length; i++)
            {
                var buffer = new byte[size];
                rnd.NextBytes(buffer);
                list[i] = buffer;
                bloom.Add(buffer);
                Assert.True(bloom.PossiblyContains(buffer));
            }

            for (var i = 0; i < list.Length; i++)
            {
                Assert.True(bloom.PossiblyContains(list[i]));
            }

            var expectedError = bloom.FalsePositiveErrorRate;
        }
    }
}
