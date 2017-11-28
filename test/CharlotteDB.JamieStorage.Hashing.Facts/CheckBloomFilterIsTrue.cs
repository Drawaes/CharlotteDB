using System;
using System.Collections.Generic;
using System.IO;
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

            var bloom = new BloomFilter(count, 4, 3, new FNVHash());

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

        [Fact]
        public void TestDictionaryFNV1Hash()
        {
            var list = File.ReadAllLines("C:\\code\\words.txt");
            var bloom = new BloomFilter(list.Length, 4, 3, new FNV1Hash());

            for (var i = 0; i < list.Length; i++)
            {
                var item = System.Text.Encoding.UTF8.GetBytes(list[i]);
                bloom.Add(item);
            }

            for (var i = 0; i < list.Length; i++)
            {
                var item = System.Text.Encoding.UTF8.GetBytes(list[i]);
                Assert.True(bloom.PossiblyContains(item));
            }
        }

        [Fact]
        public void TestDictionaryFNV1HashSaveAndLoad()
        {
            var list = File.ReadAllLines("C:\\code\\words.txt");
            var bloom = new BloomFilter(list.Length, 4, 3, new FNV1Hash());

            for (var i = 0; i < list.Length; i++)
            {
                var item = System.Text.Encoding.UTF8.GetBytes(list[i]);
                bloom.Add(item);
            }

            using (var mem = new MemoryStream())
            {
                bloom.SaveAsync(mem).Wait();
                var storage = mem.ToArray();
                bloom = new BloomFilter(storage, new FNV1Hash());
            }

            for (var i = 0; i < list.Length; i++)
            {
                var item = System.Text.Encoding.UTF8.GetBytes(list[i]);
                Assert.True(bloom.PossiblyContains(item));
            }
        }
    }
}
