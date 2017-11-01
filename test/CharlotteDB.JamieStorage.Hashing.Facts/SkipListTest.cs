using System;
using System.Collections.Generic;
using System.Text;
using CharlotteDB.JamieStorage.Core.Allocation;
using CharlotteDB.JamieStorage.Core.InMemory;
using CharlotteDB.JamieStorage.Core.Keys;
using Xunit;

namespace CharlotteDB.JamieStorage.Hashing
{
    public class SkipListTest
    {
        [Fact]
        public void Update()
        {
            var comparer = new ByteByByteComparer();
            var allocator = new Allocator(2048);
            var skipList = new SkipList<ByteByByteComparer, Allocator>(2048, comparer, allocator);

            var data = new byte[500];
            var mem = new Memory<byte>(data);

            skipList.Insert(GetSpanFromString("Tim"), mem);
            skipList.Insert(GetSpanFromString("Katie"), mem);
            skipList.Insert(GetSpanFromString("Blah"), mem);
        }

        private static Span<byte> GetSpanFromString(string input)
        {
            var bytes = Encoding.UTF8.GetBytes(input);
            return (Span<byte>)bytes;
        }
    }
}
