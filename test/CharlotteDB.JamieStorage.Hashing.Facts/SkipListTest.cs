using System;
using System.Collections.Generic;
using System.Text;
using CharlotteDB.Core.Allocation;
using CharlotteDB.Core.Keys;
using CharlotteDB.JamieStorage.InMemory;
using Xunit;

namespace CharlotteDB.JamieStorage.Hashing
{
    public class SkipListTest
    {
        [Fact]
        public void Update()
        {
            var comparer = new ByteByByteComparer();
            var allocator = new DummyAllocator(2048);
            var skipList = new SkipList2<ByteByByteComparer>(comparer, allocator);

            var data = new byte[500];
            
            skipList.Insert(GetSpanFromString("Tim"), data);
            skipList.Insert(GetSpanFromString("Katie"), data);
            skipList.Insert(GetSpanFromString("Blah"), data);
        }

        private static Span<byte> GetSpanFromString(string input)
        {
            var bytes = Encoding.UTF8.GetBytes(input);
            return (Span<byte>)bytes;
        }
    }
}
