using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace CharlotteDB.JamieStorage.Hashing
{
    public class SkipListTest
    {
        [Fact]
        public void Update()
        {
            var comparer = new Core.InMemory.ByteByByteComparer();
            var skipList = new Core.InMemory.SkipList<Core.InMemory.ByteByByteComparer>(2048, comparer);

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
