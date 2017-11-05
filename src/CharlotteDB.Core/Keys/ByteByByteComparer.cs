using System;
using System.Collections.Generic;
using System.Text;

namespace CharlotteDB.Core.Keys
{
    public class ByteByByteComparer : IKeyComparer
    {
        public int Compare(Span<byte> key1, Span<byte> key2)
        {
            var max = key1.Length > key2.Length ? key2.Length : key1.Length;
            for (var i = 0; i < max; i++)
            {
                if (key1[i] == key2[i]) continue;
                if (key1[i] > key2[i]) return 1;

                return -1;
            }

            if (key1.Length == key2.Length) return 0;
            if (key1.Length == max) return 1;

            return -1;
        }

        public bool Equals(Span<byte> key1, Span<byte> key2) => key1.SequenceEqual(key2);
    }
}
