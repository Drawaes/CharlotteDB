using System;
using System.Collections.Generic;
using System.Text;

namespace CharlotteDB.Core.Keys
{
    public class CharComparer : IKeyComparer
    {
        public unsafe int Compare(Span<byte> key1, Span<byte> key2)
        {
            fixed(void* ptr1 = &key1.DangerousGetPinnableReference())
            fixed(void* ptr2 = &key2.DangerousGetPinnableReference())
            {
                var str1 = new string((char*)ptr1, 0, key1.Length);
                var str2 = new string((char*)ptr2, 0, key2.Length);

                return StringComparer.OrdinalIgnoreCase.Compare(str1, str2);
            }
        }

        public bool Equals(Span<byte> key1, Span<byte> key2)
        {
            return Compare(key1, key2) == 0;
        }
    }
}
