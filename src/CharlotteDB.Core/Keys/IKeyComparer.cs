using System;
using System.Collections;
using System.Collections.Generic;

namespace CharlotteDB.Core.Keys
{
    public interface IKeyComparer : IComparer<Memory<byte>>
    {
        bool Equals(Span<byte> key1, Span<byte> key2);
        int Compare(Span<byte> key1, Span<byte> key2);
    }
}
