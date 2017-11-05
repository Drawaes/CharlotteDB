using System;

namespace CharlotteDB.Core.Keys
{
    public interface IKeyComparer
    {
        bool Equals(Span<byte> key1, Span<byte> key2);
        int Compare(Span<byte> key1, Span<byte> key2);
    }
}
