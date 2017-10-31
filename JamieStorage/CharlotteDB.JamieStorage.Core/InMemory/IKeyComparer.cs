using System;
using System.Collections.Generic;
using System.Text;

namespace CharlotteDB.JamieStorage.Core.InMemory
{
    public interface IKeyComparer
    {
        bool Equals(Span<byte> key1, Span<byte> key2);
        int Compare(Span<byte> key1, Span<byte> key2);
    }
}
