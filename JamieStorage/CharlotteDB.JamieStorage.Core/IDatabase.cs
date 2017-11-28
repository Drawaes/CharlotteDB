using System;
using System.Collections.Generic;
using System.Text;
using CharlotteDB.Core.Keys;
using CharlotteDB.JamieStorage.Hashing;

namespace CharlotteDB.JamieStorage.Core
{
    public interface IDatabase
    {
        IKeyComparer Comparer { get; }
        IHash Hasher { get; }
        bool MayContainNode(Memory<byte> key);
    }
}
