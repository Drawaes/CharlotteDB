using System;
using System.Collections.Generic;
using System.Text;

namespace CharlotteDB.JamieStorage.InMemory
{
    public struct MemoryNode
    {
        public Memory<byte> Key;
        public Memory<byte> Data;
        public ItemState State;
    }
}
