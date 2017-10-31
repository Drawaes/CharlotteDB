using System;
using System.Collections.Generic;
using System.Text;

namespace CharlotteDB.JamieStorage.Core.InMemory
{
    public struct SkipListNode
    {
        public ulong NextNode;
        public int Height;
        public int KeyLength;
    }
}
