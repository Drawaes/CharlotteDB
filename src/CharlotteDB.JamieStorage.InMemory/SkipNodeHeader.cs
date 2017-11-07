using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace CharlotteDB.JamieStorage.InMemory
{
    [StructLayout(LayoutKind.Sequential, Pack =1)]
    internal struct SkipNodeHeader
    {
        public ushort Height;
        public ushort KeyLength;
        public int DataPointer;
        public ItemState State;
    }
}
