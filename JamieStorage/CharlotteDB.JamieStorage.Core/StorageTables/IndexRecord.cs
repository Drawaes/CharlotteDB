using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace CharlotteDB.JamieStorage.Core.StorageTables
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal struct IndexRecord
    {
        public int BlockStart;
        public int BlockEnd;
        public ushort KeySize;
    }
}
