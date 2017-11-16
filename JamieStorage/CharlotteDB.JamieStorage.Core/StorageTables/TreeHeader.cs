using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace CharlotteDB.JamieStorage.Core.StorageTables
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal struct TreeHeader
    {
        public ushort KeySize;
        public int DataSize;
        public int LeftNode;
        public int RightNode;
    }
}
