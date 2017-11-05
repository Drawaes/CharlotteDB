using System;
using System.Collections.Generic;
using System.Text;

namespace CharlotteDB.JamieStorage.Core.StorageTables
{
    internal struct EntryHeader
    {
        public ushort KeySize;
        public int DataSize;
    }
}
