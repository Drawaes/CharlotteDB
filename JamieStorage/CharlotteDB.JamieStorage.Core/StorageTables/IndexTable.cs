using System;
using System.Collections.Generic;
using System.Text;

namespace CharlotteDB.JamieStorage.Core.StorageTables
{
    internal struct IndexTable
    {
        public long BloomFilterIndex;
        public long IndexFilterIndex;
        public long IndexFilterLength;
    }
}
