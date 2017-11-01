using System;
using System.Collections.Generic;
using System.Text;

namespace CharlotteDB.JamieStorage.Core
{
    public class DatabaseSettings
    {
        public int MaxInMemoryTableUse { get; set; }
        public int BufferAllocationSize { get; set; }
    }
}
