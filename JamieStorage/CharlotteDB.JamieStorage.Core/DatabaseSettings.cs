using System;
using System.Collections.Generic;
using System.Text;

namespace CharlotteDB.JamieStorage.Core
{
    public class DatabaseSettings
    {
        public int MaxInMemoryTableUse { get; set; } = 1024 * 1024 * 10;
        public int BufferAllocationSize { get; set; }
    }
}
