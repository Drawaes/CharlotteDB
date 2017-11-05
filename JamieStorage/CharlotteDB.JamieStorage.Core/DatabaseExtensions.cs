using System;
using System.Collections.Generic;
using System.Text;
using CharlotteDB.Core.Allocation;
using CharlotteDB.Core.Keys;

namespace CharlotteDB.JamieStorage.Core
{
    public static class Database
    {
        public static Database<TComparer> Create<TComparer>(string folder, TComparer comparer, Allocator allocator)
            where TComparer : IKeyComparer => new Database<TComparer>(folder, comparer, allocator);
    }
}
