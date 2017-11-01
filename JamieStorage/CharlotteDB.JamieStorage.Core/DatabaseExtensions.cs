using System;
using System.Collections.Generic;
using System.Text;
using CharlotteDB.JamieStorage.Core.Allocation;
using CharlotteDB.JamieStorage.Core.InMemory;
using CharlotteDB.JamieStorage.Core.Keys;

namespace CharlotteDB.JamieStorage.Core
{
    public static class Database
    {
        public static Database<TComparer, TAllocator> Create<TComparer, TAllocator>(string folder, TComparer comparer, TAllocator allocator)
            where TComparer : IKeyComparer
            where TAllocator : IAllocator => new Database<TComparer, TAllocator>(folder, comparer, allocator);
    }
}
