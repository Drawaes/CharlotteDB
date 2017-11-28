using System;
using System.Collections.Generic;
using System.Text;
using CharlotteDB.Core.Allocation;
using CharlotteDB.Core.Keys;
using CharlotteDB.JamieStorage.InMemory;
using Microsoft.Extensions.Logging;

namespace CharlotteDB.JamieStorage.Core
{
    public static class Database
    {
        public static Database<TComparer, TInMemoryStore> Create<TComparer, TInMemoryStore>(string folder, TComparer comparer, Allocator allocator, ILoggerFactory loggerFactory)
            where TComparer : IKeyComparer
            where TInMemoryStore : IInMemoryStore, new()
            => new Database<TComparer, TInMemoryStore>(folder, new DatabaseSettings(), comparer, allocator, loggerFactory);
    }
}
