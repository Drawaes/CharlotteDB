using System;
using System.Collections.Generic;
using System.Text;
using CharlotteDB.Core.Allocation;
using CharlotteDB.Core.Keys;
using Microsoft.Extensions.Logging;

namespace CharlotteDB.JamieStorage.Core
{
    public static class Database
    {
        public static Database<TComparer> Create<TComparer>(string folder, TComparer comparer, Allocator allocator, ILoggerFactory loggerFactory)
            where TComparer : IKeyComparer => new Database<TComparer>(folder, new DatabaseSettings(), comparer, allocator, loggerFactory);
    }
}
