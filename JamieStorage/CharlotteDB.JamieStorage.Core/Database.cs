using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CharlotteDB.JamieStorage.Core.Allocation;
using CharlotteDB.JamieStorage.Core.Keys;

namespace CharlotteDB.JamieStorage.Core
{
    public class Database<TComparer, TAllocator>
        where TComparer : IKeyComparer
        where TAllocator : IAllocator
    {
        private string _folder;
        private InMemory.SkipList<TComparer, TAllocator> _currentSkipList;
        private InMemory.SkipList<TComparer, TAllocator> _oldSkipList;
        private SemaphoreSlim _writeSemaphore = new SemaphoreSlim(1);
        private DatabaseSettings _settings;
        private TComparer _comparer;
        private TAllocator _allocator;

        public Database(string folder, DatabaseSettings settings, TComparer comparer, TAllocator allocator)
        {
            _allocator = allocator;
            _comparer = comparer;
            _folder = folder;
            _settings = settings;
            _currentSkipList = new InMemory.SkipList<TComparer, TAllocator>(comparer, allocator);
        }

        public Database(string folder, TComparer comparer, TAllocator allocator)
            : this(folder, new DatabaseSettings()
            {
                MaxInMemoryTableUse = 1024 * 1024 * 4,
            }, comparer, allocator)
        {
        }

        public bool TryGetData(Span<byte> key, out Span<byte> data) => throw new NotImplementedException();

        public Task PutAsync(Span<byte> key, out Span<byte> data) => throw new NotImplementedException();

        public ValueTask<bool> TryRemoveAsync(Span<byte> key) => throw new NotImplementedException();
    }
}
