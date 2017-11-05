using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CharlotteDB.JamieStorage.Core.Allocation;
using CharlotteDB.JamieStorage.Core.InMemory;
using CharlotteDB.JamieStorage.Core.Keys;

namespace CharlotteDB.JamieStorage.Core
{
    public class Database<TComparer, TAllocator> : IDisposable
        where TComparer : IKeyComparer
        where TAllocator : IAllocator
    {
        private string _folder;
        private SkipList<TComparer, TAllocator> _currentSkipList;
        private SkipList<TComparer, TAllocator> _oldSkipList;
        private SemaphoreSlim _writeSemaphore = new SemaphoreSlim(1);
        private List<StorageTables.StorageFile<TComparer, TAllocator>> _storageTables = new List<StorageTables.StorageFile<TComparer, TAllocator>>();
        private DatabaseSettings _settings;
        private TComparer _comparer;
        private TAllocator _allocator;
        private int _currentLevelOneCount = 0;

        public Database(string folder, DatabaseSettings settings, TComparer comparer, TAllocator allocator)
        {
            _allocator = allocator;
            _comparer = comparer;
            _folder = folder;
            _settings = settings;
            _currentSkipList = new SkipList<TComparer, TAllocator>(comparer, allocator);
        }

        public Database(string folder, TComparer comparer, TAllocator allocator)
            : this(folder, new DatabaseSettings()
            {
                MaxInMemoryTableUse = 1024 * 1024 * 14,
            }, comparer, allocator)
        {
        }

        public TComparer Comparer => _comparer;

        public bool TryGetData(Span<byte> key, out Memory<byte> data)
        {
            var result = _currentSkipList.TryFind(key, out data);
            if (result == SearchResult.NotFound)
            {
                if (_oldSkipList != null)
                {
                    result = _oldSkipList.TryFind(key, out data);
                    if (result == SearchResult.NotFound)
                    {
                        throw new NotImplementedException("Need to keep searching down the layers");
                    }
                }
            }
            return result == SearchResult.Found;
        }

        public Task PutAsync(Memory<byte> key, Memory<byte> data)
        {
            _currentSkipList.Insert(key.Span, data.Span);
            if (_currentSkipList.SpaceUsed > _settings.MaxInMemoryTableUse)
            {
                return WriteInMemoryTable();
            }
            return Task.CompletedTask;
        }

        public Task TryRemoveAsync(Memory<byte> key)
        {
            _currentSkipList.Remove(key.Span);
            if (_currentSkipList.SpaceUsed > _settings.MaxInMemoryTableUse)
            {
                return WriteInMemoryTable();
            }
            return Task.CompletedTask;
        }

        internal async Task<SearchResult> FindNodeAsync(Memory<byte> key)
        {
            for(var i = _storageTables.Count -1; i >= 0;i--)
            {
                var st = _storageTables[i];
                var searchResult = await st.FindNodeAsync(key);
                if(searchResult == SearchResult.Deleted || searchResult == SearchResult.Found)
                {
                    return searchResult;
                }
            }
            return SearchResult.NotFound;
        }

        private async Task WriteInMemoryTable()
        {
            _oldSkipList = _currentSkipList;
            _currentSkipList = new SkipList<TComparer, TAllocator>(_comparer, _allocator);
            var storage = new StorageTables.StorageFile<TComparer, TAllocator>(NextFileTableName(), 5, this);
            await storage.WriteInMemoryTableAsync(_oldSkipList);
            _storageTables.Add(storage);
            _oldSkipList = null;
        }

        private string NextFileTableName() => System.IO.Path.Combine(_folder, $"table-1-{_currentLevelOneCount++}.bin");

        public void Dispose()
        {
            WriteInMemoryTable().GetAwaiter().GetResult();
            foreach(var st in _storageTables)
            {
                st.Dispose();
            }
        }
    }
}
