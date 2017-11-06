using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CharlotteDB.Core;
using CharlotteDB.Core.Allocation;
using CharlotteDB.Core.Keys;
using CharlotteDB.JamieStorage.Core.StorageTables;
using CharlotteDB.JamieStorage.InMemory;

namespace CharlotteDB.JamieStorage.Core
{
    public class Database<TComparer> : IDisposable where TComparer : IKeyComparer
    {
        private string _folder;
        private SkipList<TComparer> _currentSkipList;
        private SkipList<TComparer> _oldSkipList;
        private List<StorageFile<TComparer>> _storageTables = new List<StorageFile<TComparer>>();
        private DatabaseSettings _settings;
        private TComparer _comparer;
        private Allocator _allocator;
        private int _currentLevelOneCount = 0;

        public Database(string folder, DatabaseSettings settings, TComparer comparer, Allocator allocator)
        {
            _allocator = allocator;
            _comparer = comparer;
            _folder = folder;
            _settings = settings;
            _currentSkipList = new SkipList<TComparer>(comparer, allocator);
        }

        public Database(string folder, TComparer comparer, Allocator allocator) : this(folder, new DatabaseSettings(), comparer, allocator)
        {
        }

        public TComparer Comparer => _comparer;

        public (bool found, Memory<byte> data) TryGetData(Memory<byte> key)
        {
            var result = _currentSkipList.TryFind(key.Span, out var data);
            switch (result)
            {
                case SearchResult.Deleted:
                    return (false, default);
                case SearchResult.Found:
                    return (true, data);
                case SearchResult.NotFound:
                    if (_oldSkipList != null)
                    {
                        result = _oldSkipList.TryFind(key.Span, out data);
                        if (result == SearchResult.Deleted)
                        {
                            return (false, default);
                        }
                        else if (result == SearchResult.Found)
                        {
                            return (true, data);
                        }
                    }
                    break;
            }

            for (var i = _storageTables.Count - 1; i >= 0; i--)
            {
                var outputResult = _storageTables[i].TryGetData(key);
                if (outputResult.result == SearchResult.Found)
                {
                    return (true, outputResult.data);
                }
                else if (outputResult.result == SearchResult.Deleted)
                {
                    return (false, default);
                }
            }

            return (false, default);
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

        internal SearchResult FindNode(Memory<byte> key)
        {
            for (var i = _storageTables.Count - 1; i >= 0; i--)
            {
                var st = _storageTables[i];
                var searchResult = st.FindNode(key);
                if (searchResult == SearchResult.Deleted || searchResult == SearchResult.Found)
                {
                    return searchResult;
                }
            }

            return SearchResult.NotFound;
        }

        private async Task WriteInMemoryTable()
        {
            _oldSkipList = _currentSkipList;
            _currentSkipList = new SkipList<TComparer>(_comparer, _allocator);
            var storage = new StorageFile<TComparer>(NextFileTableName(), this);
            await storage.WriteInMemoryTableAsync(_oldSkipList, 4);
            _storageTables.Add(storage);
            _oldSkipList = null;
        }

        private string NextFileTableName() => System.IO.Path.Combine(_folder, $"table-1-{_currentLevelOneCount++}.bin");

        public void Dispose()
        {
            WriteInMemoryTable().GetAwaiter().GetResult();
            foreach (var st in _storageTables)
            {
                st.Dispose();
            }
        }
    }
}
