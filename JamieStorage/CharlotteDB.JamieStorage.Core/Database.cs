using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CharlotteDB.Core;
using CharlotteDB.Core.Allocation;
using CharlotteDB.Core.Keys;
using CharlotteDB.JamieStorage.Core.StorageTables;
using CharlotteDB.JamieStorage.Hashing;
using CharlotteDB.JamieStorage.InMemory;
using Microsoft.Extensions.Logging;

namespace CharlotteDB.JamieStorage.Core
{
    public class Database<TComparer, TInMemoryList> : IDisposable, IDatabase
        where TComparer : IKeyComparer
        where TInMemoryList : IInMemoryStore, new()
    {
        private string _folder;
        private IInMemoryStore _currentSkipList;
        private IInMemoryStore _oldSkipList;
        private List<StorageFile> _storageTables = new List<StorageFile>();
        private FNV1Hash _hasher;
        private DatabaseSettings _settings;
        private TComparer _comparer;
        private Allocator _allocator;
        private int _currentLevelOneCount = 0;
        private ILoggerFactory _loggerFactory;
        private ILogger _logger;
        private SemaphoreSlim _semaphore = new SemaphoreSlim(0);
        private ManualResetEvent _finishedWrite = new ManualResetEvent(false);
        // private Task _backgroundWriter;

        public Database(string folder, DatabaseSettings settings, TComparer comparer, Allocator allocator, ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory;
            _logger = _loggerFactory.CreateLogger<Database<TComparer, TInMemoryList>>();
            _hasher = new FNV1Hash();
            _allocator = allocator;
            _comparer = comparer;
            _folder = folder;
            _settings = settings;
            _currentSkipList = new TInMemoryList();
            _currentSkipList.Init(_allocator, _comparer);
        }

        public IKeyComparer Comparer => _comparer;
        public IHash Hasher => _hasher;

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

        public Task FlushToDisk() => WriteInMemoryTableAsync();

        private async Task BackgroundWriterLoop()
        {
            while (true)
            {
                await _semaphore.WaitAsync();
                if (_currentSkipList.SpaceUsed > _settings.MaxInMemoryTableUse)
                {
                    await WriteInMemoryTableAsync();
                }
                _finishedWrite.Set();
            }
        }

        public Task PutAsync(Memory<byte> key, Memory<byte> data)
        {
            _currentSkipList.Insert(key.Span, data.Span);
            if (_currentSkipList.SpaceUsed > _settings.MaxInMemoryTableUse)
            {
                return WriteInMemoryTableAsync();
            }

            return Task.CompletedTask;
        }

        public Task TryRemoveAsync(Memory<byte> key)
        {
            _currentSkipList.Remove(key.Span);
            if (_currentSkipList.SpaceUsed > _settings.MaxInMemoryTableUse)
            {
                return WriteInMemoryTableAsync();
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

        public bool MayContainNode(Memory<byte> key)
        {
            for (var i = _storageTables.Count - 1; i >= 0; i--)
            {
                var st = _storageTables[i];
                if (st.MayContainNode(key))
                {
                    return true;
                }
            }

            return false;
        }

        private async Task WriteInMemoryTableAsync()
        {
            using (var scope = _logger.BeginScope("Write Memory Table"))
            {
                _logger.LogInformation("Starting");

                Interlocked.Exchange(ref _oldSkipList, _currentSkipList);
                var currentList = new TInMemoryList();
                currentList.Init(_allocator, _comparer);
                Interlocked.Exchange(ref _currentSkipList, currentList);

                var storage = new StorageFile(NextFileTableName(), this);
                await storage.WriteInMemoryTableAsync(_oldSkipList ?? throw new ArgumentNullException(nameof(_oldSkipList)), 3);

                // TODO : Make lockless threadsafe
                _storageTables.Add(storage);
                var sList = _oldSkipList;

                Interlocked.Exchange(ref _oldSkipList, null);

                // TODO : Need to ensure there is no current reading transactions when we dispose
                //sList.Dispose();
                _logger.LogInformation("Finished");
            }
        }

        private string NextFileTableName() => System.IO.Path.Combine(_folder, $"table-1-{_currentLevelOneCount++}.bin");

        public void Dispose()
        {
            _semaphore.Release();
            //WriteInMemoryTable().GetAwaiter().GetResult();
            foreach (var st in _storageTables)
            {
                st.Dispose();
            }
        }
    }
}
