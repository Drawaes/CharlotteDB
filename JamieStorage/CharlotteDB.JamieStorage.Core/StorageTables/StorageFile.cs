using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using CharlotteDB.Core;
using CharlotteDB.JamieStorage.Core.Allocation;
using CharlotteDB.JamieStorage.Core.InMemory;
using CharlotteDB.JamieStorage.Core.Keys;

namespace CharlotteDB.JamieStorage.Core.StorageTables
{
    public class StorageFile<TComparer, TAllocator>
        where TComparer : IKeyComparer
        where TAllocator : IAllocator
    {
        private string _fileName;
        private int _bitsToUseForBloomFilter;
        private Hashing.BloomFilter<Hashing.FNV1Hash> _bloomFilter;
        private Database<TComparer, TAllocator> _database;
        private long _bloomFilterIndex;

        public StorageFile(string fileName, int bitsToUseForBloomFilter, Database<TComparer, TAllocator> database)
        {
            _bitsToUseForBloomFilter = bitsToUseForBloomFilter;
            _fileName = fileName;
            _database = database;
        }

        public async Task WriteInMemoryTableAsync(SkipList<TComparer, TAllocator> inMemory)
        {
            var tempBuffer = new byte[8];
            
            using (var file = System.IO.File.Open(_fileName, System.IO.FileMode.CreateNew, System.IO.FileAccess.Write, System.IO.FileShare.None))
            {
                var deletedCount = await WriteDeletedRecords(inMemory, tempBuffer, file);

                _bloomFilter = new Hashing.BloomFilter<Hashing.FNV1Hash>(inMemory.Count - deletedCount, _bitsToUseForBloomFilter, new Hashing.FNV1Hash());
                inMemory.Reset();
                var index = new List<(Memory<byte> key, long fileIndex)>();
                while (inMemory.Next())
                {
                    var node = inMemory.CurrentNode;
                    if(node.State != ItemState.Alive)
                    {
                        continue;
                    }

                    _bloomFilter.Add(node.Key.Span);
                    index.Add((node.Key, file.Position));
                    (new Span<byte>(tempBuffer)).WriteAdvance(node.Key.Length);
                    await file.WriteAsync(tempBuffer, 0, 4);
                    await file.WriteAsync(node.Key);

                    (new Span<byte>(tempBuffer)).WriteAdvance(node.Data.Length);
                    await file.WriteAsync(tempBuffer, 0, 4);
                    await file.WriteAsync(node.Data);

                }

                // write out bloom filter
                _bloomFilterIndex = file.Position;
                await _bloomFilter.SaveAsync(file);

                // now we need to write out indexes
                throw new NotImplementedException();
            }
        }

        private async Task<int> WriteDeletedRecords(SkipList<TComparer, TAllocator> inMemory, byte[] tempBuffer, System.IO.FileStream file)
        {
            await file.WriteAsync(tempBuffer);
            var deletedCount = 0;
            while (inMemory.Next())
            {
                var node = inMemory.CurrentNode;
                switch (node.State)
                {
                    case ItemState.Deleted:
                        var searchResult = await _database.FindNodeAsync(node.Key);
                        if (searchResult == SearchResult.Found)
                        {
                            ((Span<byte>)tempBuffer).WriteAdvance(node.Key.Length);
                            await file.WriteAsync(new Memory<byte>(tempBuffer, 0, 4));
                            await file.WriteAsync(node.Key);
                        }
                        deletedCount++;
                        break;
                }
            }
            var currentLocation = file.Position;
            file.Position = 0;
            ((Span<byte>)tempBuffer).WriteAdvance(currentLocation);
            await file.WriteAsync(tempBuffer);
            file.Position = currentLocation;
            return deletedCount;
        }

        public bool MayContainKey(Span<byte> key) => throw new NotImplementedException();

        internal Task<SearchResult> FindNodeAsync(Memory<byte> key) => throw new NotImplementedException();
    }
}
