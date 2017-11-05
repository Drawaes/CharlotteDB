using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Text;
using System.Threading.Tasks;
using CharlotteDB.Core;
using CharlotteDB.JamieStorage.Core.Allocation;
using CharlotteDB.JamieStorage.Core.InMemory;
using CharlotteDB.JamieStorage.Core.Keys;

namespace CharlotteDB.JamieStorage.Core.StorageTables
{
    public class StorageFile<TComparer, TAllocator> : IDisposable
        where TComparer : IKeyComparer
        where TAllocator : IAllocator
    {
        private string _fileName;
        private int _bitsToUseForBloomFilter;
        private Hashing.BloomFilter<Hashing.FNV1Hash> _bloomFilter;
        private Database<TComparer, TAllocator> _database;
        private long _bloomFilterIndex;
        private long _indexFilter;
        private MemoryMappedFile _memoryMappedFile;
        private MemoryMappedViewStream _indexView;

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
                    if (node.State != ItemState.Alive)
                    {
                        continue;
                    }

                    _bloomFilter.Add(node.Key.Span);
                    index.Add((node.Key, file.Position));
                    (new Span<byte>(tempBuffer)).WriteAdvance((ushort)node.Key.Length);
                    await file.WriteAsync(tempBuffer, 0, 2);
                    (new Span<byte>(tempBuffer)).WriteAdvance(node.Data.Length);
                    await file.WriteAsync(tempBuffer, 0, 4);

                    await file.WriteAsync(node.Key);
                    await file.WriteAsync(node.Data);
                }

                // write out bloom filter
                _bloomFilterIndex = file.Position;
                await _bloomFilter.SaveAsync(file);

                _indexFilter = file.Position;
                var i = 0;
                for (; i < index.Count; i += 10)
                {
                    ((Span<byte>)tempBuffer).WriteAdvance((ushort)index[i].key.Length);
                    await file.WriteAsync(tempBuffer, 0, 2);
                    tempBuffer.AsSpan().WriteAdvance(index[i].fileIndex);
                    await file.WriteAsync(tempBuffer, 0, 8);
                    await file.WriteAsync(index[i].key);
                }
                var indexSize = file.Position - _indexFilter;

                tempBuffer = new byte[24];
                ((Span<byte>)tempBuffer).WriteAdvance(_bloomFilterIndex).WriteAdvance(_indexFilter).WriteAdvance(indexSize);
                await file.WriteAsync(tempBuffer);
            }
            LoadFile();
        }

        private void LoadFile()
        {
            _memoryMappedFile = MemoryMappedFile.CreateFromFile(_fileName, System.IO.FileMode.Open);
            var fileInfo = new System.IO.FileInfo(_fileName);
            var fileSize = fileInfo.Length;
            var indexNames = fileSize - 24;
            var span = (Span<byte>)new byte[24];
            using (var indexStream = _memoryMappedFile.CreateViewStream(indexNames, 24))
            {
                var count = indexStream.Read(span);
                if (count != span.Length)
                {
                    throw new NotImplementedException();
                }
                span = span.ReadAdvance(out _bloomFilterIndex);
                span = span.ReadAdvance(out _indexFilter);
                span = span.ReadAdvance<long>(out var indexSize);
                _indexView = _memoryMappedFile.CreateViewStream(_indexFilter, indexSize);
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

        public bool MayContainKey(Span<byte> key) => _bloomFilter.PossiblyContains(key);

        internal async Task<SearchResult> FindNodeAsync(Memory<byte> key)
        {
            if (MayContainKey(key.Span))
            {
                var (index, exactMatch) = await FindBlockToSearch(key);
                if (exactMatch)
                {
                    return SearchResult.Found;
                }
                var mem = await FindBlock(key, index);
                if (mem.Length == 0)
                {
                    return SearchResult.NotFound;
                }
                return SearchResult.Found;
            }
            return SearchResult.NotFound;
        }

        private async Task<Memory<byte>> FindBlock(Memory<byte> key, long blockStart)
        {
            var lengthsBuffer = new byte[6];
            var keyBuffer = ArrayPool<byte>.Shared.Rent(256);
            try
            {
                using (var stream = _memoryMappedFile.CreateViewStream(blockStart, _bloomFilterIndex - blockStart))
                {
                    while (stream.Position < stream.Length)
                    {
                        var count = await stream.ReadAsync(lengthsBuffer);
                        if (count != lengthsBuffer.Length)
                        {
                            throw new NotImplementedException();
                        }
                        lengthsBuffer.AsSpan().ReadAdvance<ushort>(out var keyLength).ReadAdvance<int>(out var dataLength);
                        if (keyBuffer.Length < keyLength)
                        {
                            ArrayPool<byte>.Shared.Return(keyBuffer);
                            keyBuffer = ArrayPool<byte>.Shared.Rent(keyLength);
                        }
                        count = await stream.ReadAsync(keyBuffer, 0, keyLength);
                        if (count != keyLength)
                        {
                            throw new NotImplementedException();
                        }
                        var compare = _database.Comparer.Compare(key.Span, keyBuffer.AsSpan().Slice(0, keyLength));
                        if (compare == 0)
                        {
                            throw new NotImplementedException("Need to read out the data and slice it");
                        }
                        else if (compare > 0)
                        {
                            stream.Position += dataLength;
                        }
                        else if (compare < 0)
                        {
                            return default;
                        }
                    }
                }
                return default;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(keyBuffer);
            }
        }

        private async Task<(long index, bool exactMatch)> FindBlockToSearch(Memory<byte> key)
        {
            _indexView.Position = 0;
            var tempBuffer = (Memory<byte>)new byte[10];
            var tempKeyBuffer = ArrayPool<byte>.Shared.Rent(2048);
            try
            {
                var previousIndex = 0L;
                while (_indexView.Position < _indexView.Length)
                {
                    var count = await _indexView.ReadAsync(tempBuffer);
                    if (count != tempBuffer.Length)
                    {
                        throw new NotImplementedException();
                    }
                    tempBuffer.Span.ReadAdvance<ushort>(out var keySize).ReadAdvance<long>(out var recordPosition);
                    if (tempKeyBuffer.Length < keySize)
                    {
                        ArrayPool<byte>.Shared.Return(tempKeyBuffer);
                        tempKeyBuffer = ArrayPool<byte>.Shared.Rent(keySize);
                    }
                    count = await _indexView.ReadAsync(tempKeyBuffer, 0, keySize);
                    if (count != keySize)
                    {
                        throw new NotImplementedException();
                    }
                    var compare = _database.Comparer.Compare(key.Span, tempKeyBuffer.AsSpan().Slice(0, keySize));
                    if (compare == 0)
                    {
                        return (recordPosition, true);
                    }
                    else if (compare < 0)
                    {
                        return (previousIndex, false);
                    }
                    previousIndex = recordPosition;
                }
                return (previousIndex, false);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(tempKeyBuffer);
            }
        }

        public void Dispose()
        {
            _indexView?.Dispose();
            _memoryMappedFile?.Dispose();
        }
    }
}
