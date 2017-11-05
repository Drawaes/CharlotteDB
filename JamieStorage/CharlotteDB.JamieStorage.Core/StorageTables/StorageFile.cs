using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using CharlotteDB.Core;
using CharlotteDB.Core.Keys;
using CharlotteDB.JamieStorage.InMemory;

namespace CharlotteDB.JamieStorage.Core.StorageTables
{
    public class StorageFile<TComparer> : IDisposable where TComparer : IKeyComparer
    {
        private string _fileName;
        private int _bitsToUseForBloomFilter;
        private Hashing.BloomFilter<Hashing.FNV1Hash> _bloomFilter;
        private Database<TComparer> _database;
        private IndexTable _indexTable;
        private MemoryMappedFile _memoryMappedFile;

        public StorageFile(string fileName, int bitsToUseForBloomFilter, Database<TComparer> database)
        {
            _bitsToUseForBloomFilter = bitsToUseForBloomFilter;
            _fileName = fileName;
            _database = database;
        }

        public async Task WriteInMemoryTableAsync(SkipList<TComparer> inMemory)
        {
            var tempBuffer = new byte[8];

            using (var file = System.IO.File.Open(_fileName, System.IO.FileMode.CreateNew, System.IO.FileAccess.Write, System.IO.FileShare.None))
            {
                var deletedCount = await WriteDeletedRecordsAsync(inMemory, tempBuffer, file);

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
                    var header = new EntryHeader() { KeySize = (ushort)node.Key.Length, DataSize = node.Data.Length };
                    tempBuffer.AsSpan().WriteAdvance(header);
                    await file.WriteAsync(tempBuffer, 0, Unsafe.SizeOf<EntryHeader>());

                    await file.WriteAsync(node.Key);
                    await file.WriteAsync(node.Data);
                }

                // write out bloom filter
                _indexTable.BloomFilterIndex = file.Position;
                await _bloomFilter.SaveAsync(file);

                _indexTable.IndexFilterIndex = file.Position;
                var i = 0;
                for (; i < index.Count; i += 10)
                {
                    ((Span<byte>)tempBuffer).WriteAdvance((ushort)index[i].key.Length);
                    await file.WriteAsync(tempBuffer, 0, 2);
                    tempBuffer.AsSpan().WriteAdvance(index[i].fileIndex);
                    await file.WriteAsync(tempBuffer, 0, 8);
                    await file.WriteAsync(index[i].key);
                }
                _indexTable.IndexFilterLength = file.Position - _indexTable.IndexFilterIndex;

                tempBuffer = new byte[Unsafe.SizeOf<IndexTable>()];
                tempBuffer.AsSpan().WriteAdvance(_indexTable);
                await file.WriteAsync(tempBuffer);
            }
            LoadFile();
        }

        internal async Task<(SearchResult result, Memory<byte> data)> TryGetDataAsync(Memory<byte> key)
        {
            if (!_bloomFilter.PossiblyContains(key.Span))
            {
                return (SearchResult.NotFound, default);
            }
            var (index, exactMatch) = await FindBlockToSearchAsync(key);
            if (exactMatch)
            {
                throw new NotImplementedException();
                //return (SearchResult.Found, );
            }
            throw new NotImplementedException();
        }

        private void LoadFile()
        {
            _memoryMappedFile = MemoryMappedFile.CreateFromFile(_fileName, System.IO.FileMode.Open);
            var fileInfo = new System.IO.FileInfo(_fileName);
            var fileSize = fileInfo.Length;
            var span = (Span<byte>)new byte[Unsafe.SizeOf<IndexTable>()];
            var indexNames = fileSize - span.Length;
            using (var indexStream = _memoryMappedFile.CreateViewStream(indexNames, span.Length))
            {
                var count = indexStream.Read(span);
                if (count != span.Length)
                {
                    throw new NotImplementedException();
                }
                _indexTable = span.Read<IndexTable>();
            }
        }

        private MemoryMappedViewStream CreateIndexStream() => _memoryMappedFile.CreateViewStream(_indexTable.IndexFilterIndex, _indexTable.IndexFilterLength);

        private async Task<int> WriteDeletedRecordsAsync(SkipList<TComparer> inMemory, byte[] tempBuffer, System.IO.FileStream file)
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

        internal async Task<SearchResult> FindNodeAsync(Memory<byte> key)
        {
            if (!_bloomFilter.PossiblyContains(key.Span))
            {
                return SearchResult.NotFound;
            }
            var (index, exactMatch) = await FindBlockToSearchAsync(key);
            if (exactMatch)
            {
                return SearchResult.Found;
            }
            var memLocation = await FindBlockAsync(key, index);
            if (memLocation == 0)
            {
                return SearchResult.NotFound;
            }
            return SearchResult.Found;
        }

        private async Task<long> FindBlockAsync(Memory<byte> key, long blockStart)
        {
            var lengthsBuffer = new byte[Unsafe.SizeOf<EntryHeader>()];
            var keyBuffer = ArrayPool<byte>.Shared.Rent(256);
            try
            {
                using (var stream = _memoryMappedFile.CreateViewStream(blockStart, _indexTable.BloomFilterIndex - blockStart))
                {
                    while (stream.Position < stream.Length)
                    {
                        var count = await stream.ReadAsync(lengthsBuffer);
                        if (count != lengthsBuffer.Length)
                        {
                            throw new NotImplementedException();
                        }
                        var header = lengthsBuffer.AsSpan().Read<EntryHeader>();
                        if (keyBuffer.Length < header.KeySize)
                        {
                            ArrayPool<byte>.Shared.Return(keyBuffer);
                            keyBuffer = ArrayPool<byte>.Shared.Rent(header.KeySize);
                        }
                        count = await stream.ReadAsync(keyBuffer, 0, header.KeySize);
                        if (count != header.KeySize)
                        {
                            throw new NotImplementedException();
                        }
                        var compare = _database.Comparer.Compare(key.Span, keyBuffer.AsSpan().Slice(0, header.KeySize));
                        if (compare == 0)
                        {
                            throw new NotImplementedException("Need to read out the data and slice it");
                        }
                        else if (compare > 0)
                        {
                            stream.Position += header.DataSize;
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

        private async Task<(long index, bool exactMatch)> FindBlockToSearchAsync(Memory<byte> key)
        {
            using (var indexStream = CreateIndexStream())
            {
                var tempBuffer = (Memory<byte>)new byte[10];
                var tempKeyBuffer = ArrayPool<byte>.Shared.Rent(2048);
                try
                {
                    var previousIndex = 0L;
                    while (indexStream.Position < indexStream.Length)
                    {
                        var count = await indexStream.ReadAsync(tempBuffer);
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
                        count = await indexStream.ReadAsync(tempKeyBuffer, 0, keySize);
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
        }

        public void Dispose() => _memoryMappedFile?.Dispose();
    }
}
