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
        private MappedFileMemory _mappedFile;

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
                var deletedCount = await WriteDeletedRecordsAsync(inMemory, file);

                _bloomFilter = new Hashing.BloomFilter<Hashing.FNV1Hash>(inMemory.Count - deletedCount, _bitsToUseForBloomFilter, new Hashing.FNV1Hash());
                inMemory.Reset();
                var index = new List<(Memory<byte> key, int fileIndex, int indexEnd)>();
                _indexTable.BlockRegionIndex = (int)file.Position;
                while (inMemory.Next())
                {
                    var node = inMemory.CurrentNode;
                    if (node.State != ItemState.Alive)
                    {
                        continue;
                    }

                    _bloomFilter.Add(node.Key.Span);
                    var startIndex = (int)file.Position;
                    var header = new EntryHeader() { KeySize = (ushort)node.Key.Length, DataSize = node.Data.Length };
                    tempBuffer.AsSpan().WriteAdvance(header);
                    await file.WriteAsync(tempBuffer, 0, Unsafe.SizeOf<EntryHeader>());

                    await file.WriteAsync(node.Key);
                    await file.WriteAsync(node.Data);

                    index.Add((node.Key, startIndex, (int)file.Position));

                }
                _indexTable.BlockRegionLength = (int)(file.Position - _indexTable.BlockRegionIndex);
                // write out bloom filter
                _indexTable.BloomFilterIndex = (int)file.Position;
                await _bloomFilter.SaveAsync(file);
                _indexTable.BloomFilterLength = (int)(file.Position - _indexTable.BloomFilterIndex);

                await WriteIndex(file, index);

                tempBuffer = new byte[Unsafe.SizeOf<IndexTable>()];
                tempBuffer.AsSpan().WriteAdvance(_indexTable);
                await file.WriteAsync(tempBuffer);
            }
            LoadFile();
        }

        private async Task WriteIndex(System.IO.FileStream file, List<(Memory<byte> key, int fileIndex, int indexEnd)> index)
        {
            _indexTable.IndexFilterIndex = (int)file.Position;
            var finalItemAdded = false;

            var tempBuffer = new byte[Unsafe.SizeOf<IndexRecord>()];
            var newIndex = new List<(Memory<byte> key, IndexRecord)>();
            for (var i = 0; i < index.Count; i += 10)
            {
                var indexRecord = new IndexRecord()
                {
                    KeySize = (ushort)index[i].key.Length,
                    BlockStart = index[i].fileIndex,
                };
                newIndex.Add((index[i].key, indexRecord));
                if (i == index.Count - 1)
                {
                    finalItemAdded = true;
                }
            }
            if (!finalItemAdded)
            {
                newIndex.Add((index[index.Count - 1].key
                    , new IndexRecord()
                    {
                        BlockStart = index[index.Count - 1].fileIndex,
                        KeySize = (ushort)index[index.Count - 1].key.Length
                    }));
            }

            for (var i = 0; i < newIndex.Count; i++)
            {
                var currentIndex = newIndex[i].Item2;
                if (i == (newIndex.Count - 1))
                {
                    currentIndex.BlockEnd = _indexTable.BlockRegionIndex + _indexTable.BlockRegionLength;
                }
                else
                {
                    currentIndex.BlockEnd = newIndex[i + 1].Item2.BlockStart;
                }
                tempBuffer.AsSpan().WriteAdvance(currentIndex);
                await file.WriteAsync(tempBuffer);
                await file.WriteAsync(newIndex[i].key);
            }
            _indexTable.IndexFilterLength = (int)(file.Position - _indexTable.IndexFilterIndex);
        }

        internal (SearchResult result, Memory<byte> data) TryGetData(Memory<byte> key)
        {
            if (!_bloomFilter.PossiblyContains(key.Span))
            {
                return (SearchResult.NotFound, default);
            }
            var (index, end, exactMatch) = FindBlockToSearch(key);
            if (exactMatch)
            {
                throw new NotImplementedException();
                //return (SearchResult.Found, );
            }
            if(index == -1)
            {
                return (SearchResult.NotFound, default);
            }
            var rowIndex = FindRow(key, index, end);
            if(rowIndex == -1)
            {
                return (SearchResult.NotFound, default);
            }
            throw new NotImplementedException();
        }

        private void LoadFile()
        {
            _memoryMappedFile = MemoryMappedFile.CreateFromFile(_fileName, System.IO.FileMode.Open);
            var fileInfo = new System.IO.FileInfo(_fileName);
            var fileSize = fileInfo.Length;

            _mappedFile = new MappedFileMemory(0, (int)fileSize, _memoryMappedFile);
            _indexTable = _mappedFile.Memory.Span.Slice(_mappedFile.Length - Unsafe.SizeOf<IndexTable>()).Read<IndexTable>();
            
        }

        private async Task<int> WriteDeletedRecordsAsync(SkipList<TComparer> inMemory, System.IO.FileStream file)
        {
            _indexTable.DeletedRegionIndex = 0;
            var deletedCount = 0;
            var tempBuffer = new byte[2];
            while (inMemory.Next())
            {
                var node = inMemory.CurrentNode;
                switch (node.State)
                {
                    case ItemState.Deleted:
                        var searchResult = _database.FindNode(node.Key);
                        if (searchResult == SearchResult.Found)
                        {
                            ((Span<byte>)tempBuffer).WriteAdvance((ushort)node.Key.Length);
                            await file.WriteAsync(new Memory<byte>(tempBuffer));
                            await file.WriteAsync(node.Key);
                        }
                        deletedCount++;
                        break;
                }
            }
            _indexTable.DeletedRegionLength = (int)(file.Position - _indexTable.DeletedRegionIndex);

            return deletedCount;
        }

        internal SearchResult FindNode(Memory<byte> key)
        {
            if (!_bloomFilter.PossiblyContains(key.Span))
            {
                return SearchResult.NotFound;
            }

            var (start, end, exactMatch) = FindBlockToSearch(key);
            if (exactMatch)
            {
                return SearchResult.Found;
            }

            var memLocation = FindRow(key, start, end);
            if (memLocation == -1)
            {
                return SearchResult.NotFound;
            }
            return SearchResult.Found;
        }

        private int FindRow(Memory<byte> key, int blockStart, int blockEnd)
        {
            var endOfBlock = blockEnd - blockStart;
            var blockMemory = _mappedFile.Memory.Span.Slice(blockStart, endOfBlock);
            while (blockMemory.Length > 0)
            {
                blockMemory = blockMemory.ReadAdvance<EntryHeader>(out var header);
                var key2 = blockMemory.Slice(0, header.KeySize);
                var compare = _database.Comparer.Compare(key.Span, key2);
                if (compare == 0)
                {
                    throw new NotImplementedException("Need to read out the data and slice it");
                }
                else if (compare > 0)
                {
                    blockMemory = blockMemory.Slice(header.KeySize + header.DataSize);
                }
                else if (compare < 0)
                {
                    return -1;
                }
            }

            return -1;
        }

        private (int start, int end, bool exactMatch) FindBlockToSearch(Memory<byte> key)
        {
            var indexMem = _mappedFile.Memory.Span.Slice(_indexTable.IndexFilterIndex, _indexTable.IndexFilterLength);

            var previousStart = -1;
            var previousEnd = -1;
            while (indexMem.Length > 0)
            {
                indexMem = indexMem.ReadAdvance<IndexRecord>(out var record);

                var compare = _database.Comparer.Compare(key.Span, indexMem.Slice(0, record.KeySize));
                if (compare == 0)
                {
                    return (record.BlockStart, record.BlockEnd, true);
                }
                else if (compare < 0)
                {
                    return (previousStart, previousEnd, false);
                }
                indexMem = indexMem.Slice(record.KeySize);
                previousStart = record.BlockStart;
                previousEnd = record.BlockEnd;
            }
            return (previousStart, previousEnd, false);
        }

        public void Dispose() => _memoryMappedFile?.Dispose();
    }
}
