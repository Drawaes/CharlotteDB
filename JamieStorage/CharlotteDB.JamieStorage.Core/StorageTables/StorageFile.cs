using System;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using CharlotteDB.Core;
using CharlotteDB.Core.Keys;
using CharlotteDB.JamieStorage.Hashing;
using CharlotteDB.JamieStorage.InMemory;

namespace CharlotteDB.JamieStorage.Core.StorageTables
{
    public class StorageFile<TComparer> : IDisposable where TComparer : IKeyComparer
    {
        private string _fileName;
        private BloomFilter<FNV1Hash> _bloomFilter;
        private Database<TComparer> _database;
        private IndexTable _indexTable;
        private MemoryMappedFile _memoryMappedFile;
        private MappedFileMemory _mappedFile;
        private DeletedRecords<TComparer> _deletedRecords;

        public StorageFile(string fileName, Database<TComparer> database)
        {
            _fileName = fileName;
            _database = database;
        }

        public async Task WriteInMemoryTableAsync(SkipList2<TComparer> inMemory, int bitsToUseForBloomFilter)
        {
            using (var write = new StorageWriter<TComparer>(bitsToUseForBloomFilter, inMemory, _database.Comparer, _database))
            {
                await write.WriteToFile(_fileName);
            }
            LoadFile();
        }

        internal (SearchResult result, Memory<byte> data) TryGetData(Memory<byte> key)
        {
            if (_deletedRecords.IsDeleted(key.Span))
            {
                return (SearchResult.Deleted, default);
            }

            if (!_bloomFilter.PossiblyContains(key.Span))
            {
                return (SearchResult.NotFound, default);
            }
            var (index, end, exactMatch) = FindBlockToSearch(key);
            if (exactMatch)
            {
                ReturnFoundData(index);
            }
            if (index == -1)
            {
                return (SearchResult.NotFound, default);
            }
            var rowIndex = FindRow(key, index, end);
            if (rowIndex == -1)
            {
                return (SearchResult.NotFound, default);
            }
            return ReturnFoundData(rowIndex);
        }

        private (SearchResult result, Memory<byte> data) ReturnFoundData(int index)
        {
            var m = _mappedFile.Memory.Slice(index);
            m.Span.ReadAdvance<EntryHeader>(out var header);
            return (SearchResult.Found, m.Slice(header.KeySize + Unsafe.SizeOf<EntryHeader>(), header.DataSize));
        }

        private void LoadFile()
        {
            _memoryMappedFile = MemoryMappedFile.CreateFromFile(_fileName, System.IO.FileMode.Open);
            var fileInfo = new System.IO.FileInfo(_fileName);
            var fileSize = fileInfo.Length;

            _mappedFile = new MappedFileMemory(0, (int)fileSize, _memoryMappedFile);

            var header = _mappedFile.Memory.Span.Slice(0, StorageFile.MagicHeader.Length);
            if (!header.SequenceEqual(StorageFile.MagicHeader))
            {
                throw new NotImplementedException("There was an error we need to cover for the file loading");
            }

            var trailer = _mappedFile.Memory.Span.Slice(_mappedFile.Memory.Length - StorageFile.MagicTrailer.Length);
            if (!trailer.SequenceEqual(StorageFile.MagicTrailer))
            {
                throw new NotImplementedException("There was an error loading the file we need to sort this out");
            }

            _indexTable = _mappedFile.Memory.Span.Slice(_mappedFile.Length - Unsafe.SizeOf<IndexTable>() - StorageFile.MagicTrailer.Length).Read<IndexTable>();
            _bloomFilter = new BloomFilter<FNV1Hash>(_mappedFile.Memory.Slice(_indexTable.BloomFilterIndex, _indexTable.BloomFilterLength).Span, _database.Hasher);
            _deletedRecords = new DeletedRecords<TComparer>(_indexTable, _mappedFile.Memory, _database.Hasher, _database.Comparer);
        }

        internal SearchResult FindNode(Memory<byte> key)
        {
            if (_deletedRecords.IsDeleted(key.Span))
            {
                return SearchResult.Deleted;
            }

            if (!_bloomFilter.PossiblyContains(key.Span))
            {
                return SearchResult.NotFound;
            }

            var (start, end, exactMatch) = FindBlockToSearch(key);
            if (exactMatch)
            {
                return SearchResult.Found;
            }
            if (start == -1)
            {
                return SearchResult.NotFound;
            }
            var memLocation = FindRow(key, start, end);
            if (memLocation == -1)
            {
                return SearchResult.NotFound;
            }
            return SearchResult.Found;
        }

        internal bool MayContainNode(Memory<byte> key) => _bloomFilter.PossiblyContains(key.Span);

        private int FindRow(Memory<byte> key, int blockStart, int blockEnd)
        {
            var sizeOfBlock = blockEnd - blockStart;
            var blockMemory = _mappedFile.Memory.Span.Slice(blockStart, sizeOfBlock);
            while (blockMemory.Length > 0)
            {
                var rowStart = sizeOfBlock - blockMemory.Length;
                blockMemory = blockMemory.ReadAdvance<EntryHeader>(out var header);
                var key2 = blockMemory.Slice(0, header.KeySize);
                var compare = _database.Comparer.Compare(key.Span, key2);
                if (compare == 0)
                {
                    return blockStart + rowStart;
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
