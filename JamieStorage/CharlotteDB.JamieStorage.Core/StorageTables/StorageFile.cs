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
    public class StorageFile : IDisposable
    {
        public readonly static byte[] MagicHeader = Encoding.UTF8.GetBytes("CharlotteDBStart");
        public readonly static byte[] MagicTrailer = Encoding.UTF8.GetBytes("CharlotteDBEnd");

        private string _fileName;
        private IDatabase _database;
        private IndexTable _indexTable;
        private MemoryMappedFile _memoryMappedFile;
        private MappedFileMemory _mappedFile;
        private RecordSet _deletedRecords;
        private RecordSet _mainRecords;

        public StorageFile(string fileName, IDatabase database)
        {
            _fileName = fileName;
            _database = database;
        }

        public async Task WriteInMemoryTableAsync(IInMemoryStore inMemory, int bitsToUseForBloomFilter)
        {
            using (var write = new StorageWriter(bitsToUseForBloomFilter, inMemory, _database.Comparer, _database, _fileName))
            {
                await write.WriteToFile();
            }
            LoadFile();
        }

        internal (SearchResult result, Memory<byte> data) TryGetData(Memory<byte> key)
        {
            if (_deletedRecords.FindNode(key.Span).found)
            {
                return (SearchResult.Deleted, default);
            }

            var (found, data) = _mainRecords.FindNode(key.Span);
            if (found)
            {
                return (SearchResult.Found, data);
            }
            return (SearchResult.NotFound, default);
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

            var header = _mappedFile.Memory.Span.Slice(0, MagicHeader.Length);
            if (!header.SequenceEqual(MagicHeader))
            {
                throw new NotImplementedException("There was an error we need to cover for the file loading");
            }

            var trailer = _mappedFile.Memory.Span.Slice(_mappedFile.Memory.Length - MagicTrailer.Length);
            if (!trailer.SequenceEqual(MagicTrailer))
            {
                throw new NotImplementedException("There was an error loading the file we need to sort this out");
            }

            _indexTable = _mappedFile.Memory.Span.Slice(_mappedFile.Length - Unsafe.SizeOf<IndexTable>() - MagicTrailer.Length).Read<IndexTable>();
            _mainRecords = new RecordSet(_indexTable.MainIndexes, _mappedFile.Memory, _database.Hasher, _database.Comparer);
            _deletedRecords = new RecordSet(_indexTable.DeletedIndexes, _mappedFile.Memory, _database.Hasher, _database.Comparer);
        }

        internal SearchResult FindNode(Memory<byte> key)
        {
            if (_deletedRecords.FindNode(key.Span).found)
            {
                return SearchResult.Deleted;
            }

            var (result, data) = _mainRecords.FindNode(key.Span);
            if (result)
            {
                return SearchResult.Found;
            }
            return SearchResult.NotFound;
        }

        internal bool MayContainNode(Memory<byte> key) => _mainRecords.PossiblyContains(key.Span);

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

        public void Dispose() => _memoryMappedFile?.Dispose();
    }
}
