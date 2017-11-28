using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using CharlotteDB.Core;
using CharlotteDB.Core.Keys;
using CharlotteDB.JamieStorage.Hashing;
using CharlotteDB.JamieStorage.InMemory;

namespace CharlotteDB.JamieStorage.Core.StorageTables
{
    public class StorageWriter : IDisposable
    {
        private IInMemoryStore _inMemory;
        private IKeyComparer _comparer;
        private int _bitsToUseForBloomFilter;
        private Stream _stream;
        private IndexTable _indexTable;
        private int _deletedCount;
        private BloomFilter _deleteBloomFilter;
        private IDatabase _database;

        public StorageWriter(int bitsToUseForBloomFilter, IInMemoryStore inMemory, IKeyComparer comparer, IDatabase database, string fileName)
        {
            _stream = File.Open(fileName, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
            _database = database;
            _bitsToUseForBloomFilter = bitsToUseForBloomFilter;
            _inMemory = inMemory;
            _comparer = comparer;
        }

        public void Dispose() => _stream?.Dispose();

        public async Task WriteToFile()
        {
            _deletedCount = CountDeleted();

            await _stream.WriteAsync(StorageFile.MagicHeader);

            var deletedBinWriter = new BinaryTreeWriter(3, _database, _deletedCount, _inMemory, ItemState.Deleted);
            await deletedBinWriter.WriteTreeAsync(_stream);
            _indexTable.DeletedIndexes.RegionIndex = deletedBinWriter.StartOfData;
            _indexTable.DeletedIndexes.RegionLength = deletedBinWriter.EndOfData - deletedBinWriter.StartOfData;
            _indexTable.DeletedIndexes.HeadNodeIndex = deletedBinWriter.RootNode;

            _indexTable.DeletedIndexes.BloomFilterIndex = (int)_stream.Position;
            await deletedBinWriter.BloomFilter.SaveAsync(_stream);
            _indexTable.DeletedIndexes.BloomFilterLength = (int)(_stream.Position - _indexTable.DeletedIndexes.BloomFilterIndex);

            var binWriter = new BinaryTreeWriter(3, _database, _inMemory.Count - _deletedCount, _inMemory, ItemState.Alive);
            await binWriter.WriteTreeAsync(_stream);
            _indexTable.MainIndexes.RegionIndex = binWriter.StartOfData;
            _indexTable.MainIndexes.RegionLength = binWriter.EndOfData - binWriter.StartOfData;
            _indexTable.MainIndexes.HeadNodeIndex = binWriter.RootNode;
            
            _indexTable.MainIndexes.BloomFilterIndex = (int)_stream.Position;
            await binWriter.BloomFilter.SaveAsync(_stream);
            _indexTable.MainIndexes.BloomFilterLength = (int)(_stream.Position - _indexTable.MainIndexes.BloomFilterIndex);

            await WriteIndexTableAsync();
            await _stream.WriteAsync(StorageFile.MagicTrailer);
        }

        private int CountDeleted()
        {
            var count = 0;
            _inMemory.Reset();
            while(_inMemory.Next())
            {
                var node = _inMemory.CurrentNode;
                if(node.State == ItemState.Deleted)
                {
                    count++;
                }
            }
            return count;
        }
        
        private async Task WriteIndexTableAsync()
        {
            var tempBuffer = new byte[Unsafe.SizeOf<IndexTable>()];
            tempBuffer.AsSpan().WriteAdvance(_indexTable);
            await _stream.WriteAsync(tempBuffer);
        }
    }
}
