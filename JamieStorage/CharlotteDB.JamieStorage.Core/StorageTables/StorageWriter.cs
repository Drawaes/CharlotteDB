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
    public class StorageWriter<TComparer> : IDisposable where TComparer : IKeyComparer
    {
        private SkipList2<TComparer> _inMemory;
        private TComparer _comparer;
        private int _bitsToUseForBloomFilter;
        private Stream _stream;
        private IndexTable _indexTable;
        private int _deletedCount;
        private BloomFilter<FNV1Hash>? _deleteBloomFilter;
        private Database<TComparer> _database;

        public StorageWriter(int bitsToUseForBloomFilter, SkipList2<TComparer> inMemory, TComparer comparer, Database<TComparer> database, string fileName)
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
            await _stream.WriteAsync(StorageFile.MagicHeader);
            await WriteDeletedRecordsAsync();

            var binWriter = new BinaryTreeWriter<TComparer>(3, _database, _inMemory.Count - _deletedCount, _inMemory);
            await binWriter.WriteTreeAsync(_stream);

            _indexTable.BlockRegionIndex = binWriter.StartOfData;
            _indexTable.BlockRegionLength = binWriter.EndOfData - binWriter.StartOfData;
            _indexTable.HeadNodeIndex = binWriter.RootNode;
            _indexTable.BloomFilterIndex = (int)_stream.Position;
            await binWriter.BloomFilter.SaveAsync(_stream);
            _indexTable.BloomFilterLength = (int)(_stream.Position - _indexTable.BloomFilterIndex);

            await WriteDeletedBloomFilter();
            await WriteIndexTableAsync();
            await _stream.WriteAsync(StorageFile.MagicTrailer);
        }

        private async Task WriteDeletedBloomFilter()
        {
            // write out bloom filter
            _indexTable.DeletedBloomFilterIndex = (int)_stream.Position;
            await _deleteBloomFilter?.SaveAsync(_stream);
            _indexTable.DeletedBloomFilterLength = (int)(_stream.Position - _indexTable.DeletedBloomFilterIndex);
        }

        private async Task WriteIndexTableAsync()
        {
            var tempBuffer = new byte[Unsafe.SizeOf<IndexTable>()];
            tempBuffer.AsSpan().WriteAdvance(_indexTable);
            await _stream.WriteAsync(tempBuffer);
        }
        
        private async Task WriteDeletedRecordsAsync()
        {
            _indexTable.DeletedRegionIndex = 0;
            var deletedCount = 0;
            var deletedNewCount = 0;
            var tempBuffer = new byte[2];
            while (_inMemory.Next())
            {
                var node = _inMemory.CurrentNode;
                switch (node.State)
                {
                    case ItemState.Deleted:
                        if (_database.MayContainNode(node.Key))
                        {
                            tempBuffer.AsSpan().WriteAdvance((ushort)node.Key.Length);
                            await _stream.WriteAsync(new Memory<byte>(tempBuffer));
                            await _stream.WriteAsync(node.Key);
                            _inMemory.UpdateState(ItemState.DeletedNew);
                            deletedNewCount++;
                        }
                        deletedCount++;
                        break;
                }
            }

            _indexTable.DeletedRegionLength = (int)(_stream.Position - _indexTable.DeletedRegionIndex);
            _deletedCount = deletedCount;
            _deleteBloomFilter = BloomFilter.Create(deletedNewCount, _bitsToUseForBloomFilter, 2, _database.Hasher);
        }
    }
}
