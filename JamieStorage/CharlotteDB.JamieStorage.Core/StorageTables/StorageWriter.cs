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
        private BloomFilter<FNV1Hash> _bloomFilter;
        private BloomFilter<FNV1Hash> _deleteBloomFilter;
        private List<(Memory<byte> key, int start, int end)> _index = new List<(Memory<byte> key, int start, int end)>();
        private Database<TComparer> _database;

        public StorageWriter(int bitsToUseForBloomFilter, SkipList2<TComparer> inMemory, TComparer comparer, Database<TComparer> database)
        {
            _database = database;
            _bitsToUseForBloomFilter = bitsToUseForBloomFilter;
            _inMemory = inMemory;
            _comparer = comparer;
        }

        public void Dispose() => _stream?.Dispose();

        public async Task WriteToFile(string fileName)
        {
            _stream = File.Open(fileName, FileMode.CreateNew, FileAccess.Write, FileShare.None);
            await WriteDeletedRecordsAsync();
            await WriteBlockDataAsync();
            await WriteBloomFilterAsync();
            await WriteDeletedBloomFilter();
            await WriteIndexAsync();
            await WriteIndexTableAsync();
        }

        private async Task WriteDeletedBloomFilter()
        {
            // write out bloom filter
            _indexTable.DeletedBloomFilterIndex = (int)_stream.Position;
            await _deleteBloomFilter.SaveAsync(_stream);
            _indexTable.DeletedBloomFilterLength = (int)(_stream.Position - _indexTable.DeletedBloomFilterIndex);
        }

        private async Task WriteIndexTableAsync()
        {
            var tempBuffer = new byte[Unsafe.SizeOf<IndexTable>()];
            tempBuffer.AsSpan().WriteAdvance(_indexTable);
            await _stream.WriteAsync(tempBuffer);
        }

        private async Task WriteIndexAsync()
        {
            _indexTable.IndexFilterIndex = (int)_stream.Position;

            var tempBuffer = new byte[Unsafe.SizeOf<IndexRecord>()];
            for (var i = 0; i < _index.Count; i++)
            {
                var (key, start, end) = _index[i];
                var record = new IndexRecord()
                {
                    KeySize = (ushort)key.Length,
                    BlockStart = start,
                    BlockEnd = i == (_index.Count - 1) ?
                        (_indexTable.BlockRegionIndex + _indexTable.BlockRegionLength) : _index[i + 1].start,
                };
                tempBuffer.AsSpan().WriteAdvance(record);
                await _stream.WriteAsync(tempBuffer);
                await _stream.WriteAsync(key);
            }

            _indexTable.IndexFilterLength = (int)(_stream.Position - _indexTable.IndexFilterIndex);
        }

        private async Task WriteBloomFilterAsync()
        {
            _indexTable.BloomFilterIndex = (int)_stream.Position;
            await _bloomFilter.SaveAsync(_stream);
            _indexTable.BloomFilterLength = (int)(_stream.Position - _indexTable.BloomFilterIndex);
        }

        private async Task WriteBlockDataAsync()
        {
            _bloomFilter = BloomFilter.Create(_inMemory.Count - _deletedCount, _bitsToUseForBloomFilter, 2, _database.Hasher);
            _inMemory.Reset();
            _indexTable.BlockRegionIndex = (int)_stream.Position;
            var nodeCount = 0;
            var tempBuffer = new byte[Unsafe.SizeOf<EntryHeader>()];
            while (_inMemory.Next())
            {
                var node = _inMemory.CurrentNode;
                if (node.State != ItemState.Alive)
                {
                    if (node.State == ItemState.DeletedNew)
                    {
                        _deleteBloomFilter.Add(node.Key.Span);
                    }
                    continue;
                }

                _bloomFilter.Add(node.Key.Span);
                var header = new EntryHeader() { KeySize = (ushort)node.Key.Length, DataSize = node.Data.Length };
                if (nodeCount % 15 == 0)
                {
                    _index.Add((node.Key, (int)_stream.Position, 0));
                }
                nodeCount++;
                tempBuffer.AsSpan().WriteAdvance(header);
                await _stream.WriteAsync(tempBuffer);
                await _stream.WriteAsync(node.Key);
                await _stream.WriteAsync(node.Data);
            }
            _indexTable.BlockRegionLength = (int)(_stream.Position - _indexTable.BlockRegionIndex);
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
