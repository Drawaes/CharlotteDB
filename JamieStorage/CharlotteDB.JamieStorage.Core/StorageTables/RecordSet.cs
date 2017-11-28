using System;
using System.Collections.Generic;
using System.Text;
using CharlotteDB.Core;
using CharlotteDB.Core.Keys;
using CharlotteDB.JamieStorage.Hashing;

namespace CharlotteDB.JamieStorage.Core.StorageTables
{
    internal class RecordSet
    {
        private BloomFilter _bloomFilter;
        private BinaryTree _records;
        private IKeyComparer _comparer;

        public RecordSet(BTreeAndBloomFilter indexTable, Memory<byte> storage,
            IHash hasher, IKeyComparer comparer)
        {
            _comparer = comparer;
            _bloomFilter = new BloomFilter(storage.Slice(indexTable.BloomFilterIndex, indexTable.BloomFilterLength).Span, hasher);
            _records = new BinaryTree(storage, indexTable, comparer);
        }

        internal bool PossiblyContains(Span<byte> key) => _bloomFilter.PossiblyContains(key);

        internal (bool found, Memory<byte> data) FindNode(Span<byte> key)
        {
            if (!PossiblyContains(key))
            {
                return (false, default);
            }

            var result = _records.FindNode(key);
            if (result.result == SearchResult.Found)
            {
                return (true, result.data);
            }
            return (false, default);
        }
    }
}
