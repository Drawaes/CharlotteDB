using System;
using System.Collections.Generic;
using System.Text;
using CharlotteDB.Core;
using CharlotteDB.Core.Keys;
using CharlotteDB.JamieStorage.Hashing;

namespace CharlotteDB.JamieStorage.Core.StorageTables
{
    internal class DeletedRecords<TComparer> where TComparer : IKeyComparer
    {
        private BloomFilter<FNV1Hash> _bloomFilter;
        private Memory<byte> _deletedRecords;
        private TComparer _comparer;

        public DeletedRecords(IndexTable indexTable, Memory<byte> storage,
            FNV1Hash hasher, TComparer comparer)
        {
            _comparer = comparer;
            _bloomFilter = new BloomFilter<FNV1Hash>(storage.Slice(indexTable.DeletedBloomFilterIndex, indexTable.DeletedBloomFilterLength).Span, hasher);
            _deletedRecords = storage.Slice(indexTable.DeletedRegionIndex, indexTable.DeletedRegionLength);
        }

        internal bool ConfirmDeleted(Span<byte> key)
        {
            var deletedSpan = _deletedRecords.Span;
            while (deletedSpan.Length > 0)
            {
                deletedSpan = deletedSpan.ReadAdvance<ushort>(out var keyLength);
                var key2 = deletedSpan.Slice(0, keyLength);
                var result = _comparer.Compare(key, key2);
                if (result == 0)
                {
                    return true;
                }
                else if (result == -1)
                {
                    return false;
                }
                deletedSpan = deletedSpan.Slice(keyLength);
            }
            return false;
        }

        internal bool IsDeleted(Span<byte> key)
        {
            if (!_bloomFilter.PossiblyContains(key))
            {
                return false;
            }
            return ConfirmDeleted(key);
        }
    }
}
