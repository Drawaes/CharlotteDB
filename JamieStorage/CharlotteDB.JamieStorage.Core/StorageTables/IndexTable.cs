using System.Runtime.InteropServices;

namespace CharlotteDB.JamieStorage.Core.StorageTables
{
    [StructLayout(LayoutKind.Explicit)]
    internal struct IndexTable
    {
        [FieldOffset(0)]
        public BTreeAndBloomFilter MainIndexes;
        [FieldOffset(20)]
        public BTreeAndBloomFilter DeletedIndexes;
    }

    [StructLayout(LayoutKind.Explicit)]
    public struct BTreeAndBloomFilter
    {
        [FieldOffset(0)]
        public int BloomFilterIndex;
        [FieldOffset(4)]
        public int BloomFilterLength;
        [FieldOffset(8)]
        public int RegionIndex;
        [FieldOffset(12)]
        public int RegionLength;
        [FieldOffset(16)]
        public int HeadNodeIndex;
    }
}
