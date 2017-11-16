using System.Runtime.InteropServices;

namespace CharlotteDB.JamieStorage.Core.StorageTables
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal struct IndexTable
    {
        public int BloomFilterIndex;
        public int BloomFilterLength;
        public int BlockRegionIndex;
        public int BlockRegionLength;
        public int HeadNodeIndex;

        public int DeletedBloomFilterIndex;
        public int DeletedBloomFilterLength;
        public int DeletedRegionIndex;
        public int DeletedRegionLength;
    }
}
