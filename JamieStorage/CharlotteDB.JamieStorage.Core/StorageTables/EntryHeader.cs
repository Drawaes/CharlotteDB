using System.Runtime.InteropServices;

namespace CharlotteDB.JamieStorage.Core.StorageTables
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal struct EntryHeader
    {
        public ushort KeySize;
        public int DataSize;
    }
}
