using System.Runtime.InteropServices;

namespace CharlotteDB.JamieStorage.Core.StorageTables
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal struct IndexRecord
    {
        public int BlockStart;
        public int BlockEnd;
        public ushort KeySize;
    }
}
